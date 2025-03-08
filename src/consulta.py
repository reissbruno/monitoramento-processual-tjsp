from os import environ as env
from datetime import datetime
import time
from urllib.parse import urlparse, urlunparse, urljoin

from fastapi.logger import logger
from fastapi.responses import JSONResponse
from fastapi import status
from bs4 import BeautifulSoup
import httpx

# Local imports
from src.models import Movimentacao, Telemetria

# Captura variáveis de ambiente e cria constantes
TEMPO_LIMITE = int(env.get('TEMPO_LIMITE', 180))
TENTATIVAS_MAXIMAS_RECURSIVAS = int(env.get('TENTATIVAS_MAXIMAS_RECURSIVAS', 30))


async def capturar_todas_movimentacoes(pagina_html: str) -> list[Movimentacao]:
    """
    Extrai todas as movimentações (visíveis e ocultas) do HTML da página.
    Para cada movimentação, extrai a data, a descrição e o link do documento.
    """
    soup = BeautifulSoup(pagina_html, "html.parser")
    movimentacoes = []
    tbody_ids = ["tabelaUltimasMovimentacoes", "tabelaTodasMovimentacoes"]

    for tbody_id in tbody_ids:
        tbody = soup.find("tbody", id=tbody_id)
        if not tbody:
            continue

        for tr in tbody.find_all("tr", class_="containerMovimentacao"):
            td_data = tr.find("td", class_="dataMovimentacao")
            data_text = td_data.get_text(strip=True) if td_data else ""
            td_desc = tr.find("td", class_="descricaoMovimentacao")
            descricao = td_desc.get_text(separator=" ", strip=True) if td_desc else ""
            link_tag = tr.find("a", class_="linkMovVincProc")
            documentos = link_tag.get("href", "") if link_tag else ""
            
            movimentacoes.append(
                Movimentacao(
                    data_hora=data_text,
                    descricao=descricao,
                    documentos=documentos
                )
            )

    return movimentacoes

async def fetch(numero_processo: str, telemetria: Telemetria) -> dict:
    """
    Acessa a página inicial do TJSC, resolve o CAPTCHA com retry, envia o formulário via GET e
    captura as movimentações do processo. Retorna os resultados como objetos Movimentacao,
    incluindo os links completos dos documentos (com ticket).
    """
    if not numero_processo or not isinstance(numero_processo, str):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={'code': 2, 'message': 'ERRO_ENTIDADE_NAO_PROCESSAVEL'}
        )

    if telemetria.tentativas >= TENTATIVAS_MAXIMAS_RECURSIVAS:
        logger.error("Número máximo de tentativas recursivas atingido.")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={'code': 3, 'message': 'ERRO_SERVIDOR_INTERNO'}
        )

    logger.info(f'Função fetch() iniciou. Processo: {numero_processo} - Tentativa {telemetria.tentativas}')

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Host": "esaj.tjsp.jus.br",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    }

    client = httpx.Client(timeout=TEMPO_LIMITE, verify=False, headers=headers)
    results = None
    base_url = "https://esaj.tjsp.jus.br"

    try:
        get_inicial = (
            "https://esaj.tjsp.jus.br/cpopg/search.do?conversationId=&cbPesquisa=NUMPROC"
            "&numeroDigitoAnoUnificado=&foroNumeroUnificado="
            "&dadosConsulta.valorConsultaNuUnificado=&dadosConsulta.valorConsultaNuUnificado=UNIFICADO"
            f"&dadosConsulta.valorConsulta={numero_processo}"
            "&dadosConsulta.tipoNuProcesso=SAJ"
        )
        response = client.get(get_inicial, follow_redirects=False)
        content_length = response.headers.get('Content-Length')
        if content_length:
            telemetria.bytes_enviados += int(content_length)
        else:
            telemetria.bytes_enviados += len(response.content)
        
        if response.status_code == 302:
            redirect_url = response.headers.get("Location")
            logger.info(f"Redirecionamento encontrado para: {redirect_url}")
            absolute_url = urljoin(base_url, redirect_url)
            parsed = urlparse(absolute_url)
            clean_path = parsed.path.split(";")[0]
            clean_url = urlunparse((parsed.scheme, parsed.netloc, clean_path, parsed.params, parsed.query, parsed.fragment))
            response = client.get(clean_url, follow_redirects=True)
            content_length = response.headers.get('Content-Length')
            if content_length:
                telemetria.bytes_enviados += int(content_length)
            else:
                telemetria.bytes_enviados += len(response.content)
        
        capturar_movimentacoes = await capturar_todas_movimentacoes(response.text)
        results = {
            'code': 200,
            'message': 'SUCESSO',
            'datetime': datetime.now().isoformat(),
            'results': [mov for mov in capturar_movimentacoes]
        }
        
    except httpx.RequestError as e:
        logger.error(f"Erro de requisição: {e}")
        telemetria.tempo_total = round(time.time() - telemetria.tempo_total, 2)
        results = JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={'code': 4, 'message': 'ERRO_SERVIDOR_INTERNO', 'telemetria': telemetria.dict()}
        )
    except Exception as e:
        logger.error(f"Erro durante a consulta: {e}")
        if telemetria.tentativas < TENTATIVAS_MAXIMAS_RECURSIVAS:
            logger.info("Tentando novamente...")
            telemetria.tentativas += 1
            return await fetch(numero_processo, telemetria)
        else:
            telemetria.tempo_total = round(time.time() - telemetria.tempo_total, 2)
            results = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={'code': 4, 'message': 'ERRO_SERVIDOR_INTERNO', 'telemetria': telemetria.dict()}
            )
    finally:
        client.close()
        telemetria.tempo_total = round(time.time() - telemetria.tempo_total, 2)
        if results is not None and isinstance(results, dict) and "telemetria" not in results:
            results["telemetria"] = telemetria.dict()
    
    return results
