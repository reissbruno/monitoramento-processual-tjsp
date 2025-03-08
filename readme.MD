# Monitoramento Processual - Tribunal de Justiça de São Paulo :robot:

## Descrição
API de automação para monitoramento de processos no Tribunal de Justiça de São Paulo (TJSP). A aplicação utiliza **FastAPI** para expor endpoints de consulta processual, integrando com Requisições assincronas para a captura de dados do site oficial do TJSP.


## Funcionalidades
- Consulta automática de processos no site do TJSP
- Captura de movimentações processuais, incluindo eventos, data/hora, descrição e documentos
- Endpoint REST para integração com outras aplicações


## Tecnologias Utilizadas
- **FastAPI**: Framework para construção de APIs rápidas e performáticas
- **HTTPX**: Para as requisições http
- **BeautifulSoup**: Extração e parsing de dados HTML
- **Uvicorn**: Servidor ASGI para execução do FastAPI
- **Docker**: Containerização para facilidade de deployment


## Requisitos
- Python 3.9 ou superior
- Docker (opcional para rodar via container)


## Instalação e Execução Local


1. Clone o repositório:
- git clone https://github.com/reissbruno/monitoramento-processual-tjsp
- cd monitoramento-processual-tjsp


2. Instale as dependências:
- pip install -r requirements.txt


3. Execute o servidor:
- uvicorn server:app --host 0.0.0.0 --port 8000


4. Acesse a documentação da API no navegador:
- http://localhost:8000/docs


## Utilizando com Docker
1. Construa a imagem Docker:
- docker build -t monitoramento-processual-tjsp .


2. Rode o container:
- docker run -d -p 8000:8000 --name monitoramento-tjsp monitoramento-processual-tjsp


3. Acesse a API no navegador:
- http://localhost:8000/docs


## Variáveis de Ambiente
| ENV VAR | Descrição | Default |
| --------- | ---------- | --------- |
| `BOT_NAME` | Nome do bot. Útil caso houver mais de um container rodando. | `monitoramento-processual-tjsp` |
| `LOG_LEVEL` | Nível de log (DEBUG, INFO, WARNING, ERROR) | `INFO` |
| `TEMPO_LIMITE` | Tempo limite em segundos para carregamento de página. | `180` |
| `TENTATIVAS_MAXIMAS_RECURSIVAS` | Máximo de tentativas recursivas para consulta. | `30` |



## Endpoints da API

### Consultar Processo
* Consulta movimentações de um processo específico no tjsp.
    - GET /api/monitoramento-processual-tjsp/consulta


### Parâmetros
* processo (string, obrigatório): Número do processo a ser consultado
    - Exemplo de Requisição
    - curl -X 'GET' \
        'http://localhost:8000/api/monitoramento-processual-tjsp/consulta?processo=12345678901234567890' \
        -H 'accept: application/json'



| HTTP CODE | Descrição |
| --------- | --------- |
| `200`     |Sucesso |
| `422`     |Não foi possível processar |
| `502`     |Bad Gateway |
| `512`     |Erro ao executar parse da página |
