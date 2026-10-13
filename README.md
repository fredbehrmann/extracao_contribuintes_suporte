# Extração de Contribuintes Suporte

Pipeline em Python para identificar a carteira de contribuintes suporte, extrair
dados dos bancos Oracle e PostgreSQL, carregar o Data Warehouse e monitorar
notícias relacionadas às empresas e aos seus vínculos societários.

## Fluxo principal

O arquivo `orquestrador_geral.py` coordena o processamento completo:

1. identificação dos contribuintes suporte;
2. carga das tabelas de staging e da dimensão tempo;
3. carga das dimensões iniciais e do complemento de RDT;
4. carga das dimensões dependentes;
5. carga das dimensões cadastrais finais;
6. carga dos fatos de arrecadação, NFS-e e inadimplência;
7. carga dos fatos consolidados, de fiscalização e de RDT;
8. montagem da base de monitoramento de notícias;
9. busca e persistência das notícias.

As fases são executadas em ordem. Dentro das fases marcadas como paralelas, o
orquestrador executa até três scripts ao mesmo tempo por padrão.

## Orquestradores

### `orquestrador_geral.py`

Executa todo o DW e o monitoramento de notícias. A identificação inicial é
condicional: ela roda quando ainda não há carteira para o ano final do período,
ou quando `--com-identificacao` é informado.

```powershell
python orquestrador_geral.py
python orquestrador_geral.py --escopo dw
python orquestrador_geral.py --escopo noticias
python orquestrador_geral.py --inicio 2021-01-01 --fim 2026-12-31
python orquestrador_geral.py --dry-run
```

### `executar_pipeline_periodo.py`

Executa somente as sete fases do DW para uma janela temporal inclusiva.

```powershell
python executar_pipeline_periodo.py --inicio 2021-01-01 --fim 2026-12-31
python executar_pipeline_periodo.py --dry-run
```

### `orquestrador_noticias.py`

Executa, em sequência, `montar_entrada_db.py` e `scrapper_noticias_v2.py`. A
busca só começa quando a montagem da base termina com código de saída zero.

```powershell
python orquestrador_noticias.py
```

O `scrapper_noticias_v3.py` é uma implementação incremental independente. Os
orquestradores ainda utilizam a versão V2.

## Preparação do ambiente

Recomenda-se Python 3.11 ou mais recente e um ambiente virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Também é necessário instalar e configurar o Oracle Instant Client compatível
com o pacote `cx-Oracle`.

## Configuração das conexões

Credenciais não são armazenadas no repositório. Defina-as no ambiente do
processo antes da execução. O arquivo `.env.example` lista as variáveis usadas,
mas não é carregado automaticamente.

Exemplo em PowerShell:

```powershell
$env:RFB_PG_PASS = "senha-do-postgres"
$env:SAT_PASS = "senha-do-sat"
$env:DWH_PASS = "senha-do-dwh"
```

As principais variáveis são:

| Variável | Uso |
| --- | --- |
| `RFB_PG_PASS` | Senha do PostgreSQL usado pelo DW e pelas notícias |
| `SAT_PASS` | Senha do Oracle SAT |
| `DWH_PASS` | Senha do Oracle DWH |
| `RFB_PG_HOST`, `RFB_PG_PORT` | Endereço do PostgreSQL, quando parametrizado pelo script |
| `RFB_PG_USER`, `RFB_PG_DB` | Usuário e banco PostgreSQL, quando parametrizados pelo script |
| `SAT_USER`, `SAT_DSN` | Usuário e DSN do SAT, quando parametrizados pelo script |
| `DWH_USER`, `DWH_DSN` | Usuário e DSN do DWH, quando parametrizados pelo script |

## Período de processamento

Os orquestradores propagam o período por argumentos e pelas variáveis
`DATA_INICIO`, `DATA_FIM`, `ANO_INICIO`, `ANO`, `ANO_REFERENCIA`,
`COMPETENCIA_INICIO`, `COMPETENCIA_FIM`, `BEGIN_DATE` e `END_DATE`.

## Logs e dados locais

Logs, locks, caches `.pkl`, bytecode Python e resultados de execução não são
versionados. Os diretórios são recriados automaticamente pelos scripts quando
necessário.

## Testes

Os testes unitários centrais cobrem a lógica do monitor de notícias V3:

```powershell
python -m unittest test_scrapper_noticias_v3.V3CoreTests
```

Os ETLs dependem de acesso às redes e aos bancos do ambiente de execução; por
isso, a validação completa deve ser feita no servidor autorizado.
