import os
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text
from datetime import datetime
import time
from sqlalchemy.exc import OperationalError

from periodo_processamento import obter_periodo

# ================================================
# PARAMETRIZAÇÃO
# ================================================
PERIODO = obter_periodo(ano_fim_padrao=2025)
ANO_INICIO = PERIODO.inicio.year
ANO = PERIODO.fim.year
COMPETENCIA_INICIO = PERIODO.competencia_inicio
COMPETENCIA_FIM = PERIODO.competencia_fim
DATA_INICIO = PERIODO.inicio.isoformat()
DATA_FIM_EXCLUSIVO = PERIODO.fim_exclusivo.isoformat()

SCHEMA = "suporte_arrecadacao"

# Conexões Oracle
oracle_dwh_config = {
    "user": "AFROCHA",
    "password": os.getenv("DWH_PASS", ""),
    "dsn": "barroquinha:1521/DWH"
}

oracle_sat_config = {
    "user": "AFROCHA",
    "password": os.getenv("SAT_PASS", ""),
    "dsn": "pituba:2118/SAT"
}

# Conexão Postgres (SEFAZ - staging e suporte)
pg_config_sefaz = {
    'dbname': 'db_rfb',
    'user': 'cif_user',
    'password': os.getenv("RFB_PG_PASS", ""),
    'host': 'valedosrios',
    'port': 5432
}
#pg_engine_sefaz = create_engine(
#    f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
#)
pg_engine_sefaz = create_engine(
    f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}",
    pool_pre_ping=True,
    pool_recycle=1800
)

# Conexão Postgres (RFB - consulta externa)
RFB_DBNAME = "db_rfb"
RFB_USER = "cif_user"
RFB_PASS = os.getenv("RFB_PG_PASS", "")
RFB_HOST = "valedosrios"
RFB_PORT = 5432
#pg_engine_rfb = create_engine(
#    f"postgresql+psycopg2://{RFB_USER}:{RFB_PASS}@{RFB_HOST}:{RFB_PORT}/{RFB_DBNAME}"
#)
pg_engine_rfb = create_engine(
    f"postgresql+psycopg2://{RFB_USER}:{RFB_PASS}@{RFB_HOST}:{RFB_PORT}/{RFB_DBNAME}",
    pool_pre_ping=True,
    pool_recycle=1800
)
# ================================================
# FUNÇÕES AUXILIARES
# ================================================
def conectar_oracle(config):
    return cx_Oracle.connect(config["user"], config["password"], config["dsn"])

def executar_select(engine, query):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

#def truncar_tabela(tabela):
#    with pg_engine_sefaz.begin() as conn:
#        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{tabela}"))
def truncar_tabela(tabela, tentativas=3):
    for tentativa in range(1, tentativas + 1):
        try:
            with pg_engine_sefaz.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{tabela}"))
            return
        except OperationalError as e:
            print(f"   -> Erro ao truncar {tabela}. Tentativa {tentativa}/{tentativas}.")
            if tentativa == tentativas:
                raise
            time.sleep(10)

def carregar_dataframe(df, tabela):
    if not df.empty:
        df.columns = [col.lower() for col in df.columns]
        df.to_sql(tabela, pg_engine_sefaz, schema=SCHEMA, if_exists="append", index=False, chunksize=1000)
        print(f"   -> {len(df)} registros inseridos em {tabela}")
    else:
        print(f"   -> Nenhum registro retornado para {tabela}")

def validar_resultado_obrigatorio(df, tabela):
    if df.empty:
        raise RuntimeError(
            f"A extração obrigatória de {tabela} retornou zero registros. "
            "A staging existente foi preservada e o pipeline foi interrompido."
        )

def limpar_datas(df, colunas):
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"0": None, "00000000": None, "": None})
            df[col] = df[col].apply(
                lambda x: datetime.strptime(x, "%Y%m%d").date() if x and len(x) == 8 else None
            )
    return df

# Função para dividir lista em lotes de até 999 itens
def chunk_list(lst, size=999):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def formatar_lista_sql_texto(valores):
    """Formata CNPJs-base textuais já validados para os lotes do operador IN."""
    return ",".join(
        f"'{str(valor).strip().replace(chr(39), chr(39) * 2)}'" for valor in valores
    )

# Função genérica para executar SELECTs por chunks de CNPJ_BASE
def executar_select_em_chunks(engine_or_conn, query_template, lista_cnpjs, is_oracle=False):
    dfs = []

    if not lista_cnpjs:
        return pd.DataFrame()

    for chunk in chunk_list(lista_cnpjs, 999):
        # NUCNPJBASE passou a ser VARCHAR2. Preserva zeros à esquerda e evita
        # conversão numérica implícita nas consultas Oracle.
        cnpj_in = formatar_lista_sql_texto(chunk)
        query = query_template.format(
            CNPJ_IN=cnpj_in,
            DATA_INICIO=DATA_INICIO,
            DATA_FIM_EXCLUSIVO=DATA_FIM_EXCLUSIVO,
            COMPETENCIA_INICIO=COMPETENCIA_INICIO,
            COMPETENCIA_FIM=COMPETENCIA_FIM,
            ANO_INICIO=ANO_INICIO,
            ANO=ANO,
        )

        if is_oracle:
            df_chunk = pd.read_sql(query, engine_or_conn)
        else:
            df_chunk = executar_select(engine_or_conn, query)

        if not df_chunk.empty:
            dfs.append(df_chunk)

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# ================================================
# CNPJs SUPORTE
# ================================================
print("[1/9] Carregando lista de contribuintes suporte...")
df_suporte = executar_select(pg_engine_sefaz, f"""
    SELECT distinct cnpj_base
    FROM {SCHEMA}.stg_contribuintes_suporte
    WHERE ano BETWEEN {ANO_INICIO} AND {ANO}
""")
cnpj_bases = (
    df_suporte["cnpj_base"]
    .dropna()
    .astype("string")
    .str.strip()
)
cnpj_bases = cnpj_bases[cnpj_bases.str.fullmatch(r"[0-9A-Za-z]{8}", na=False)]
cnpj_bases = cnpj_bases.drop_duplicates().tolist()

print(f"   -> {len(cnpj_bases)} CNPJs suporte carregados")

# ================================================
# ETAPA 1 - CADASTRAIS (Oracle DWH)
# ================================================
print("[2/9] Extraindo dados cadastrais...")

query_cadastrais = """
SELECT   LPAD(co.COD_NRO_CGC_CPF,14,'0')          AS CNPJ,
  co.COD_CTBU_MOBI                          AS CGA,
  co.NOM_RZAO_SOCI_CTBU                     AS RAZAO_SOCIAL,
  co.NMFANTASIA                             AS NOME_FANTASIA,
  LPAD(co.COD_CPF_CNPJ_SINTETICO,8,'0')     AS CNPJ_BASE,
  co.NMSEGMENTOFISCAL                       AS SEGMENTO_FISCAL,
  co.CDTIPOSITUACAOCADASTRAL                AS CD_SITUACAO_CADASTRAL,
  co.DSTIPOSITUACAOCADASTRAL                AS SITUACAO_CADASTRAL,
  co.IND_SN                                 AS INDICADOR_SIMPLES_NACIONAL,
  co.IND_MEI                                AS INDICADOR_MEI,
  co.IND_DIR_PUB                            AS INDICADOR_DIREITO_PUBLICO,
  co.DT_INSCRICAO                           AS DATA_INSCRICAO,
  co.QTD_FILIAL                             AS QTD_FILIAIS,
  co.NOM_MUNC_CTBU                          AS MUNICIPIO,
  co.NOM_BAI_CTBU                           AS BAIRRO,
  co.COD_TIP_LOG_CTBU                       AS COD_TIPO_LOGRADOURO,
  co.TXT_NOME_LOG_CTBU                      AS LOGRADOURO,
  co.TXT_CPLM_ENDE_CTBU                     AS COMPLEMENTO_ENDERECO,
  co.COD_CEP                                AS CEP,
  co.ind_supf                               AS INDICADOR_SUP
FROM odi_nfse.dim_contribuinte co
WHERE co.COD_CPF_CNPJ_SINTETICO IN ({CNPJ_IN})
"""

with conectar_oracle(oracle_dwh_config) as conn:
    df = executar_select_em_chunks(conn, query_cadastrais, cnpj_bases, is_oracle=True)

truncar_tabela("stg_cadastrais")
carregar_dataframe(df, "stg_cadastrais")

# ================================================
# ETAPA 2 - ARRECADAÇÃO (Oracle SAT)
# ================================================
print("[3/9] Extraindo arrecadação...")

query_arrec = """
SELECT   TRIM(r.sgReceita) || ' - ' || tel.nmEventoLancam  AS RECEITA_EVENTO,
  SUBSTR(TRIM(pj.NUCNPJBASE), -8)                    AS CNPJ_BASE,
  SUBSTR(TRIM(pj.NUCNPJBASE), -8) ||
  TRIM(pj.NUCNPJFILIAL) ||
  TRIM(pj.NUDIGITOCNPJ)                              AS CNPJ,
  pj.DSRAZAOSOCIAL                                  AS RAZAO_SOCIAL,
  sf.NMSEGMENTOFISCAL                               AS SEGMENTO_FISCAL,
  sm.DSSITUACAOMOVIMENTO                            AS SITUACAO_MOVIMENTO,
  EXTRACT(YEAR FROM arr.DTPAGTO)                    AS ANO,
  EXTRACT(MONTH FROM arr.DTPAGTO)                   AS MES,
  SUM(arr.VLPAGTO)                                  AS VALOR_PAGO,
      lpad(est.nucgabase, 6, '0') ||
    lpad(est.NUCGAFILIAL, 3, '0') ||
    lpad(est.NUCGADIGITO, 2, '0') AS cga
FROM DBASAT.ESTABELECIMENTO est 
LEFT JOIN DBASAT.MOVIMARRECADRECEITABANCO arr
  ON est.nuCGABase   = arr.nuBase
 AND est.nuCGAFilial = arr.nuFilial
LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
  ON est.NUCNPJBASE   = pj.NUCNPJBASE
 AND est.NUCNPJFILIAL = pj.NUCNPJFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
  ON est.NUCGABASE   = cseg.NUCGABASE
 AND est.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.RECEITA r
  ON r.CDRECEITA = arr.CDRECEITA
LEFT JOIN DBASAT.EVENTOLANCAMENTO tel
  ON tel.cdReceita = r.cdReceita
 AND tel.cdEventoLancam = arr.cdTipoLancamento
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
  ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
LEFT JOIN DBASAT.SITUACAOMOVIMENTO sm
  ON arr.STMOVIMENTO = sm.CDSITUACAOMOVIMENTO
WHERE SUBSTR(TRIM(pj.nucnpjbase), -8) IN ({CNPJ_IN})
  AND arr.DTPAGTO >= TO_DATE('{DATA_INICIO}', 'YYYY-MM-DD')
  AND arr.DTPAGTO < TO_DATE('{DATA_FIM_EXCLUSIVO}', 'YYYY-MM-DD')
GROUP BY   r.sgReceita, tel.nmEventoLancam,
  pj.NUCNPJBASE, pj.NUCNPJFILIAL, pj.NUDIGITOCNPJ,
  pj.DSRAZAOSOCIAL, sf.NMSEGMENTOFISCAL, sm.DSSITUACAOMOVIMENTO,
  EXTRACT(YEAR FROM arr.DTPAGTO), EXTRACT(MONTH FROM arr.DTPAGTO),
  est.nucgabase, est.NUCGAFILIAL, est.NUCGADIGITO
"""

with conectar_oracle(oracle_sat_config) as conn:
    df = executar_select_em_chunks(conn, query_arrec, cnpj_bases, is_oracle=True)

validar_resultado_obrigatorio(df, "stg_arrecadacao")
truncar_tabela("stg_arrecadacao")
carregar_dataframe(df, "stg_arrecadacao")

# ================================================
# ETAPA 3 - NFSe (Oracle DWH)
# ================================================
print("[4/9] Extraindo NFSe...")

query_nfse = """
SELECT   LPAD(co.COD_NRO_CGC_CPF,14,'0')           AS CNPJ,
  LPAD(co.COD_CPF_CNPJ_SINTETICO,8,'0')     AS CNPJ_BASE,
  co.COD_CTBU_MOBI                           AS CGA,
  pe.ano                                     AS ANO,
  pe.mes                                     AS MES,
  at.cod_atvd_rtnc                           AS CODIGO_ATIVIDADE,
  bf.TXT_BENEFICIO_FISCAL                    AS BENEFICIO_FISCAL,
  tr.TXT_TIP_TRBC_NFE                        AS TIPO_TRIBUTACAO,
  rt.TXT_DCR_REGI_TRBC                       AS REGIME_TRIBUTARIO,
  SUM(fp.val_serv_nfe)                       AS VALOR_SERVICO, 
  SUM(fp.VL_ISS_DECLARADO)                   AS VALOR_ISS,
  SUM(fp.vl_iss_proprio_declarado)           AS VALOR_ISS_PROPRIO, 
  SUM(fp.VL_ISS_RETIDO_DECLARADO)            AS VALOR_ISS_RETIDO, 
  SUM(fp.VAL_ISS_PROPRIO_PAGO)               AS VALOR_ISS_PROPRIO_PAGO, 
  SUM(fp.VAL_ISS_RETIDO_PAGO)                AS VALOR_ISS_RETIDO_PAGO, 
  SUM(fp.VAL_DEDC_NFE)                       AS VALOR_DEDUCOES, 
  SUM(fp.VAL_CRED_NFE)                       AS VALOR_CREDITO,
  SUM(fp.qtd_notas)                          AS QTD_NOTAS,
  fp.sk_status_nota
FROM odi_nfse.fato_prestador_agregada fp
JOIN odi_dw.dim_periodo pe
  ON fp.sk_dt_periodo = pe.sk_dt_periodo
JOIN odi_nfse.dim_contribuinte co
  ON fp.sk_contribuinte = co.sk_contribuinte
JOIN odi_nfse.dim_atividade at
  ON fp.sk_atividade_nfe = at.sk_atividade
JOIN odi_nfse.dim_beneficio_fiscal bf
  ON fp.sk_beneficio_fiscal = bf.sk_beneficio_fiscal
JOIN odi_nfse.dim_tributacao tr
  ON fp.sk_tp_tributacao  = tr.sk_tributacao
JOIN odi_nfse.dim_regime_tributario rt
  ON fp.sk_regime_tributario = rt.sk_regime_tributario
WHERE co.COD_CPF_CNPJ_SINTETICO IN ({CNPJ_IN})
  AND (pe.ano * 100 + pe.mes) BETWEEN {COMPETENCIA_INICIO} AND {COMPETENCIA_FIM}
GROUP BY
  LPAD(co.COD_NRO_CGC_CPF,14,'0'),
  LPAD(co.COD_CPF_CNPJ_SINTETICO,8,'0'),
  co.COD_CTBU_MOBI, pe.ano, pe.mes,
  at.cod_atvd_rtnc, bf.TXT_BENEFICIO_FISCAL,
  tr.TXT_TIP_TRBC_NFE, rt.TXT_DCR_REGI_TRBC, fp.sk_status_nota
"""

with conectar_oracle(oracle_dwh_config) as conn:
    df = executar_select_em_chunks(conn, query_nfse, cnpj_bases, is_oracle=True)

truncar_tabela("stg_nfse")
carregar_dataframe(df, "stg_nfse")

# ================================================
# ETAPA 4 - FISCALIZAÇÕES (Oracle SAT)
# ================================================
print("[5/9] Extraindo fiscalizações...")

query_fisc = """
SELECT   p.CDPROGRAMACAO                                  AS NUMERO_PROGRAMACAO,
  SUBSTR(TRIM(pj.NUCNPJBASE), -8) ||
  TRIM(pj.NUCNPJFILIAL) ||
  TRIM(pj.NUDIGITOCNPJ)                            AS CNPJ,
  SUBSTR(TRIM(pj.NUCNPJBASE), -8)                  AS CNPJ_BASE,
  pj.DSRAZAOSOCIAL                                 AS RAZAO_SOCIAL,
  pj.NMFANTASIA                                    AS NOME_FANTASIA,
  pca.NUBASE                                       AS CGA_BASE,
  pca.NUFILIAL                                     AS CGA_FILIAL,
  p.DSPROGRAMACAO                                  AS DESCRICAO_PROGRAMACAO,
  p.DTPROGRAMACAO                                  AS DATA_PROGRAMACAO,
  fs.DTSITUACAOFISCALIZACAO                        AS DATA_SITUACAO_FISCALIZACAO,
  sfisc.DSSITUACAOFISCALIZACAO                     AS SITUACAO_FISCALIZACAO
FROM DBASAT.PROGRAMACAO p
LEFT JOIN DBASAT.PROGRAMACAOCONTRIBUINTEAUDITOR pca
  ON p.CDPROGRAMACAO = pca.CDPROGRAMACAO
LEFT JOIN DBASAT.FISCALIZACAOSITUACAOCONTRIB fs
  ON p.CDPROGRAMACAO = fs.CDPROGRAMACAO
LEFT JOIN DBASAT.SITUACAOFISCALIZACAO sfisc
  ON sfisc.CDSITUACAOFISCALIZACAO = fs.CDSITUACAOFISCALIZACAO
LEFT JOIN DBASAT.ESTABELECIMENTO et
  ON pca.NUBASE = et.NUCGABASE
 AND pca.NUFILIAL = et.NUCGAFILIAL
LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
  ON et.NUCNPJBASE = pj.NUCNPJBASE
 AND et.NUCNPJFILIAL = pj.NUCNPJFILIAL
WHERE SUBSTR(TRIM(pj.nucnpjbase), -8) IN ({CNPJ_IN})
  AND fs.DTSITUACAOFISCALIZACAO >= TO_DATE('{DATA_INICIO}', 'YYYY-MM-DD')
  AND fs.DTSITUACAOFISCALIZACAO < TO_DATE('{DATA_FIM_EXCLUSIVO}', 'YYYY-MM-DD')
"""

with conectar_oracle(oracle_sat_config) as conn:
    df = executar_select_em_chunks(conn, query_fisc, cnpj_bases, is_oracle=True)

truncar_tabela("stg_fiscalizacoes")
carregar_dataframe(df, "stg_fiscalizacoes")

# ================================================
# ETAPA 5 - INADIMPLÊNCIA (Oracle SAT)
# ================================================
print("[6/9] Extraindo inadimplência...")

query_inad = """
SELECT   inp.NUBASE                                      AS CGA_INS,
  inp.NUFILIAL                                    AS CGA_FILIAL,
  inp.NUDIGITO                                    AS CGA_DV,
  SUBSTR(TRIM(inp.NUCPFCNPJBASE), -8)             AS CNPJ_BASE,
  SUBSTR(TRIM(inp.NUCPFCNPJBASE), -8) ||
  TRIM(inp.NUCNPJFILIAL) ||
  TRIM(inp.NUDIGITOCPFCNPJ)                       AS CNPJ,
  pj.DSRAZAOSOCIAL                                AS RAZAO_SOCIAL,
  inp.AAEXERCICIO                                 AS ANO,
  rec.SGRECEITA                                   AS RECEITA,
  SUM(inp.VLDEBITO)                               AS VALOR_DEBITO,
  SUM(inp.VLTOTALDEBITO)                          AS VALOR_TOTAL_DEBITO
FROM DBASAT.INADIMPLENCIA inp
LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
  ON inp.NUCPFCNPJBASE = pj.NUCNPJBASE
 AND inp.NUCNPJFILIAL  = pj.NUCNPJFILIAL
LEFT JOIN DBASAT.RECEITA rec
  ON rec.CDRECEITA = inp.CDRECEITA
LEFT JOIN DBASAT.TIPOCONTRIBUINTE08 tpc
  ON tpc.CDTIPOCONTRIBUINTE = inp.CDTIPOCONTRIBUINTE
WHERE SUBSTR(TRIM(inp.NUCPFCNPJBASE), -8) IN ({CNPJ_IN})
  AND tpc.CDTIPOCONTRIBUINTE = 'E'
  AND inp.AAEXERCICIO BETWEEN {ANO_INICIO} AND {ANO}
GROUP BY
  inp.NUBASE, inp.NUFILIAL, inp.NUDIGITO,
  inp.NUCPFCNPJBASE, inp.NUCNPJFILIAL, inp.NUDIGITOCPFCNPJ,
  pj.DSRAZAOSOCIAL, inp.AAEXERCICIO, rec.SGRECEITA
"""

with conectar_oracle(oracle_sat_config) as conn:
    df = executar_select_em_chunks(conn, query_inad, cnpj_bases, is_oracle=True)

truncar_tabela("stg_inadimplencia")
carregar_dataframe(df, "stg_inadimplencia")

# =========================================================
# 8. Extraindo Contribuinte Municipal (CGA)
# =========================================================
print("[8/10] Extraindo contribuinte municipal (CGA)...")

query_cgm = """
SELECT
    lpad(est.nucgabase, 6, '0') || 
    lpad(est.NUCGAFILIAL, 3, '0') || 
    lpad(est.NUCGADIGITO, 2, '0') AS cga,
    SUBSTR(TRIM(pj.nucnpjbase), -8) ||
    TRIM(pj.nucnpjfilial) ||
    TRIM(pj.nudigitocnpj) AS cnpj,
    tt.dstipotributacao AS tipo_tributacao,
    dc.dsdocumentoconstituicao AS documento_constituicao,
    est.dtinscricaoprefeitura AS data_inscricao_prefeitura,    
    est.dtiniciosituacaocadastral AS data_inicio_situacao_cadastral,
    est.dtfimsituacaocadastral AS data_fim_situacao_cadastral,
    est.nuinscricaoestadual AS inscricao_estadual,
    pj.DSRAZAOSOCIAL AS razao_social, 
    pj.NMFANTASIA AS nome_fantasia, 
    tsit.DSTIPOSITUACAOCADASTRAL AS situacao_cadastral,
    sc.dssituacaocadastral AS motivo_situacao_cadastral, 
    ses.dssituacaoespecial AS situacao_especial,
    lo.cdespecie AS especie_logradouro, 
    lo.DSTITULO AS titulo_logradouro, 
    lo.DSLOGRADOURO AS logradouro,    
    est.dscomplemento AS complemento_endereco,
    est.nuporta1metrico AS numero_porta,
    est.dsedificio AS edificio,
    est.dspontoreferencia AS ponto_referencia,
    est.cdcep AS cep,
    est.cdnaturezajuridica AS natureza_juridica,
    est.nutelefone AS telefone,
    te.dstipoestabelecimento AS tipo_estabelecimento,
    sf.NMSEGMENTOFISCAL AS SEGMENTO_FISCAL
FROM dbasat.estabelecimento est
INNER JOIN dbasat.contribuintepj pj
    ON pj.nucnpjbase = est.nucnpjbase
   AND pj.nucnpjfilial = est.nucnpjfilial
INNER JOIN dbasat.situacaocadastral sc
    ON est.cdsituacaocadastral = sc.cdsituacaocadastral  
INNER JOIN dbasat.tiposituacaocadastral tsit
    ON sc.cdtiposituacaocadastral = tsit.CDTIPOSITUACAOCADASTRAL   
INNER JOIN dbasat.logradouro lo
    ON est.cdlogradouro = lo.cdlogradouro    
INNER JOIN dbasat.tipoestabelecimento te
    ON est.CDTIPOESTABELECIMENTO = te.CDTIPOESTABELECIMENTO
LEFT JOIN dbasat.tipotributacao tt
    ON est.CDTIPOTRIBUTACAO = tt.CDTIPOTRIBUTACAO
LEFT JOIN dbasat.documentoconstituicao dc
    ON est.CDDOCUMENTOCONSTITUICAO = dc.CDDOCUMENTOCONSTITUICAO
LEFT JOIN dbasat.situacaoespecial ses
    ON est.cdsituacaoespecial = ses.cdsituacaoespecial
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg 
    ON est.NUCGABASE = cseg.NUCGABASE AND est.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.SEGMENTOFISCAL sf 
    ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL    
WHERE SUBSTR(TRIM(pj.nucnpjbase), -8) IN ({CNPJ_IN})
"""

with conectar_oracle(oracle_sat_config) as conn:
    df = executar_select_em_chunks(conn, query_cgm, cnpj_bases, is_oracle=True)

validar_resultado_obrigatorio(df, "stg_contribuinte_municipal")
truncar_tabela("stg_contribuinte_municipal")
carregar_dataframe(df, "stg_contribuinte_municipal")

# ================================================
# ETAPA 6 - RFB (Postgres externo)
# ================================================
print("[7/9] Extraindo dados da RFB...")

query_rfb_empresas = """
SELECT
  LPAD(e.cnpj_basico, 8, '0')                        AS cnpj_base,
  e.razao_social,
  e.natureza_juridica                                 AS natureza_juridica_cod,
  nj.descricao                                        AS natureza_juridica_desc,
  e.qualificacao_responsavel,
  e.porte_empresa,
  e.ente_federativo_responsavel,
  e.capital_social
FROM public.empresas e
LEFT JOIN public.natureza_juridica nj
       ON nj.codigo = e.natureza_juridica
WHERE e.cnpj_basico::bigint IN ({CNPJ_IN})
"""

query_rfb_estab = """
SELECT   LPAD(est.cnpj_basico, 8, '0')                                         AS cnpj_base,
  LPAD(est.cnpj_basico, 8, '0') || LPAD(est.cnpj_ordem, 4, '0') ||
  LPAD(est.cnpj_dv, 2, '0')                                             AS cnpj,
  est.matriz_filial,
  est.nome_fantasia,
  est.situacao_cadastral,
  est.data_situacao_cadastral,
  est.motivo_situacao_cadastral,
  est.data_inicio_atividades,
  est.cnae_fiscal,
  est.cnae_fiscal_secundaria,
  est.tipo_logradouro, 
  est.logradouro, 
  est.numero, 
  est.complemento,
  est.bairro, 
  est.cep, 
  est.uf,
  est.municipio                                                          AS municipio_cod
FROM public.estabelecimento est  
WHERE est.cnpj_basico::bigint IN ({CNPJ_IN})
"""

query_rfb_socios = """
SELECT   LPAD(s.cnpj_basico, 8, '0')         AS cnpj_base,
  s.cnpj                               AS cnpj,
  s.identificador_de_socio,
  s.nome_socio,
  s.cnpj_cpf_socio,
  s.qualificacao_socio,
  s.data_entrada_sociedade,
  s.pais,
  s.representante_legal,
  s.nome_representante,
  s.qualificacao_representante_legal,
  s.faixa_etaria
FROM public.socios s
WHERE s.cnpj_basico::bigint IN ({CNPJ_IN})
"""

query_rfb_simples = """
SELECT   LPAD(s.cnpj_basico, 8, '0') AS cnpj_base,
  s.opcao_simples,
  s.data_opcao_simples,
  s.data_exclusao_simples,
  s.opcao_mei,
  s.data_opcao_mei,
  s.data_exclusao_mei
FROM public.simples s  
WHERE s.cnpj_basico::bigint IN ({CNPJ_IN})
"""

print("[7.1/9] Extraindo dados da empresa...")
df = executar_select_em_chunks(pg_engine_rfb, query_rfb_empresas, cnpj_bases, is_oracle=False)
truncar_tabela("stg_rfb_empresas")
carregar_dataframe(df, "stg_rfb_empresas")

print("[7.2/9] Extraindo dados do estabelecimento...")
df = executar_select_em_chunks(pg_engine_rfb, query_rfb_estab, cnpj_bases, is_oracle=False)
df = limpar_datas(df, ["data_situacao_cadastral", "data_inicio_atividades"])
truncar_tabela("stg_rfb_estabelecimentos")
carregar_dataframe(df, "stg_rfb_estabelecimentos")

print("[7.3/9] Extraindo dados dos sócios...")
df = executar_select_em_chunks(pg_engine_rfb, query_rfb_socios, cnpj_bases, is_oracle=False)
df = limpar_datas(df, ["data_entrada_sociedade"])
truncar_tabela("stg_rfb_socios")
carregar_dataframe(df, "stg_rfb_socios")

print("[7.4/9] Extraindo dados do simples...")
df = executar_select_em_chunks(pg_engine_rfb, query_rfb_simples, cnpj_bases, is_oracle=False)
df = limpar_datas(df, ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"])
truncar_tabela("stg_rfb_simples")
carregar_dataframe(df, "stg_rfb_simples")

print("[9/9] Processo concluído com sucesso ")
