# etl_stage_fiscalizacoes.py (v4) — SAT + filtro Contribuintes Suporte
# - Conexão Oracle SAT conforme solicitado
# - Filtra por CNPJs base de suporte (tabela {SCHEMA}.stg_contribuintes_suporte, por ANO)
# - Extrai 4 staging tables: stg_fisc_programacoes, stg_fisc_situacoes, stg_fisc_autos, stg_fisc_valores_autos
# - Ajustado para filtrar NUCNPJBASE em lotes de até 999 valores (limite do Oracle para IN)

import os
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text

from periodo_processamento import obter_periodo

# ================================================
# PARAMETRIZAÇÃO
# ================================================
PERIODO = obter_periodo(ano_fim_padrao=2025)
ANO_INICIO = PERIODO.inicio.year
ANO = PERIODO.fim.year
SCHEMA = "suporte_arrecadacao"

# Datas já validadas; o formato abaixo é o esperado pelas consultas Oracle.
BEGIN_DATE = PERIODO.inicio.strftime("%d/%m/%Y")
END_DATE_EXCLUSIVE = PERIODO.fim_exclusivo.strftime("%d/%m/%Y")
COMPETENCIA_INICIO = PERIODO.competencia_inicio
COMPETENCIA_FIM = PERIODO.competencia_fim

# Conexão Oracle SAT (conforme solicitado)
oracle_sat_config = {
    "user": "AFROCHA",
    "password": os.getenv("SAT_PASS", ""),
    "dsn": "pituba:2118/SAT"
}

# Conexão Postgres (SEFAZ)
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}
pg_engine_sefaz = create_engine(
    f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
)

# ================================================
# UTILITARIOS
# ================================================
def conectar_oracle(config):
    return cx_Oracle.connect(config["user"], config["password"], config["dsn"])

def executar_select(engine, query):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

def truncar_tabela(tabela):
    with pg_engine_sefaz.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{tabela}"))

def carregar_dataframe(df, tabela):
    if not df.empty:
        df.columns = [col.lower() for col in df.columns]
        df.to_sql(tabela, pg_engine_sefaz, schema=SCHEMA, if_exists="append", index=False, chunksize=1000)
        print(f"   -> {len(df)} registros inseridos em {tabela}")
    else:
        print(f"   -> Nenhum registro retornado para {tabela}")

def chunk_list(lst, size):
    """Quebra uma lista em pedaços de até 'size' elementos."""
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def formatar_lista_sql_texto(valores):
    """Formata valores textuais já validados para uso nos lotes do IN."""
    return ",".join(
        f"'{valor.replace(chr(39), chr(39) * 2)}'" for valor in valores
    )

# ================================================
# 0) LISTA DE CONTRIBUINTES SUPORTE (CNPJ_BASE)
# ================================================
print("[1/5] Carregando CNPJs base dos contribuintes suporte...")
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

if not cnpj_bases:
    raise SystemExit("Nenhum CNPJ base encontrado em stg_contribuintes_suporte para o ANO informado.")

# ================================================
# 1) PROGRAMACOES
# ================================================
print("[2/5] Extraindo Programações (apenas contribuintes suporte)...")

truncar_tabela("stg_fisc_programacoes")

with conectar_oracle(oracle_sat_config) as conn_oracle:
    dfs_chunks = []

    for idx, chunk in enumerate(chunk_list(cnpj_bases, 999), start=1):
        print(f"   -> Lote Programações {idx}: {len(chunk)} CNPJs base")
        # NUCNPJBASE é VARCHAR2(9) e possui um prefixo técnico; a raiz são os 8 caracteres finais.
        cnpj_in = formatar_lista_sql_texto(chunk)

        SQL_PROGRAMACOES = f"""
        SELECT p.CDPROGRAMACAO AS NUMERO_PROGRAMACAO
             , sa.NMSERVIDOR AS NOME_AUDITOR
             , pj.DSRAZAOSOCIAL AS RAZAO_SOCIAL
             , sf.NMSEGMENTOFISCAL AS SEGMENTO_FISCAL
             , CASE WHEN p.CDTIPOCONTRIBUINTE = 1 THEN
                        LPAD(et.NUCGABASE, 6, '0') || LPAD(et.NUCGAFILIAL, 3, '0') || LPAD(et.NUCGADIGITO, 2, '0')
                    ELSE LPAD(pr.CDINSCRICAOIMOB, 6, '0') || pr.NUDIGITOINSCRICAOIMOB
               END AS INSCRICAO_MUNICIPAL
             , CASE WHEN p.CDTIPOCONTRIBUINTE = 1 THEN 'ATIVIDADE' ELSE 'IMÓVEL' END AS TIPO_CONTRIBUINTE
             , p.DSPROGRAMACAO AS DESCRICAO_PROGRAMACAO
             , e.DSEXTENSAO AS EXTENSAO
             , p.MMREFERENCIA || '/' || p.AAREFERENCIA AS MES_ANO_REFERENCIA
             , tp.DSTIPOPROGRAMACAO AS TIPO_PROGRAMACAO
        FROM DBASAT.PROGRAMACAO p
        LEFT JOIN DBASAT.TIPOCONTRIBUINTE tct ON p.CDTIPOCONTRIBUINTE = tct.CDTIPOCONTRIBUINTE
        LEFT JOIN DBASAT.PROGRAMACAOCONTRIBUINTEAUDITOR pca ON p.CDPROGRAMACAO = pca.CDPROGRAMACAO
        LEFT JOIN DBASAT.PROPRIEDADE pr ON pca.NUBASE = pr.CDINSCRICAOIMOB
        LEFT JOIN DBASAT.ESTABELECIMENTO et ON pca.NUBASE = et.NUCGABASE AND pca.NUFILIAL = et.NUCGAFILIAL
        LEFT JOIN DBASAT.CONTRIBUINTEPJ pj ON et.NUCNPJBASE = pj.NUCNPJBASE AND et.NUCNPJFILIAL = pj.NUCNPJFILIAL
        LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg ON et.NUCGABASE = cseg.NUCGABASE AND et.NUCGAFILIAL = cseg.NUCGAFILIAL
        LEFT JOIN DBASAT.SEGMENTOFISCAL sf ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
        LEFT JOIN DBASAT.PROGRAMACAOAUDITOR pa ON p.CDPROGRAMACAO = pa.CDPROGRAMACAO
        LEFT JOIN DBASAT.SERVIDOR_AUDITOR sa ON pa.CDSERVIDOR = sa.CDSERVIDOR
        LEFT JOIN DBASAT.EXTENSAO e ON p.CDEXTENSAO = e.CDEXTENSAO
        LEFT JOIN DBASAT.TIPOPROGRAMACAO tp ON p.CDTIPOPROGRAMACAO = tp.CDTIPOPROGRAMACAO
        WHERE p.DTPROGRAMACAO < TO_DATE('{END_DATE_EXCLUSIVE}', 'DD/MM/YYYY')
          AND SUBSTR(TRIM(pj.NUCNPJBASE), -8) IN ({cnpj_in})
        """

        df_chunk = pd.read_sql(SQL_PROGRAMACOES, conn_oracle)
        dfs_chunks.append(df_chunk)

    df_programacoes = pd.concat(dfs_chunks, ignore_index=True) if dfs_chunks else pd.DataFrame()

carregar_dataframe(df_programacoes, "stg_fisc_programacoes")

# ================================================
# 2) SITUACOES HISTORICAS (apenas contribuintes suporte)
# ================================================
print("[3/5] Extraindo Situações Históricas (apenas contribuintes suporte)...")

truncar_tabela("stg_fisc_situacoes")

with conectar_oracle(oracle_sat_config) as conn_oracle:
    dfs_chunks = []

    for idx, chunk in enumerate(chunk_list(cnpj_bases, 999), start=1):
        print(f"   -> Lote Situações {idx}: {len(chunk)} CNPJs base")
        cnpj_in = formatar_lista_sql_texto(chunk)

        SQL_SITUACOES = f"""
        SELECT p.CDPROGRAMACAO AS numero_programacao
             , pca.NUBASE
             , pca.NUFILIAL
             , fs.DTSITUACAOFISCALIZACAO
             , sfisc.DSSITUACAOFISCALIZACAO
        FROM DBASAT.PROGRAMACAO p
        LEFT JOIN DBASAT.PROGRAMACAOCONTRIBUINTEAUDITOR pca ON p.CDPROGRAMACAO = pca.CDPROGRAMACAO
        LEFT JOIN DBASAT.FISCALIZACAOSITUACAOCONTRIB fs ON p.CDPROGRAMACAO = fs.CDPROGRAMACAO
        LEFT JOIN DBASAT.SITUACAOFISCALIZACAO sfisc ON sfisc.CDSITUACAOFISCALIZACAO = fs.CDSITUACAOFISCALIZACAO
        LEFT JOIN DBASAT.ESTABELECIMENTO et ON pca.NUBASE = et.NUCGABASE AND pca.NUFILIAL = et.NUCGAFILIAL
        LEFT JOIN DBASAT.CONTRIBUINTEPJ pj ON et.NUCNPJBASE = pj.NUCNPJBASE AND et.NUCNPJFILIAL = pj.NUCNPJFILIAL
        WHERE fs.DTSITUACAOFISCALIZACAO >= TO_DATE('{BEGIN_DATE}', 'DD/MM/YYYY')
          AND fs.DTSITUACAOFISCALIZACAO < TO_DATE('{END_DATE_EXCLUSIVE}', 'DD/MM/YYYY')
          AND SUBSTR(TRIM(pj.NUCNPJBASE), -8) IN ({cnpj_in})
        ORDER BY fs.DTSITUACAOFISCALIZACAO DESC
        """

        df_chunk = pd.read_sql(SQL_SITUACOES, conn_oracle)
        dfs_chunks.append(df_chunk)

    df_situacoes = pd.concat(dfs_chunks, ignore_index=True) if dfs_chunks else pd.DataFrame()

carregar_dataframe(df_situacoes, "stg_fisc_situacoes")

# ================================================
# 3) AUTOS/NOTAS LAVRADOS (apenas contribuintes suporte)
# ================================================
print("[4/5] Extraindo Autos/Notificações (apenas contribuintes suporte)...")

truncar_tabela("stg_fisc_autos")

with conectar_oracle(oracle_sat_config) as conn_oracle:
    dfs_chunks = []

    for idx, chunk in enumerate(chunk_list(cnpj_bases, 999), start=1):
        print(f"   -> Lote Autos {idx}: {len(chunk)} CNPJs base")
        cnpj_in = formatar_lista_sql_texto(chunk)

        SQL_AUTOS = f"""
        SELECT p.CDPROGRAMACAO AS NUMERO_PROGRAMACAO
             , sat.NMSERVIDOR AS NOME_AUDITOR
             , sf.NMSEGMENTOFISCAL AS SEGMENTO_FISCAL
             , pj.DSRAZAOSOCIAL AS RAZAO_SOCIAL
             , CASE WHEN p.CDTIPOCONTRIBUINTE = 1 THEN
                        LPAD(et.NUCGABASE, 6, '0') || LPAD(et.NUCGAFILIAL, 3, '0') || LPAD(et.NUCGADIGITO, 2, '0')
                    ELSE LPAD(pr.CDINSCRICAOIMOB, 6, '0') || pr.NUDIGITOINSCRICAOIMOB
               END AS INSCRICAO_MUNICIPAL
             , CASE WHEN p.CDTIPOCONTRIBUINTE = 1 THEN 'ATIVIDADE' ELSE 'IMÓVEL' END AS TIPO_CONTRIBUINTE
             , p.DSPROGRAMACAO AS DESCRICAO_PROGRAMACAO
             , e.DSEXTENSAO AS EXTENSAO
             , p.MMREFERENCIA || '/' || p.AAREFERENCIA AS MES_ANO_REFERENCIA
             , tp.DSTIPOPROGRAMACAO AS TIPO_PROGRAMACAO
             , nai.NUNOTAUTO AS NUMERO_NOTIFICACAO
             , nai.AANOTAUTO AS ANO_NOTIFICACAO
             , sa.DSSITUACAOAUTO AS SITUACAO_AUTO
             , tna.DSTIPONOTAUTO AS TIPO_NOTIFICACAO
             , f.DSFATO AS FATO
             , nai.DTREGISTRONOTAUTO AS DATA_REGISTRO_AUTO
        FROM DBASAT.NOTIFICACAOAUTODEINFRACAO nai
        LEFT JOIN DBASAT.SITUACAOAUTO sa ON nai.CDSITUACAOAUTOATUAL = sa.CDSITUACAOAUTO
        LEFT JOIN DBASAT.FATO f ON nai.CDFATO = f.CDFATO
        LEFT JOIN DBASAT.TIPONOTAUTO tna ON nai.STTIPONOTAUTO = tna.CDTIPONOTAUTO
        LEFT JOIN DBASAT.PROGRAMACAO p ON nai.CDPROGRAMACAO = p.CDPROGRAMACAO
        LEFT JOIN DBASAT.PROPRIEDADE pr ON nai.NUBASEPROG = pr.CDINSCRICAOIMOB
        LEFT JOIN DBASAT.TIPOCONTRIBUINTE tct ON p.CDTIPOCONTRIBUINTE = tct.CDTIPOCONTRIBUINTE
        LEFT JOIN DBASAT.ESTABELECIMENTO et ON nai.NUBASEPROG = et.NUCGABASE AND nai.NUFILIALPROG = et.NUCGAFILIAL
        LEFT JOIN DBASAT.CONTRIBUINTEPJ pj ON et.NUCNPJBASE = pj.NUCNPJBASE AND et.NUCNPJFILIAL = pj.NUCNPJFILIAL
        LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg ON et.NUCGABASE = cseg.NUCGABASE AND et.NUCGAFILIAL = cseg.NUCGAFILIAL
        LEFT JOIN DBASAT.SEGMENTOFISCAL sf ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
        LEFT JOIN DBASAT.SERVIDOR_AUDITOR sat ON nai.CDSERVIDORPROG = sat.CDSERVIDOR
        LEFT JOIN DBASAT.EXTENSAO e ON p.CDEXTENSAO = e.CDEXTENSAO
        LEFT JOIN DBASAT.TIPOPROGRAMACAO tp ON p.CDTIPOPROGRAMACAO = tp.CDTIPOPROGRAMACAO
        WHERE nai.DTREGISTRONOTAUTO >= TO_DATE('{BEGIN_DATE}', 'DD/MM/YYYY')
          AND nai.DTREGISTRONOTAUTO < TO_DATE('{END_DATE_EXCLUSIVE}', 'DD/MM/YYYY')
          AND SUBSTR(TRIM(pj.NUCNPJBASE), -8) IN ({cnpj_in})
        """

        df_chunk = pd.read_sql(SQL_AUTOS, conn_oracle)
        dfs_chunks.append(df_chunk)

    df_autos = pd.concat(dfs_chunks, ignore_index=True) if dfs_chunks else pd.DataFrame()

carregar_dataframe(df_autos, "stg_fisc_autos")

# ================================================
# 4) VALORES POR NOTA/AUTO (apenas contribuintes suporte)
# ================================================
print("[5/5] Extraindo Valores das Notificações/Autos (apenas contribuintes suporte)...")

truncar_tabela("stg_fisc_valores_autos")

with conectar_oracle(oracle_sat_config) as conn_oracle:
    dfs_chunks = []

    for idx, chunk in enumerate(chunk_list(cnpj_bases, 999), start=1):
        print(f"   -> Lote Valores Autos {idx}: {len(chunk)} CNPJs base")
        cnpj_in = formatar_lista_sql_texto(chunk)

        SQL_VALORES_AUTOS = f"""
        SELECT p.CDPROGRAMACAO AS NUMERO_PROGRAMACAO
             , nai.NUNOTAUTO AS NUMERO_NOTIFICACAO
             , vna.NUCOTA AS MES
             , nai.AANOTAUTO AS ANO
             , r.SGRECEITA AS SIGLA_RECEITA
             , vna.VLMOEDAORIGINAL AS VL_MOEDA_ORIGINAL
             , vna.VLCORRECAOMONETARIA  AS VL_CORRECAO_MONETARIA
             , vna.VLMULTAINFRACAO AS VL_MULTA_INFRACAO
             , vna.VLMULTAMORA AS VL_MULTA_MORA
             , vna.VLJUROSMORA AS VL_JUROS_MORA
        FROM DBASAT.NOTIFICACAOAUTODEINFRACAO nai
        LEFT JOIN DBASAT.VALORNOTAUTO vna ON nai.NUNOTAUTO = vna.NUNOTAUTO AND nai.AANOTAUTO = vna.AANOTAUTO
        LEFT JOIN DBASAT.RECEITA r ON vna.CDRECEITA = r.CDRECEITA
        LEFT JOIN DBASAT.PROGRAMACAO p ON nai.CDPROGRAMACAO = p.CDPROGRAMACAO
        LEFT JOIN DBASAT.ESTABELECIMENTO et ON nai.NUBASEPROG = et.NUCGABASE AND nai.NUFILIALPROG = et.NUCGAFILIAL
        LEFT JOIN DBASAT.CONTRIBUINTEPJ pj ON et.NUCNPJBASE = pj.NUCNPJBASE AND et.NUCNPJFILIAL = pj.NUCNPJFILIAL
        WHERE (nai.AANOTAUTO * 100 + vna.NUCOTA)
              BETWEEN {COMPETENCIA_INICIO} AND {COMPETENCIA_FIM}
          AND vna.NUCOTA BETWEEN 1 AND 12
          AND SUBSTR(TRIM(pj.NUCNPJBASE), -8) IN ({cnpj_in})
        """

        df_chunk = pd.read_sql(SQL_VALORES_AUTOS, conn_oracle)
        dfs_chunks.append(df_chunk)

    df_valores_autos = pd.concat(dfs_chunks, ignore_index=True) if dfs_chunks else pd.DataFrame()

carregar_dataframe(df_valores_autos, "stg_fisc_valores_autos")

print("[OK] Extração para staging (fiscalizações) concluída com sucesso")
