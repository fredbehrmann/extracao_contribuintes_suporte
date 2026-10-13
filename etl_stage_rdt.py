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

oracle_sat_config = {
    "user": "AFROCHA",
    "password": os.getenv("SAT_PASS", ""),
    "dsn": "pituba:2118/SAT"
}

pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432
}

PG_URL = (
    f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}"
    f"@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
)
pg_engine = create_engine(PG_URL)

# ================================================
# HELPERS
# ================================================
def conectar_oracle(cfg):
    return cx_Oracle.connect(cfg["user"], cfg["password"], cfg["dsn"])

def truncar_tabela(nome):
    with pg_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{nome}"))

def carregar_dataframe(df, tabela):
    if df.empty:
        print(f"   -> Nenhum registro retornado para {tabela}")
        return
    df.columns = [c.lower() for c in df.columns]
    df.to_sql(tabela, pg_engine, schema=SCHEMA, if_exists="append", index=False, chunksize=1000)
    print(f"   -> {len(df)} registros inseridos em {tabela}")

def chunk_list(lst, size=999):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def oracle_select_df(conn, sql: str, params: dict) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=cols)

def make_varchar2_list(conn, values):
    """
    Cria um objeto do tipo SYS.ODCIVARCHAR2LIST para bind em SQL.
    """
    t = conn.gettype("SYS.ODCIVARCHAR2LIST")
    obj = t.newobject()
    obj.extend(values)
    return obj

# ================================================
# QUERY (todos os RDT; filtro por lista VARCHAR2 bindada)
# NUCNPJBASE é VARCHAR2(9) e possui um prefixo técnico; a raiz são os 8 caracteres finais.
# ================================================
QUERY_RDT = """
SELECT
      rdt.SQRESUMODECLARACAOTRIBUTARIA AS SQ_RDT
    , rdt.NURESUMODECLARACAOTRIBUTARIA AS NUMERO_RDT
    , rdt.AAGERACAO AS ANO_GERACAO

    , SUBSTR(TRIM(pj.NUCNPJBASE), -8) AS CNPJ_BASE
    , SUBSTR(TRIM(pj.NUCNPJBASE), -8) || TRIM(pj.NUCNPJFILIAL) || TRIM(pj.NUDIGITOCNPJ) AS CNPJ
    , LPAD(est.NUCGABASE, 6, '0') || LPAD(est.NUCGAFILIAL, 3, '0') || LPAD(est.NUCGADIGITO, 2, '0') AS CGA

    ,  nvl(tipotrib.DSTIPOTRIBUTACAO,'Não Informado') AS TIPO_TRIBUTACAO

    , CASE
        WHEN est.FLSUBSTITUTOTRIBUT = 'S' THEN 'Substituto Tributário'
        WHEN est.FLSUBSTITUTOTRIBUT = 'N' THEN 'Não Substituto Tributário'
        ELSE 'Não Informado'
      END AS SUBSTITUTO_TRIBUTARIO_DESC

    , rdt.AACOMPETENCIA AS ANO
    , rdt.MMCOMPETENCIA AS MES

    , RTRIM(rec.SGRECEITA) AS RECEITA
    , evento.NMEVENTOLANCAM AS EVENTO_LANCAMENTO
    , loc.DSLOCALIZACAORDT AS LOCALIZACAO_RDT

    , rdt.DTINCLUSAOSAT AS DATA_INCLUSAO_SAT

    , CASE
        WHEN rdt.FLRDTBLOQUEADO = 1 THEN 'Bloqueado'
        WHEN rdt.FLRDTBLOQUEADO = 0 THEN 'Desbloqueado'
        ELSE 'Não Informado'
      END AS RDT_BLOQUEIO_DESC

    , CASE
        WHEN rdt.FLEXIGIBILIDADESUSPENSA = 1 THEN 'Inexigível'
        WHEN rdt.FLEXIGIBILIDADESUSPENSA = 0 THEN 'Exigível'
        ELSE 'Não Informado'
      END AS EXIGIBILIDADE_DESC

    , CASE
        WHEN rdt.FLRDTLIQUIDADO = 1 THEN 'Liquidado'
        WHEN rdt.FLRDTLIQUIDADO = 2 THEN 'Parcialmente Liquidado'
        WHEN rdt.FLRDTLIQUIDADO = 0 OR rdt.FLRDTLIQUIDADO IS NULL THEN 'Sem Liquidação'
        ELSE 'Não Informado'
      END AS LIQUIDACAO_DESC

    , CASE
        WHEN rdt.FLPAGO = 1 THEN 'Pago'
        WHEN rdt.FLPAGO = 0 THEN 'Não Pago'
        ELSE 'Não Informado'
      END AS PAGAMENTO_DESC

    , CASE
        WHEN rdt.CDSISTEMARDT = 1 THEN 'NOTA SALVADOR'
        WHEN rdt.CDSISTEMARDT = 2 THEN 'DMS EDUCAÇÃO'
        ELSE 'Outro/Não Informado'
      END AS SISTEMA_RDT_DESC

    , rdt.VLRESUMODECLARACAOTRIBUTARIA AS VALOR_RDT

FROM DBASATFIN.RESUMODECLARACAOTRIBUTARIA rdt
    LEFT JOIN DBASAT.ESTABELECIMENTO est
        ON rdt.NUCGABASE = est.NUCGABASE AND rdt.NUCGAFILIAL = est.NUCGAFILIAL
    LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
        ON est.NUCNPJBASE = pj.NUCNPJBASE AND est.NUCNPJFILIAL = pj.NUCNPJFILIAL
    LEFT JOIN DBASAT.TIPOTRIBUTACAO tipotrib
        ON est.CDTIPOTRIBUTACAO = tipotrib.CDTIPOTRIBUTACAO
    LEFT JOIN DBASAT.RECEITA rec
        ON rdt.CDRECEITA = rec.CDRECEITA
    LEFT JOIN DBASAT.EVENTOLANCAMENTO evento
        ON rdt.CDEVENTOLANCAM = evento.CDEVENTOLANCAM AND rdt.CDRECEITA = evento.CDRECEITA
    LEFT JOIN DBASATFIN.LOCALIZACAORDT loc
        ON rdt.CDSITUACAOLOCALIZACAORDT = loc.CDLOCALIZACAORDT

WHERE SUBSTR(TRIM(pj.NUCNPJBASE), -8) IN (SELECT COLUMN_VALUE FROM TABLE(:cnpjs))
  AND (rdt.AACOMPETENCIA * 100 + rdt.MMCOMPETENCIA)
      BETWEEN :comp_inicio AND :comp_fim
"""

def main():
    print("[1/3] Carregando lista de contribuintes suporte...")
    with pg_engine.connect() as conn:
        df_suporte = pd.read_sql(
            text(f"""
                SELECT DISTINCT cnpj_base
                FROM {SCHEMA}.stg_contribuintes_suporte
                WHERE ano BETWEEN :ano_inicio AND :ano_fim
            """),
            conn,
            params={"ano_inicio": ANO_INICIO, "ano_fim": ANO},
        )

    # Mantém CNPJ_BASE como texto para preservar zeros à esquerda.
    cnpj_bases = (
        df_suporte["cnpj_base"]
        .dropna()
        .astype("string")
        .str.strip()
    )
    cnpj_bases = cnpj_bases[cnpj_bases.str.fullmatch(r"[0-9A-Za-z]{8}", na=False)]
    cnpj_bases = cnpj_bases.drop_duplicates().tolist()

    print(f"   -> {len(cnpj_bases)} CNPJs base válidos carregados")

    print("[2/3] Truncando staging...")
    truncar_tabela("stg_rdt")

    print("[3/3] Extraindo e gravando RDT em chunks (999)...")
    total = 0
    with conectar_oracle(oracle_sat_config) as conn_oracle:
        for chunk in chunk_list(cnpj_bases, 999):
            cnpj_list_obj = make_varchar2_list(conn_oracle, chunk)
            df = oracle_select_df(
                conn_oracle,
                QUERY_RDT,
                {
                    "cnpjs": cnpj_list_obj,
                    "comp_inicio": PERIODO.competencia_inicio,
                    "comp_fim": PERIODO.competencia_fim,
                },
            )

            if not df.empty:
                for col in ["CNPJ", "CGA"]:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip()

                carregar_dataframe(df, "stg_rdt")
                total += len(df)

    print(f"[OK] stg_rdt carregada (TODOS os RDT). Linhas: {total}")

if __name__ == "__main__":
    main()
