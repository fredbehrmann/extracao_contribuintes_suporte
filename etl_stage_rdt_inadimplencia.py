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

PG_URL = f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}@" \
         f"{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
pg_engine = create_engine(PG_URL)

def conectar_oracle(cfg):
    return cx_Oracle.connect(cfg["user"], cfg["password"], cfg["dsn"])

def truncar_tabela(nome):
    with pg_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{nome}"))

def carregar_dataframe(df, tabela):
    if df.empty:
        return
    df.columns = [c.lower() for c in df.columns]
    df.to_sql(tabela, pg_engine, schema=SCHEMA, if_exists="append", index=False, chunksize=1000)

def chunk_list(lst, size=999):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

QUERY_INAD_TEMPLATE = """
SELECT
      ROWIDTOCHAR(inad.ROWID) AS ID_INAD_ORIGEM
    , rdt.SQRESUMODECLARACAOTRIBUTARIA AS SQ_RDT
    , rdt.AACOMPETENCIA AS ANO
    , rdt.MMCOMPETENCIA AS MES

    -- para resolver dim_contribuinte_municipal na carga (opcional, mas útil)
    , SUBSTR(TRIM(pj.NUCNPJBASE), -8) AS CNPJ_BASE
    , SUBSTR(TRIM(pj.NUCNPJBASE), -8) || TRIM(pj.NUCNPJFILIAL) || TRIM(pj.NUDIGITOCNPJ) AS CNPJ
    , LPAD(est.NUCGABASE, 6, '0') || LPAD(est.NUCGAFILIAL, 3, '0') || LPAD(est.NUCGADIGITO, 2, '0') AS CGA

    , inad.DTENTRADA AS DATA_ENTRADA_INADIMPLENCIA
    , inad.DTSAIDA   AS DATA_SAIDA_INADIMPLENCIA

    , inad.VLSALDOORIGINAL       AS VALOR_SALDO_ORIGINAL_INAD
    , inad.VLMULTAMORA           AS VALOR_MULTA_MORA_INAD
    , inad.VLJUROSMORA           AS VALOR_JUROS_MORA_INAD
    , inad.VLCORRECAOMONETARIA   AS VALOR_CORRECAO_INAD
    , inad.VLTOTALDEBITO         AS VALOR_TOTAL_DEBITO_INAD
    , inad.VLORIGINAL            AS VALOR_ORIGINAL_INAD

FROM DBASATFIN.INADIMPLENCIADEBITOSRDT inad
    JOIN DBASATFIN.RESUMODECLARACAOTRIBUTARIA rdt
        ON rdt.SQRESUMODECLARACAOTRIBUTARIA = inad.SQRESUMODECLARACAOTRIBUTARIA
    LEFT JOIN DBASAT.ESTABELECIMENTO est
        ON rdt.NUCGABASE = est.NUCGABASE AND rdt.NUCGAFILIAL = est.NUCGAFILIAL
    LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
        ON est.NUCNPJBASE = pj.NUCNPJBASE AND est.NUCNPJFILIAL = pj.NUCNPJFILIAL

WHERE SUBSTR(TRIM(pj.NUCNPJBASE), -8) IN ({CNPJ_IN})
  AND inad.DTENTRADA IS NOT NULL
  AND (rdt.AACOMPETENCIA * 100 + rdt.MMCOMPETENCIA)
      BETWEEN {COMP_INICIO} AND {COMP_FIM}
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
    cnpj_bases = (
        df_suporte["cnpj_base"]
        .dropna()
        .astype("string")
        .str.strip()
    )
    cnpj_bases = cnpj_bases[cnpj_bases.str.fullmatch(r"[0-9A-Za-z]{8}", na=False)]
    cnpj_bases = cnpj_bases.drop_duplicates().tolist()
    print(f"   -> {len(cnpj_bases)} CNPJs base carregados")

    print("[2/3] Truncando staging...")
    truncar_tabela("stg_rdt_inadimplencia")

    print("[3/3] Extraindo e gravando inadimplência em chunks (999)...")
    total = 0
    with conectar_oracle(oracle_sat_config) as conn_oracle:
        for chunk in chunk_list(cnpj_bases, 999):
            cnpj_in = ",".join(
                f"'{cnpj.replace(chr(39), chr(39) * 2)}'" for cnpj in chunk
            )
            query = QUERY_INAD_TEMPLATE.format(
                CNPJ_IN=cnpj_in,
                COMP_INICIO=PERIODO.competencia_inicio,
                COMP_FIM=PERIODO.competencia_fim,
            )
            df = pd.read_sql(query, conn_oracle)
            if not df.empty:
                for col in ["CNPJ", "CGA", "ID_INAD_ORIGEM"]:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip()
                carregar_dataframe(df, "stg_rdt_inadimplencia")
                total += len(df)

    print(f"[OK] stg_rdt_inadimplencia carregada. Linhas: {total}")

if __name__ == "__main__":
    main()
