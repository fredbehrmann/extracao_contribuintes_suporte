import os
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text

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

def conectar_oracle(cfg):
    return cx_Oracle.connect(cfg["user"], cfg["password"], cfg["dsn"])

def carregar_dataframe(df, tabela):
    if df.empty:
        return
    df.columns = [c.lower() for c in df.columns]
    df.to_sql(tabela, pg_engine, schema=SCHEMA, if_exists="append", index=False, chunksize=1000)
    print(f"   -> {len(df)} linhas inseridas em {SCHEMA}.{tabela}")

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

def make_number_list(conn, values):
    t = conn.gettype("SYS.ODCINUMBERLIST")
    obj = t.newobject()
    obj.extend([int(v) for v in values])
    return obj

QUERY_RDT_POR_SQ = """
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
WHERE rdt.SQRESUMODECLARACAOTRIBUTARIA IN (SELECT COLUMN_VALUE FROM TABLE(:sqs))
"""

def main():
    print("[1/2] Buscando SQ_RDT faltantes no Postgres...")
    with pg_engine.connect() as conn:
        df_missing = pd.read_sql(text(f"""
            SELECT DISTINCT si.sq_rdt
            FROM {SCHEMA}.stg_rdt_inadimplencia si
            LEFT JOIN {SCHEMA}.stg_rdt sr ON sr.sq_rdt = si.sq_rdt
            WHERE sr.sq_rdt IS NULL
        """), conn)

    missing = df_missing["sq_rdt"].dropna().astype(int).tolist()
    print(f"   -> SQ_RDT faltantes: {len(missing)}")

    if not missing:
        print("[OK] Nenhum SQ_RDT faltante. Nada a fazer.")
        return

    print("[2/2] Complementando stg_rdt a partir do Oracle...")
    total = 0
    with conectar_oracle(oracle_sat_config) as conn_oracle:
        for chunk in chunk_list(missing, 999):
            sq_list_obj = make_number_list(conn_oracle, chunk)
            df = oracle_select_df(conn_oracle, QUERY_RDT_POR_SQ, {"sqs": sq_list_obj})
            if not df.empty:
                carregar_dataframe(df, "stg_rdt")
                total += len(df)

    print(f"[OK] Complemento concluído. Linhas adicionadas em stg_rdt: {total}")

if __name__ == "__main__":
    main()
