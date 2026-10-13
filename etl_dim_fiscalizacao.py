# etl_dim_fiscalizacao_final.py
# Carrega DIMs a partir da STAGING (sem colunas do contribuinte em dim_programacao;
# dim_notificacao sem FKs)
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine

PG_CONFIG = {
    "host": os.getenv("RFB_PG_HOST", "valedosrios"),
    "dbname": os.getenv("RFB_PG_DB", "db_rfb"),
    "user": os.getenv("RFB_PG_USER", "cif_user"),
    "password": os.getenv("RFB_PG_PASS", ""),
    "port": int(os.getenv("RFB_PG_PORT", "5432")),
}
PG_URL = (
    f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
    f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
)
SCHEMA = "suporte_arrecadacao"

def normaliza_situacao(desc: str) -> str:
    if not desc:
        return "NAO_INFORMADA"
    d = desc.strip().upper()
    if "ABERT" in d: return "ABERTA"
    if "ANDAMENT" in d: return "EM_ANDAMENTO"
    if "CONCLU" in d or "ENCERR" in d: return "CONCLUIDA"
    if "CANCEL" in d: return "CANCELADA"
    return "OUTRAS"

def carregar_dim_programacao():
    eng = create_engine(PG_URL)
    with eng.connect() as conn:
        df = pd.read_sql(f"SELECT DISTINCT numero_programacao, nome_auditor, descricao_programacao, extensao, mes_ano_referencia, tipo_programacao FROM {SCHEMA}.stg_fisc_programacoes", conn)

    if df.empty:
        print("[INFO] stg_fisc_programacoes vazia.")
        return

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].fillna("Não Informado").astype(str).str.strip()
    df["numero_programacao"] = pd.to_numeric(df["numero_programacao"], errors="coerce").fillna(0).astype(int)

    with psycopg2.connect(**PG_CONFIG) as conn, conn.cursor() as cur:
        regs = list(df[[
            "numero_programacao","nome_auditor","descricao_programacao","extensao","mes_ano_referencia","tipo_programacao"
        ]].itertuples(index=False, name=None))

        sql = f"""
            INSERT INTO {SCHEMA}.dim_programacao_fiscal (
                numero_programacao, nome_auditor, descricao_programacao, extensao, mes_ano_referencia, tipo_programacao
            ) VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (numero_programacao) DO UPDATE SET
                nome_auditor = EXCLUDED.nome_auditor,
                descricao_programacao = EXCLUDED.descricao_programacao,
                extensao = EXCLUDED.extensao,
                mes_ano_referencia = EXCLUDED.mes_ano_referencia,
                tipo_programacao = EXCLUDED.tipo_programacao
        """
        execute_batch(cur, sql, regs, page_size=1000)
        conn.commit()
        print(f"[INFO] dim_programacao_fiscal upsert: {len(regs)}")

def carregar_dim_situacao():
    eng = create_engine(PG_URL)
    with eng.connect() as conn:
        df = pd.read_sql(f"SELECT DISTINCT dssituacaofiscalizacao AS descricao FROM {SCHEMA}.stg_fisc_situacoes", conn)

    if df.empty:
        print("[INFO] stg_fisc_situacoes vazia p/ situação.")
        return

    df["descricao"] = df["descricao"].fillna("Não Informado").astype(str).str.strip()
    df["situacao_normalizada"] = df["descricao"].apply(normaliza_situacao)

    with psycopg2.connect(**PG_CONFIG) as conn, conn.cursor() as cur:
        regs = list(df[["descricao","situacao_normalizada"]].itertuples(index=False, name=None))
        sql = f"""
            INSERT INTO {SCHEMA}.dim_situacao_fiscalizacao (descricao, situacao_normalizada)
            VALUES (%s,%s)
            ON CONFLICT (descricao) DO UPDATE SET
                situacao_normalizada = EXCLUDED.situacao_normalizada
        """
        execute_batch(cur, sql, regs, page_size=1000)
        conn.commit()
        print(f"[INFO] dim_situacao_fiscalizacao upsert: {len(regs)}")

def carregar_dim_notificacao():
    eng = create_engine(PG_URL)
    with eng.connect() as conn:
        df = pd.read_sql(f"""
            SELECT DISTINCT
                numero_notificacao, ano_notificacao, tipo_notificacao,
                situacao_auto, fato, data_registro_auto
            FROM {SCHEMA}.stg_fisc_autos
        """, conn)

    if df.empty:
        print("[INFO] stg_fisc_autos vazia p/ notificacao.")
        return

    df["numero_notificacao"] = pd.to_numeric(df["numero_notificacao"], errors="coerce").fillna(0).astype(int)
    df["ano_notificacao"]    = pd.to_numeric(df["ano_notificacao"], errors="coerce").fillna(1900).astype(int)
    for c in ["tipo_notificacao","situacao_auto","fato"]:
        df[c] = df[c].fillna("Não Informado").astype(str).str.strip()
    df["data_registro_auto"] = pd.to_datetime(df["data_registro_auto"], errors="coerce").dt.date.fillna(pd.to_datetime("1900-01-01").date())

    with psycopg2.connect(**PG_CONFIG) as conn, conn.cursor() as cur:
        regs = list(df[[
            "numero_notificacao","ano_notificacao","tipo_notificacao","situacao_auto","fato","data_registro_auto"
        ]].itertuples(index=False, name=None))

        sql = f"""
            INSERT INTO {SCHEMA}.dim_notificacao_auto (
                numero_notificacao, ano_notificacao, tipo_notificacao, situacao_auto, fato, data_registro_auto
            ) VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (numero_notificacao, ano_notificacao) DO UPDATE SET
                tipo_notificacao = EXCLUDED.tipo_notificacao,
                situacao_auto = EXCLUDED.situacao_auto,
                fato = EXCLUDED.fato,
                data_registro_auto = EXCLUDED.data_registro_auto
        """
        execute_batch(cur, sql, regs, page_size=1000)
        conn.commit()
        print(f"[INFO] dim_notificacao_auto upsert: {len(regs)}")

if __name__ == "__main__":
    carregar_dim_programacao()
    carregar_dim_situacao()
    carregar_dim_notificacao()
