import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from sqlalchemy import create_engine

# Configuração da conexão
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}

SCHEMA = "suporte_arrecadacao"
STAGING = f"{SCHEMA}.stg_rfb_simples"
DIM = f"{SCHEMA}.dim_simples"

# Criar engine SQLAlchemy
pg_url = f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
engine = create_engine(pg_url)

def carregar_dim_simples():
    with psycopg2.connect(**pg_config_sefaz) as conn, conn.cursor() as cur:
        print("[INFO] Registro de exceção garantido em dim_simples.")
        cur.execute(f"""
            INSERT INTO {DIM} (
                id_empresa, cnpj_base,
                opcao_simples, data_opcao_simples, data_exclusao_simples,
                opcao_mei, data_opcao_mei, data_exclusao_mei
            ) VALUES (-1, '-1', FALSE, '1900-01-01', '1900-01-01', FALSE, '1900-01-01', '1900-01-01')
            ON CONFLICT (cnpj_base) DO NOTHING;
        """)
        conn.commit()

    # Ler staging
    with engine.connect() as conn:
        print(f"[INFO] Lendo dados da staging {STAGING}...")
        df = pd.read_sql(f"SELECT * FROM {STAGING}", conn)
        print(f"[INFO] {len(df)} registros encontrados.")

    if df.empty:
        print("[INFO] Nenhum dado encontrado na staging. Encerrando.")
        return

    # Tratamento de nulos
    df = df.fillna({
        "cnpj_base": "-1",
        "opcao_simples": False,
        "data_opcao_simples": "1900-01-01",
        "data_exclusao_simples": "1900-01-01",
        "opcao_mei": False,
        "data_opcao_mei": "1900-01-01",
        "data_exclusao_mei": "1900-01-01"
    })

    # Garantir tipos corretos de data
    for col in ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        df[col] = df[col].fillna(pd.to_datetime("1900-01-01").date())

    # Recuperar id_empresa pela dim_empresa
    with engine.connect() as conn:
        empresas = pd.read_sql(f"SELECT id_empresa, cnpj_base FROM {SCHEMA}.dim_empresa", conn)

    df = df.merge(empresas, on="cnpj_base", how="left")
    df["id_empresa"] = df["id_empresa"].fillna(-1).astype(int)

    registros = [
        (
            row["id_empresa"],
            row["cnpj_base"],
            bool(row["opcao_simples"]),
            row["data_opcao_simples"],
            row["data_exclusao_simples"],
            bool(row["opcao_mei"]),
            row["data_opcao_mei"],
            row["data_exclusao_mei"]
        )
        for _, row in df.iterrows()
    ]

    sql = f"""
        INSERT INTO {DIM} (
            id_empresa, cnpj_base,
            opcao_simples, data_opcao_simples, data_exclusao_simples,
            opcao_mei, data_opcao_mei, data_exclusao_mei
        )
        VALUES %s
        ON CONFLICT (cnpj_base) DO UPDATE SET
            id_empresa = EXCLUDED.id_empresa,
            opcao_simples = EXCLUDED.opcao_simples,
            data_opcao_simples = EXCLUDED.data_opcao_simples,
            data_exclusao_simples = EXCLUDED.data_exclusao_simples,
            opcao_mei = EXCLUDED.opcao_mei,
            data_opcao_mei = EXCLUDED.data_opcao_mei,
            data_exclusao_mei = EXCLUDED.data_exclusao_mei;
    """

    with psycopg2.connect(**pg_config_sefaz) as conn, conn.cursor() as cur:
        print(f"[INFO] Carregando dados na {DIM}...")
        execute_values(cur, sql, registros, page_size=500)
        conn.commit()
        print("[INFO] Carga concluída com sucesso.")

if __name__ == "__main__":
    carregar_dim_simples()

