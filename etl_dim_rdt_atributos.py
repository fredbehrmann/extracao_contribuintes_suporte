import os
from sqlalchemy import create_engine, text

SCHEMA = "suporte_arrecadacao"

pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432
}

PG_URL = f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}@" \
         f"{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
engine = create_engine(PG_URL)

SQL_INSERT_DIM = f"""
INSERT INTO {SCHEMA}.dim_rdt_atributos (
    hash_atributos,
    tipo_tributacao,
    substituto_tributario_desc,
    receita,
    evento_lancamento,
    localizacao_rdt,
    data_inclusao_sat,
    rdt_bloqueio_desc,
    exigibilidade_desc,
    liquidacao_desc,
    pagamento_desc,
    sistema_rdt_desc
)
SELECT DISTINCT
    md5(concat_ws('|',
        coalesce(tipo_tributacao,''),
        coalesce(substituto_tributario_desc,''),
        coalesce(receita,''),
        coalesce(evento_lancamento,''),
        coalesce(localizacao_rdt,''),
        coalesce(to_char(data_inclusao_sat,'YYYY-MM-DD'),''),
        coalesce(rdt_bloqueio_desc,''),
        coalesce(exigibilidade_desc,''),
        coalesce(liquidacao_desc,''),
        coalesce(pagamento_desc,''),
        coalesce(sistema_rdt_desc,'')
    )) as hash_atributos,
    tipo_tributacao,
    substituto_tributario_desc,
    receita,
    evento_lancamento,
    localizacao_rdt,
    data_inclusao_sat,
    rdt_bloqueio_desc,
    exigibilidade_desc,
    liquidacao_desc,
    pagamento_desc,
    sistema_rdt_desc
FROM {SCHEMA}.stg_rdt
ON CONFLICT (hash_atributos) DO NOTHING;
"""

def main():
    with engine.begin() as conn:
        conn.execute(text(SQL_INSERT_DIM))
    print("[OK] dim_rdt_atributos carregada/atualizada.")

if __name__ == "__main__":
    main()
