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

SQL_UPSERT_FATO_RDT = f"""
INSERT INTO {SCHEMA}.fato_rdt (
    id_tempo,
    id_contribuinte_municipal,
    id_rdt_atributos,
    sq_rdt,
    numero_rdt,
    ano_geracao,
    valor_rdt
)
SELECT
    COALESCE(dt.id_tempo, -1) AS id_tempo,
    COALESCE(dcm.id_contribuinte_municipal, -1) AS id_contribuinte_municipal,
    dra.id_rdt_atributos,
    s.sq_rdt,
    s.numero_rdt,
    s.ano_geracao,
    COALESCE(s.valor_rdt, 0) AS valor_rdt
FROM {SCHEMA}.stg_rdt s
LEFT JOIN {SCHEMA}.dim_tempo dt
    ON dt.data = make_date(s.ano, s.mes, 1)
LEFT JOIN {SCHEMA}.dim_contribuinte_municipal dcm
    ON dcm.cga = s.cga
LEFT JOIN {SCHEMA}.dim_rdt_atributos dra
    ON dra.hash_atributos = md5(concat_ws('|',
        coalesce(s.tipo_tributacao,''),
        coalesce(s.substituto_tributario_desc,''),
        coalesce(s.receita,''),
        coalesce(s.evento_lancamento,''),
        coalesce(s.localizacao_rdt,''),
        coalesce(to_char(s.data_inclusao_sat,'YYYY-MM-DD'),''),
        coalesce(s.rdt_bloqueio_desc,''),
        coalesce(s.exigibilidade_desc,''),
        coalesce(s.liquidacao_desc,''),
        coalesce(s.pagamento_desc,''),
        coalesce(s.sistema_rdt_desc,'')
    ))
ON CONFLICT (sq_rdt)
DO UPDATE SET
    id_tempo = EXCLUDED.id_tempo,
    id_contribuinte_municipal = EXCLUDED.id_contribuinte_municipal,
    id_rdt_atributos = EXCLUDED.id_rdt_atributos,
    numero_rdt = EXCLUDED.numero_rdt,
    ano_geracao = EXCLUDED.ano_geracao,
    valor_rdt = EXCLUDED.valor_rdt;
"""

SQL_UPSERT_FATO_RDT_INAD = f"""
INSERT INTO {SCHEMA}.fato_rdt_inadimplencia (
    id_tempo,
    id_contribuinte_municipal,
    id_fato_rdt,
    id_inad_origem,
    data_entrada_inadimplencia,
    data_saida_inadimplencia,
    valor_saldo_original_inad,
    valor_multa_mora_inad,
    valor_juros_mora_inad,
    valor_correcao_inad,
    valor_total_debito_inad,
    valor_original_inad
)
SELECT
    COALESCE(dt.id_tempo, -1) AS id_tempo,
    COALESCE(dcm.id_contribuinte_municipal, -1) AS id_contribuinte_municipal,
    fr.id_fato_rdt,
    si.id_inad_origem,
    si.data_entrada_inadimplencia,
    si.data_saida_inadimplencia,
    COALESCE(si.valor_saldo_original_inad, 0),
    COALESCE(si.valor_multa_mora_inad, 0),
    COALESCE(si.valor_juros_mora_inad, 0),
    COALESCE(si.valor_correcao_inad, 0),
    COALESCE(si.valor_total_debito_inad, 0),
    COALESCE(si.valor_original_inad, 0)
FROM {SCHEMA}.stg_rdt_inadimplencia si
JOIN {SCHEMA}.fato_rdt fr
    ON fr.sq_rdt = si.sq_rdt
LEFT JOIN {SCHEMA}.dim_tempo dt
    ON dt.data = make_date(si.ano, si.mes, 1)
LEFT JOIN {SCHEMA}.dim_contribuinte_municipal dcm
    ON dcm.cga = si.cga
ON CONFLICT (id_inad_origem)
DO UPDATE SET
    id_tempo = EXCLUDED.id_tempo,
    id_contribuinte_municipal = EXCLUDED.id_contribuinte_municipal,
    id_fato_rdt = EXCLUDED.id_fato_rdt,
    data_entrada_inadimplencia = EXCLUDED.data_entrada_inadimplencia,
    data_saida_inadimplencia = EXCLUDED.data_saida_inadimplencia,
    valor_saldo_original_inad = EXCLUDED.valor_saldo_original_inad,
    valor_multa_mora_inad = EXCLUDED.valor_multa_mora_inad,
    valor_juros_mora_inad = EXCLUDED.valor_juros_mora_inad,
    valor_correcao_inad = EXCLUDED.valor_correcao_inad,
    valor_total_debito_inad = EXCLUDED.valor_total_debito_inad,
    valor_original_inad = EXCLUDED.valor_original_inad;
"""

def main():
    with engine.begin() as conn:
        conn.execute(text(SQL_UPSERT_FATO_RDT))
        conn.execute(text(SQL_UPSERT_FATO_RDT_INAD))
    print("[OK] Carga concluída: fato_rdt + fato_rdt_inadimplencia.")

if __name__ == "__main__":
    main()
