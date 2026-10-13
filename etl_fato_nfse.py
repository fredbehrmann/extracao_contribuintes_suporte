# etl_fato_nfse_fast.py
# Otimizações:
# 1) Lookup em massa (cnpj -> id_contribuinte_municipal) e (data_ref -> id_tempo) em vez de 2 SELECTs por linha
# 2) Agregação prévia pela chave de conflito (reduz MUITO o volume de insert/upsert)
# 3) Insert em lotes com progresso + ETA
# 4) Map de status vetorizado (sem apply linha-a-linha)

import os
import time
import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}
SCHEMA = "suporte_arrecadacao"

PG_URL = (
    f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}"
    f"@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
)

LOOKUP_CHUNK = 200_000
BATCH_SIZE_INSERT = 1000
COMMIT_EVERY_BATCH = True

# ==============================
# HELPERS
# ==============================
def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def carregar_fato_nfse():
    engine = create_engine(PG_URL)

    print("[INFO] Lendo dados da staging stg_nfse...")
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {SCHEMA}.stg_nfse", conn)

    if df.empty:
        print("[INFO] stg_nfse vazia. Nada a carregar.")
        engine.dispose()
        return

    print(f"[INFO] {len(df):,} registros encontrados na staging.")

    # ==============================
    # Normalização / tipos
    # ==============================
    # Strings principais
    str_cols = ["cnpj", "cnpj_base", "cga", "beneficio_fiscal", "tipo_tributacao", "regime_tributario", "codigo_atividade"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna("NI").astype(str).str.strip()
        else:
            # garante coluna para evitar KeyError depois
            df[col] = "NI"

    # Ano/Mês
    df["ano"] = pd.to_numeric(df.get("ano", 1900), errors="coerce").fillna(1900).astype(int)
    df["mes"] = pd.to_numeric(df.get("mes", 1), errors="coerce").fillna(1).astype(int)

    # Medidas (padroniza nomes alternativos)
    if "valor_iss_proprio_pago" not in df.columns and "valor_iss_proprio_paga" in df.columns:
        df["valor_iss_proprio_pago"] = df["valor_iss_proprio_paga"]
    if "valor_iss_retido_pago" not in df.columns and "valor_iss_retido_paga" in df.columns:
        df["valor_iss_retido_pago"] = df["valor_iss_retido_paga"]

    medidas_float = [
        "valor_servico", "valor_iss", "valor_iss_proprio", "valor_iss_retido",
        "valor_iss_proprio_pago", "valor_iss_retido_pago", "valor_deducoes", "valor_credito"
    ]
    for m in medidas_float:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0.0).astype(float)
        else:
            df[m] = 0.0

    if "qtd_notas" in df.columns:
        df["qtd_notas"] = pd.to_numeric(df["qtd_notas"], errors="coerce").fillna(0).astype(int)
    else:
        df["qtd_notas"] = 0

    # ==============================
    # Status: vetorizado
    # ==============================
    if "sk_status_nota" in df.columns:
        sk = pd.to_numeric(df["sk_status_nota"], errors="coerce")
        df["status_nota"] = "Não Informado"
        df.loc[sk == 1, "status_nota"] = "Ativo"
        df.loc[sk == 2, "status_nota"] = "Cancelado"
    else:
        df["status_nota"] = df.get("status_nota", "Não Informado")
        df["status_nota"] = df["status_nota"].fillna("Não Informado").astype(str).str.strip()

    # ==============================
    # Data referência (sempre dia 1)
    # ==============================
    df["data_ref"] = pd.to_datetime(
        df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01",
        errors="coerce"
    ).fillna(pd.Timestamp("1900-01-01"))

    df["data_ref_str"] = df["data_ref"].dt.strftime("%Y-%m-%d")

    # Normalizações finais de dimensões textuais
    df["cnpj"] = df["cnpj"].fillna("00000000000000").astype(str).str.strip()
    df["codigo_atividade"] = df["codigo_atividade"].fillna("NI").astype(str).str.strip().str.slice(0, 10)
    for c in ["beneficio_fiscal", "tipo_tributacao", "regime_tributario", "status_nota"]:
        df[c] = df[c].fillna("Não Informado").astype(str).str.strip()

    # ==============================
    # DB connection (psycopg2) para lookups + insert
    # ==============================
    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    try:
        # ==============================
        # LOOKUP em massa: cnpj -> id_contribuinte_municipal
        # ==============================
        print("[INFO] Fazendo lookup em massa: dim_contribuinte_municipal (CNPJ -> ID)...")
        t0 = time.time()

        cnpjs = df["cnpj"].dropna().unique().tolist()
        cnpj_to_id = {}

        sql_cnpj = f"""
            SELECT cnpj, id_contribuinte_municipal
            FROM {SCHEMA}.dim_contribuinte_municipal
            WHERE cnpj = ANY(%s)
        """

        for part in _chunks(cnpjs, LOOKUP_CHUNK):
            cur.execute(sql_cnpj, (part,))
            for cnpj, id_cm in cur.fetchall():
                cnpj_to_id[str(cnpj)] = int(id_cm)

        df["id_contribuinte_municipal"] = df["cnpj"].map(cnpj_to_id).fillna(-1).astype(int)
        print(f"[INFO] Lookup contribuintes concluído em {time.time() - t0:.1f}s "
              f"({len(cnpj_to_id):,} IDs encontrados).")

        qtd_mapeados = int(df["id_contribuinte_municipal"].ne(-1).sum())
        qtd_nao_mapeados = len(df) - qtd_mapeados
        if qtd_mapeados == 0:
            raise RuntimeError(
                "Nenhum CNPJ da stg_nfse foi encontrado em dim_contribuinte_municipal. "
                "A carga foi interrompida antes de gravar registros com a chave de exceção -1."
            )
        if qtd_nao_mapeados:
            print(
                f"[AVISO] {qtd_nao_mapeados:,} de {len(df):,} registros da stg_nfse "
                "não encontraram contribuinte municipal e usarão a chave -1."
            )

        # ==============================
        # LOOKUP em massa: data_ref -> id_tempo
        # (usa cast ::date[] para evitar erro date=text)
        # ==============================
        print("[INFO] Fazendo lookup em massa: dim_tempo (data -> ID)...")
        t0 = time.time()

        datas = df["data_ref_str"].dropna().unique().tolist()
        data_to_idtempo = {}

        sql_tempo = f"""
            SELECT to_char(data, 'YYYY-MM-DD') AS data_str, id_tempo
            FROM {SCHEMA}.dim_tempo
            WHERE data = ANY(%s::date[])
        """

        for part in _chunks(datas, LOOKUP_CHUNK):
            cur.execute(sql_tempo, (part,))
            for data_str, id_tempo in cur.fetchall():
                data_to_idtempo[str(data_str)] = int(id_tempo)

        df["id_tempo"] = df["data_ref_str"].map(data_to_idtempo).fillna(-1).astype(int)
        print(f"[INFO] Lookup tempo concluído em {time.time() - t0:.1f}s "
              f"({len(data_to_idtempo):,} IDs encontrados).")

        # ==============================
        # AGREGAÇÃO pela chave de conflito
        # Chave do ON CONFLICT:
        # (id_contribuinte_municipal, id_tempo, codigo_atividade,
        #  beneficio_fiscal, tipo_tributacao, regime_tributario, status_nota)
        # Medidas somadas; qtd_notas somada
        # ==============================
        print("[INFO] Agregando registros por chave de conflito (ganho de performance)...")
        t0 = time.time()

        key = [
            "id_contribuinte_municipal", "id_tempo",
            "codigo_atividade", "beneficio_fiscal", "tipo_tributacao", "regime_tributario", "status_nota"
        ]

        agg = (
            df.groupby(key, as_index=False)
              .agg(
                  valor_servico=("valor_servico", "sum"),
                  valor_iss=("valor_iss", "sum"),
                  valor_iss_proprio=("valor_iss_proprio", "sum"),
                  valor_iss_retido=("valor_iss_retido", "sum"),
                  valor_iss_proprio_pago=("valor_iss_proprio_pago", "sum"),
                  valor_iss_retido_pago=("valor_iss_retido_pago", "sum"),
                  valor_deducoes=("valor_deducoes", "sum"),
                  valor_credito=("valor_credito", "sum"),
                  qtd_notas=("qtd_notas", "sum"),
              )
        )

        # arredonda após agregação (evita erro acumulado por round antecipado)
        for m in medidas_float:
            agg[m] = agg[m].astype(float).round(2)
        agg["qtd_notas"] = agg["qtd_notas"].astype(int)

        print(f"[INFO] Agregação concluída em {time.time() - t0:.1f}s. "
              f"Linhas: {len(df):,} -> {len(agg):,}")

        # ==============================
        # Inserção em lotes + progresso
        # ==============================
        print("[INFO] Carregando dados na fato_nfse (insert em lotes)...")

        sql_insert = f"""
            INSERT INTO {SCHEMA}.fato_nfse (
                id_contribuinte_municipal, id_tempo,
                codigo_atividade, beneficio_fiscal, tipo_tributacao, regime_tributario, status_nota,
                valor_servico, valor_iss, valor_iss_proprio, valor_iss_retido,
                valor_iss_proprio_pago, valor_iss_retido_pago, valor_deducoes, valor_credito, qtd_notas
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id_contribuinte_municipal, id_tempo, codigo_atividade, beneficio_fiscal, tipo_tributacao, regime_tributario, status_nota)
            DO UPDATE SET
                valor_servico = EXCLUDED.valor_servico,
                valor_iss = EXCLUDED.valor_iss,
                valor_iss_proprio = EXCLUDED.valor_iss_proprio,
                valor_iss_retido = EXCLUDED.valor_iss_retido,
                valor_iss_proprio_pago = EXCLUDED.valor_iss_proprio_pago,
                valor_iss_retido_pago = EXCLUDED.valor_iss_retido_pago,
                valor_deducoes = EXCLUDED.valor_deducoes,
                valor_credito = EXCLUDED.valor_credito,
                qtd_notas = EXCLUDED.qtd_notas
        """

        total = len(agg)
        if total == 0:
            print("[INFO] Nada a inserir após agregação. Encerrando.")
            return

        # gera tuplas sem iterrows (mais rápido)
        registros = list(
            zip(
                agg["id_contribuinte_municipal"].astype(int).tolist(),
                agg["id_tempo"].astype(int).tolist(),
                agg["codigo_atividade"].astype(str).tolist(),
                agg["beneficio_fiscal"].astype(str).tolist(),
                agg["tipo_tributacao"].astype(str).tolist(),
                agg["regime_tributario"].astype(str).tolist(),
                agg["status_nota"].astype(str).tolist(),
                agg["valor_servico"].astype(float).tolist(),
                agg["valor_iss"].astype(float).tolist(),
                agg["valor_iss_proprio"].astype(float).tolist(),
                agg["valor_iss_retido"].astype(float).tolist(),
                agg["valor_iss_proprio_pago"].astype(float).tolist(),
                agg["valor_iss_retido_pago"].astype(float).tolist(),
                agg["valor_deducoes"].astype(float).tolist(),
                agg["valor_credito"].astype(float).tolist(),
                agg["qtd_notas"].astype(int).tolist(),
            )
        )

        t0 = time.time()
        processados = 0

        for i in range(0, total, BATCH_SIZE_INSERT):
            batch = registros[i:i + BATCH_SIZE_INSERT]
            execute_batch(cur, sql_insert, batch, page_size=BATCH_SIZE_INSERT)

            if COMMIT_EVERY_BATCH:
                conn.commit()

            processados += len(batch)
            elapsed = time.time() - t0
            taxa = processados / elapsed if elapsed > 0 else 0
            restante = (total - processados) / taxa if taxa > 0 else 0

            print(
                f"[PROGRESSO] {processados:,}/{total:,} ({processados/total:.1%}) "
                f"| {taxa:,.0f} reg/s | ETA ~ {restante/60:.1f} min"
            )

        if not COMMIT_EVERY_BATCH:
            conn.commit()

        print(f"[INFO] Carga concluída na fato_nfse. Linhas (agregadas): {total:,}")

    finally:
        cur.close()
        conn.close()
        engine.dispose()


if __name__ == "__main__":
    carregar_fato_nfse()
