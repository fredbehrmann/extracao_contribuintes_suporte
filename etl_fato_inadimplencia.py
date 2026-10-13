# etl_fato_inadimplencia_fast.py (historizada) - OTIMIZADA
# Otimizações (mesma linha do "C"):
# 1) Lookup em massa (cnpj -> id_contribuinte_municipal) e (ano -> id_tempo via data 'YYYY-01-01')
# 2) Agregação prévia na staging (já existia) + evita iterrows para montar registros
# 3) UPDATE/INSERT em lotes com progresso + ETA
# 4) Cast correto no lookup de tempo: data = ANY(%s::date[])  (evita date=text)

import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine
from datetime import date

data_extracao = date.today()

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
BATCH_SIZE = 1000
COMMIT_EVERY_BATCH = False  # em histórico, costuma ser melhor commitar no final (ou a cada N batches)


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def carregar_fato_inadimplencia_historica():
    engine = create_engine(PG_URL)
    print("[INFO] Lendo dados da staging stg_inadimplencia...")
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {SCHEMA}.stg_inadimplencia", conn)

    if df.empty:
        print("[INFO] stg_inadimplencia vazia. Nada a carregar.")
        engine.dispose()
        return

    print(f"[INFO] {len(df):,} registros encontrados na staging.")

    # ==============================
    # Normalização
    # ==============================
    for c in ["cnpj", "receita"]:
        if c in df.columns:
            df[c] = df[c].fillna("NI").astype(str).str.strip()
        else:
            df[c] = "NI"

    df["ano"] = pd.to_numeric(df.get("ano", 1900), errors="coerce").fillna(1900).astype(int)

    for m in ["valor_debito", "valor_total_debito"]:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0.0).astype(float)
        else:
            df[m] = 0.0

    # ==============================
    # Agregar duplicidades (já existia)
    # ==============================
    print("[INFO] Agregando duplicidades em (cnpj, ano, receita)...")
    t0 = time.time()

    group_cols = ["cnpj", "ano", "receita"]
    df = (
        df.groupby(group_cols, as_index=False)
          .agg(valor_debito=("valor_debito", "sum"),
               valor_total_debito=("valor_total_debito", "sum"))
    )

    df["valor_debito"] = df["valor_debito"].round(2)
    df["valor_total_debito"] = df["valor_total_debito"].round(2)

    print(f"[INFO] Agregação concluída em {time.time() - t0:.1f}s. Linhas agregadas: {len(df):,}")

    # data ref por ano (01-01 do ano)
    df["data_ref_str"] = df["ano"].astype(str) + "-01-01"
    df["cnpj"] = df["cnpj"].fillna("00000000000000").astype(str).str.strip()
    df["receita"] = df["receita"].fillna("Não Informado").astype(str).str.strip()

    # ==============================
    # Conexão psycopg2 para lookups + DML
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

        # ==============================
        # LOOKUP em massa: data_ref_str -> id_tempo
        # (cast ::date[] para evitar date=text)
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
        # Monta listas de tuplas SEM iterrows (mais rápido)
        # ==============================
        # registros para inativar: (id_contrib, id_tempo, receita)
        registros_inativar = list(
            zip(
                df["id_contribuinte_municipal"].astype(int).tolist(),
                df["id_tempo"].astype(int).tolist(),
                df["receita"].astype(str).tolist(),
                [data_extracao] * len(df),
            )
        )

        # registros para inserir: (id_contrib, id_tempo, receita, v_deb, v_tot)
        registros_inserir = list(
            zip(
                df["id_contribuinte_municipal"].astype(int).tolist(),
                df["id_tempo"].astype(int).tolist(),
                df["receita"].astype(str).tolist(),
                df["valor_debito"].astype(float).tolist(),
                df["valor_total_debito"].astype(float).tolist(),
            )
        )

        # ==============================
        # 1) Inativar registros antigos ativos (com progresso)
        # ==============================
        sql_upd = f"""
            UPDATE {SCHEMA}.fato_inadimplencia
               SET registro_ativo = FALSE
             WHERE registro_ativo = TRUE
               AND id_contribuinte_municipal = %s
               AND id_tempo = %s
               AND receita = %s
               AND DATE(dt_carga) = %s
        """

        total_u = len(registros_inativar)
        print(f"[INFO] Inativando versões antigas ativas... (chaves: {total_u:,})")
        t0 = time.time()
        done = 0

        for i in range(0, total_u, BATCH_SIZE):
            batch = registros_inativar[i:i + BATCH_SIZE]
            execute_batch(cur, sql_upd, batch, page_size=BATCH_SIZE)
            if COMMIT_EVERY_BATCH:
                conn.commit()

            done += len(batch)
            elapsed = time.time() - t0
            taxa = done / elapsed if elapsed > 0 else 0
            restante = (total_u - done) / taxa if taxa > 0 else 0
            print(f"[PROGRESSO][UPD] {done:,}/{total_u:,} ({done/total_u:.1%}) | {taxa:,.0f} chaves/s | ETA ~ {restante/60:.1f} min")

        # ==============================
        # 2) Inserir novas versões (insert-only) com upsert parcial (ativo) (com progresso)
        # ==============================
        sql_ins = f"""
            INSERT INTO {SCHEMA}.fato_inadimplencia (
                id_contribuinte_municipal, id_tempo, receita,
                valor_debito, valor_total_debito,
                dt_carga, registro_ativo
            ) VALUES (%s,%s,%s,%s,%s, now(), TRUE)
            ON CONFLICT (id_contribuinte_municipal, id_tempo, receita, dt_carga)
         --   ON CONFLICT (id_contribuinte_municipal, id_tempo, receita)
            WHERE registro_ativo
            DO UPDATE SET
                valor_debito = EXCLUDED.valor_debito,
                valor_total_debito = EXCLUDED.valor_total_debito,
            --    dt_carga = EXCLUDED.dt_carga,
                registro_ativo = TRUE
        """

        total_i = len(registros_inserir)
        print(f"[INFO] Inserindo novas versões... (linhas agregadas: {total_i:,})")
        t0 = time.time()
        done = 0

        for i in range(0, total_i, BATCH_SIZE):
            batch = registros_inserir[i:i + BATCH_SIZE]
            execute_batch(cur, sql_ins, batch, page_size=BATCH_SIZE)
            if COMMIT_EVERY_BATCH:
                conn.commit()

            done += len(batch)
            elapsed = time.time() - t0
            taxa = done / elapsed if elapsed > 0 else 0
            restante = (total_i - done) / taxa if taxa > 0 else 0
            print(f"[PROGRESSO][INS] {done:,}/{total_i:,} ({done/total_i:.1%}) | {taxa:,.0f} reg/s | ETA ~ {restante/60:.1f} min")

        if not COMMIT_EVERY_BATCH:
            conn.commit()

        print(f"[INFO] Inativados: {total_u:,} | Inseridos (agregados): {total_i:,}")

    finally:
        cur.close()
        conn.close()
        engine.dispose()


if __name__ == "__main__":
    carregar_fato_inadimplencia_historica()
