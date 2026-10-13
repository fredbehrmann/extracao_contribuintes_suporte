# etl_fato_arrecadacao_fast.py
import os
import time
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
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

BATCH_SIZE_INSERT = 500          # batch do execute_batch
LOOKUP_CHUNK = 200_000           # chunk p/ lookup em massa (cga/data) - ajuste se necessário
COMMIT_EVERY_BATCH = True        # True = commit a cada batch de insert


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def carregar_fato_arrecadacao():
    engine = create_engine(PG_URL)
    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    try:
        print("[INFO] Lendo dados da staging stg_arrecadacao...")
        df = pd.read_sql(f"SELECT * FROM {SCHEMA}.stg_arrecadacao", engine)

        if df.empty:
            print("[INFO] Nenhum registro encontrado na staging. Encerrando.")
            return

        print(f"[INFO] {len(df):,} registros encontrados na staging.")

        # ==============================
        # Tratamento / normalização em massa
        # ==============================
        # Garantir colunas como string antes de tratar
        for col in ["cga", "ano", "mes", "receita_evento", "situacao_movimento", "valor_pago"]:
            if col not in df.columns:
                raise ValueError(f"Coluna obrigatória não encontrada na staging: {col}")

        # Normalização do CGA para lookup com a dimensão.
        # Regra: mantém apenas dígitos e completa com zeros à esquerda até 11 posições.
        # Caso o CGA venha vazio/nulo, mantém string vazia para não gerar falso match.
        df["cga"] = (
            df["cga"]
              .fillna("")
              .astype(str)
              .str.strip()
              .str.replace(r"\D", "", regex=True)
        )
        df.loc[df["cga"] != "", "cga"] = df.loc[df["cga"] != "", "cga"].str.zfill(11)

        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(1900).astype(int)
        df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(1).astype(int)
        df["receita_evento"] = df["receita_evento"].fillna("NI").astype(str).str.strip()
        df["situacao_movimento"] = df["situacao_movimento"].fillna("NI").astype(str).str.strip()
        df["valor_pago"] = pd.to_numeric(df["valor_pago"], errors="coerce").fillna(0.0).astype(float)

        # Sempre dia 1 do mês
        df["data_ref"] = pd.to_datetime(
            df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01",
            errors="coerce"
        )
        # Se alguma data der problema, joga pra 1900-01-01
        df["data_ref"] = df["data_ref"].fillna(pd.Timestamp("1900-01-01"))
        df["data_ref_str"] = df["data_ref"].dt.strftime("%Y-%m-%d")

        # ==============================
        # LOOKUP EM MASSA - dim_contribuinte_municipal (por CGA)
        # ==============================
        print("[INFO] Fazendo lookup em massa: dim_contribuinte_municipal (CGA -> ID)...")
        t0 = time.time()

        cgas = df.loc[df["cga"] != "", "cga"].dropna().unique().tolist()
        cga_to_id = {}

        sql_cga = f"""
            SELECT cga, id_contribuinte_municipal
            FROM {SCHEMA}.dim_contribuinte_municipal
            WHERE cga = ANY(%s)
            ORDER BY cga, situacao_cadastral
        """

        for part in _chunks(cgas, LOOKUP_CHUNK):
            cur.execute(sql_cga, (part,))
            for cga, id_cm in cur.fetchall():
                cga_norm = str(cga).strip()
                cga_norm = ''.join(ch for ch in cga_norm if ch.isdigit()).zfill(11)
                cga_to_id[cga_norm] = int(id_cm)

        df["id_contribuinte_municipal"] = df["cga"].map(cga_to_id).fillna(-1).astype(int)

        total_sem_cga = int((df["cga"] == "").sum())
        total_sem_lookup = int((df["id_contribuinte_municipal"] == -1).sum())

        print(f"[INFO] Lookup contribuintes concluído em {time.time() - t0:.1f}s "
              f"({len(cga_to_id):,} IDs encontrados).")
        print(f"[INFO] Registros sem CGA na staging: {total_sem_cga:,}")
        print(f"[INFO] Registros sem correspondência na dimensão: {total_sem_lookup:,}")

        # ==============================
        # LOOKUP EM MASSA - dim_tempo (por data_ref)
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
        # AGREGAÇÃO (muito importante!)
        # Você usa ON CONFLICT (id_contribuinte_municipal, id_tempo, receita_evento)
        # então é melhor já agregar antes para reduzir volume de insert.
        # qtd_registros = contagem de linhas
        # ==============================
        print("[INFO] Agregando registros por chave de conflito (ganho de performance)...")
        t0 = time.time()

        # Definição de "situacao_movimento" em caso de múltiplas linhas:
        # Aqui vou usar o "último" (last) após ordenação natural do dataframe.
        # Se você tiver uma regra melhor (ex. prioridade), dá pra ajustar.
        df["_ord"] = range(len(df))

        agg = (
            df.sort_values("_ord")
              .groupby(["id_contribuinte_municipal", "id_tempo", "receita_evento"], as_index=False)
              .agg(
                  situacao_movimento=("situacao_movimento", "last"),
                  valor_pago=("valor_pago", "sum"),
                  qtd_registros=("receita_evento", "size")
              )
        )

        print(f"[INFO] Agregação concluída em {time.time() - t0:.1f}s. "
              f"Linhas: {len(df):,} -> {len(agg):,}")

        # Transformar em lista de tuplas para execute_batch
        registros = list(
            zip(
                agg["id_contribuinte_municipal"].astype(int),
                agg["id_tempo"].astype(int),
                agg["receita_evento"].astype(str),
                agg["situacao_movimento"].astype(str),
                agg["valor_pago"].astype(float),
                agg["qtd_registros"].astype(int),
            )
        )

        # ==============================
        # INSERT EM LOTES + PROGRESSO
        # ==============================
        print("[INFO] Carregando dados na fato_arrecadacao (insert em lotes)...")

        sql_insert = f"""
            INSERT INTO {SCHEMA}.fato_arrecadacao (
                id_contribuinte_municipal,
                id_tempo,
                receita_evento,
                situacao_movimento,
                valor_pago,
                qtd_registros
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_contribuinte_municipal, id_tempo, receita_evento)
            DO UPDATE SET
                situacao_movimento = EXCLUDED.situacao_movimento,
                valor_pago = EXCLUDED.valor_pago,
                qtd_registros = EXCLUDED.qtd_registros
        """

        total = len(registros)
        if total == 0:
            print("[INFO] Nada a inserir após agregação. Encerrando.")
            return

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

        print("[INFO] Carga concluída com sucesso!")

    finally:
        cur.close()
        conn.close()
        engine.dispose()


if __name__ == "__main__":
    carregar_fato_arrecadacao()
