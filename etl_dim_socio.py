import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch

# Configuração conexão Postgres
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}

SCHEMA = "suporte_arrecadacao"

# ==========================
# DE-PARA
# ==========================
identificador_map = {"1": "Pessoa Jurídica", "2": "Pessoa Física", "3": "Estrangeiro"}

faixa_etaria_map = {
    "1": "0 a 12 anos", "2": "13 a 20 anos", "3": "21 a 30 anos",
    "4": "31 a 40 anos", "5": "41 a 50 anos", "6": "51 a 60 anos",
    "7": "61 a 70 anos", "8": "71 a 80 anos", "9": "Maiores de 80 anos",
    "0": "Não se aplica"
}

# Carregar qualificações de sócios do CSV
qualificacoes_df = pd.read_csv("qualificacao socios.csv")
qualificacao_map = dict(zip(qualificacoes_df["codigo"].astype(str), qualificacoes_df["descricao"]))

# ==========================
# Funções utilitárias
# ==========================
def tratar_string(valor: str, tamanho_campo: int):
    if pd.isna(valor) or str(valor).strip() == "":
        return "Não Informado" if tamanho_campo >= 12 else "NI"
    return str(valor).strip()

def tratar_data(valor):
    if pd.isna(valor) or str(valor).strip() in ["", "0", "00000000"]:
        return "1900-01-01"
    return valor

def tratar_codigo(valor, mapa, tamanho_campo=12):
    if pd.isna(valor) or str(valor).strip() == "":
        return "Não Informado" if tamanho_campo >= 12 else "NI"
    return mapa.get(str(valor), "Não Informado")

# ==========================
# Carga da dimensão
# ==========================
def carregar_dim_socio():
    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    print("[INFO] Lendo dados da staging stg_rfb_socios...")
    df = pd.read_sql(f"SELECT * FROM {SCHEMA}.stg_rfb_socios", conn)
    print(f"[INFO] {len(df)} registros encontrados.")

    # Aplicar tratamentos
    df["identificador_socio"] = df["identificador_de_socio"].apply(lambda x: tratar_codigo(x, identificador_map))
    df["qualificacao_socio"] = df["qualificacao_socio"].apply(lambda x: tratar_codigo(x, qualificacao_map))
    df["qualificacao_representante"] = df["qualificacao_representante_legal"].apply(lambda x: tratar_codigo(x, qualificacao_map))
    df["faixa_etaria_desc"] = df["faixa_etaria"].apply(lambda x: tratar_codigo(x, faixa_etaria_map))

    df["nome_socio"] = df["nome_socio"].apply(lambda x: tratar_string(x, 255))
    df["nome_representante"] = df["nome_representante"].apply(lambda x: tratar_string(x, 255))
    df["data_entrada_sociedade"] = df["data_entrada_sociedade"].apply(tratar_data)

    # Buscar id_estabelecimento para cada CNPJ
    cur.execute(f"SELECT cnpj, id_estabelecimento FROM {SCHEMA}.dim_estabelecimento;")
    estab_map = {row[0]: row[1] for row in cur.fetchall()}

    registros = [
        (
            estab_map.get(row["cnpj"], -1),  # FK para dim_estabelecimento
            row["cnpj"],
            row["cnpj_base"],
            row["identificador_socio"],
            row["nome_socio"],
            row["cnpj_cpf_socio"],
            row["qualificacao_socio"],
            row["data_entrada_sociedade"],
            row["pais"],
            row["representante_legal"],
            row["nome_representante"],
            row["qualificacao_representante"],
            row["faixa_etaria_desc"]
        )
        for _, row in df.iterrows()
    ]

    sql_insert = f"""
        INSERT INTO {SCHEMA}.dim_socio (
            id_estabelecimento, cnpj, cnpj_base, identificador_socio, nome_socio,
            cpf_cnpj_socio, qualificacao_socio, data_entrada, pais,
            representante_legal, nome_representante,
            qualificacao_representante, faixa_etaria
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cnpj, cpf_cnpj_socio)
        DO UPDATE SET
            id_estabelecimento = EXCLUDED.id_estabelecimento,
            identificador_socio = EXCLUDED.identificador_socio,
            nome_socio = EXCLUDED.nome_socio,
            qualificacao_socio = EXCLUDED.qualificacao_socio,
            data_entrada = EXCLUDED.data_entrada,
            pais = EXCLUDED.pais,
            representante_legal = EXCLUDED.representante_legal,
            nome_representante = EXCLUDED.nome_representante,
            qualificacao_representante = EXCLUDED.qualificacao_representante,
            faixa_etaria = EXCLUDED.faixa_etaria;
    """

    print("[INFO] Carregando dados na dim_socio...")
    execute_batch(cur, sql_insert, registros, page_size=500)

    # Inserir registro de exceção (-1)
    cur.execute(f"""
        INSERT INTO {SCHEMA}.dim_socio (
            id_socio, id_estabelecimento, cnpj, cnpj_base,
            identificador_socio, nome_socio, cpf_cnpj_socio,
            qualificacao_socio, data_entrada, pais,
            representante_legal, nome_representante,
            qualificacao_representante, faixa_etaria
        )
        VALUES (
            -1, -1, '00000000000000', '00000000',
            'NI', 'Não Informado', 'NI',
            'Não Informado', '1900-01-01', 'Não Informado',
            'NI', 'Não Informado',
            'Não Informado', 'Não Informado'
        )
        ON CONFLICT (id_socio) DO NOTHING;
    """)

    conn.commit()
    print("[SUCESSO] Carga da dim_socio concluída.")

    cur.close()
    conn.close()

# Execução
if __name__ == "__main__":
    carregar_dim_socio()
