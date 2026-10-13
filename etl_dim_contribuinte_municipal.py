import os
import psycopg2
from psycopg2.extras import execute_values
import datetime

# Config de conexão
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}

# Funções auxiliares
def tratar_string(valor, tamanho_max):
    if not valor or str(valor).strip() == "":
        return "Não Informado"[:tamanho_max] if tamanho_max >= 12 else "NI"
    return str(valor).strip()[:tamanho_max]


def tratar_data(valor):
    if not valor or str(valor).strip() in ["", "0", "00000000"]:
        return datetime.date(1900, 1, 1)
    try:
        return pd.to_datetime(valor).date()
    except Exception:
        return datetime.date(1900, 1, 1)

def tratar_numero(valor):
    try:
        return float(valor)
    except Exception:
        return 0

def carregar_dim_contribuinte_municipal():
    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    # Garantir registro de exceção
    cur.execute("""
        INSERT INTO suporte_arrecadacao.dim_contribuinte_municipal (
            id_contribuinte_municipal, cga, cnpj, id_estabelecimento,
            razao_social, nome_fantasia, tipo_tributacao, documento_constituicao,
            data_inscricao_prefeitura, data_inicio_situacao_cadastral, data_fim_situacao_cadastral,
            inscricao_estadual, situacao_cadastral, motivo_situacao_cadastral, situacao_especial,
            especie_logradouro, titulo_logradouro, logradouro, complemento_endereco, numero_porta,
            edificio, ponto_referencia, cep, natureza_juridica, telefone, tipo_estabelecimento, segmento_fiscal
        )
        VALUES (
            -1, '-1', '-1', -1,
            'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado',
            '1900-01-01', '1900-01-01', '1900-01-01',
            'NI', 'Não Informado', 'Não Informado', 'Não Informado',
            'NI', 'NI', 'Não Informado', 'Não Informado', 'NI',
            'Não Informado', 'Não Informado', 'NI', 'NI', 'NI', 'Não Informado', 'Não Informado'
        )
        ON CONFLICT (id_contribuinte_municipal) DO NOTHING;
    """)
    conn.commit()

    # Ler staging
    cur.execute("SELECT * FROM suporte_arrecadacao.stg_contribuinte_municipal")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    registros = []
    for r in rows:
        row = dict(zip(colnames, r))

        cga = tratar_string(row["cga"], 11)
        cnpj = tratar_string(row["cnpj"], 14)

        # FK para estabelecimento
        cur.execute("SELECT id_estabelecimento FROM suporte_arrecadacao.dim_estabelecimento WHERE cnpj = %s", (cnpj,))
        result = cur.fetchone()
        id_estabelecimento = result[0] if result else -1

        registros.append((
            cga,                              # varchar(11)
            cnpj,                             # varchar(14)
            id_estabelecimento,
            tratar_string(row["razao_social"], 255),
            tratar_string(row["nome_fantasia"], 255),
            tratar_string(row["tipo_tributacao"], 100),
            tratar_string(row["documento_constituicao"], 100),
            tratar_data(row["data_inscricao_prefeitura"]),
            tratar_data(row["data_inicio_situacao_cadastral"]),
            tratar_data(row["data_fim_situacao_cadastral"]),
            tratar_string(row["inscricao_estadual"], 50),
            tratar_string(row["situacao_cadastral"], 50),
            tratar_string(row["motivo_situacao_cadastral"], 255),
            tratar_string(row["situacao_especial"], 100),
            tratar_string(row["especie_logradouro"], 20),
            tratar_string(row["titulo_logradouro"], 20),
            tratar_string(row["logradouro"], 255),
            tratar_string(row["complemento_endereco"], 255),
            tratar_string(row["numero_porta"], 10),
            tratar_string(row["edificio"], 50),
            tratar_string(row["ponto_referencia"], 255),
            tratar_string(row["cep"], 8),
            tratar_string(row["natureza_juridica"], 100),
            tratar_string(row["telefone"], 15),
            tratar_string(row["tipo_estabelecimento"], 50),
            tratar_string(row["segmento_fiscal"], 50)
        ))


    insert_sql = """
        INSERT INTO suporte_arrecadacao.dim_contribuinte_municipal (
            cga, cnpj, id_estabelecimento,
            razao_social, nome_fantasia, tipo_tributacao, documento_constituicao,
            data_inscricao_prefeitura, data_inicio_situacao_cadastral, data_fim_situacao_cadastral,
            inscricao_estadual, situacao_cadastral, motivo_situacao_cadastral, situacao_especial,
            especie_logradouro, titulo_logradouro, logradouro, complemento_endereco, numero_porta,
            edificio, ponto_referencia, cep, natureza_juridica, telefone, tipo_estabelecimento, segmento_fiscal
        )
        VALUES %s
        ON CONFLICT (cga) DO UPDATE SET
            cnpj = EXCLUDED.cnpj,
            id_estabelecimento = EXCLUDED.id_estabelecimento,
            razao_social = EXCLUDED.razao_social,
            nome_fantasia = EXCLUDED.nome_fantasia,
            tipo_tributacao = EXCLUDED.tipo_tributacao,
            documento_constituicao = EXCLUDED.documento_constituicao,
            data_inscricao_prefeitura = EXCLUDED.data_inscricao_prefeitura,
            data_inicio_situacao_cadastral = EXCLUDED.data_inicio_situacao_cadastral,
            data_fim_situacao_cadastral = EXCLUDED.data_fim_situacao_cadastral,
            inscricao_estadual = EXCLUDED.inscricao_estadual,
            situacao_cadastral = EXCLUDED.situacao_cadastral,
            motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
            situacao_especial = EXCLUDED.situacao_especial,
            especie_logradouro = EXCLUDED.especie_logradouro,
            titulo_logradouro = EXCLUDED.titulo_logradouro,
            logradouro = EXCLUDED.logradouro,
            complemento_endereco = EXCLUDED.complemento_endereco,
            numero_porta = EXCLUDED.numero_porta,
            edificio = EXCLUDED.edificio,
            ponto_referencia = EXCLUDED.ponto_referencia,
            cep = EXCLUDED.cep,
            natureza_juridica = EXCLUDED.natureza_juridica,
            telefone = EXCLUDED.telefone,
            tipo_estabelecimento = EXCLUDED.tipo_estabelecimento,
            segmento_fiscal = EXCLUDED.segmento_fiscal;
    """
    execute_values(cur, insert_sql, registros, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()
    print(f"[INFO] Carga concluída para dim_contribuinte_municipal. Registros processados: {len(registros)}")

if __name__ == "__main__":
    carregar_dim_contribuinte_municipal()
