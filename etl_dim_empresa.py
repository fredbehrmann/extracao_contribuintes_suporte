import os
import psycopg2
from psycopg2.extras import execute_values

# Configuração da conexão
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}

def tratar_string(valor, tamanho_max):
    if not valor or valor.strip() == "":
        return "Não Informado" if tamanho_max >= 12 else "NI"
    return valor

def tratar_numero(valor):
    return valor if valor is not None else 0

def tratar_porte(codigo):
    """
    De-para de códigos do banco para descrição:
    00 – NÃO INFORMADO
    01 - MICRO EMPRESA
    03 - EMPRESA DE PEQUENO PORTE
    05 - DEMAIS
    """
    mapping = {
        "00": "Não Informado",
        "01": "Micro Empresa",
        "03": "Pequeno Porte",
        "05": "Demais"
    }
    if not codigo or codigo.strip() == "":
        return "Não Informado"
    return mapping.get(codigo.strip(), "Não Informado")

def carregar_dim_empresa():
    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    # Garantir registro de exceção
    cur.execute("""
        INSERT INTO suporte_arrecadacao.dim_empresa (
            id_empresa,cnpj_base, razao_social, natureza_juridica, descricao_natureza,
            porte_empresa, capital_social
        )
        VALUES (-1,'-1','Não Informado','NI','Não Informado','Não Informado',0)
        ON CONFLICT (cnpj_base) DO NOTHING;
    """)
    conn.commit()

    # Buscar dados da staging
    cur.execute("""
        SELECT e.cnpj_base,
               e.razao_social,
               e.natureza_juridica_cod,
               e.natureza_juridica_desc,
               e.porte_empresa,
               e.capital_social
        FROM suporte_arrecadacao.stg_rfb_empresas e
    """)
    rows = cur.fetchall()

    registros = []
    for r in rows:
        cnpj_base = r[0] if r[0] else "-1"
        razao_social = tratar_string(r[1], 255)
        natureza_juridica = tratar_string(r[2], 10)
        descricao_natureza = tratar_string(r[3], 255)
        porte_empresa = tratar_porte(r[4])
        capital_social = tratar_numero(r[5])

        registros.append((
            cnpj_base, razao_social, natureza_juridica,
            descricao_natureza, porte_empresa, capital_social
        ))

    # UPSERT
    insert_sql = """
        INSERT INTO suporte_arrecadacao.dim_empresa (
            cnpj_base, razao_social, natureza_juridica, descricao_natureza,
            porte_empresa, capital_social
        )
        VALUES %s
        ON CONFLICT (cnpj_base) DO UPDATE SET
            razao_social = EXCLUDED.razao_social,
            natureza_juridica = EXCLUDED.natureza_juridica,
            descricao_natureza = EXCLUDED.descricao_natureza,
            porte_empresa = EXCLUDED.porte_empresa,
            capital_social = EXCLUDED.capital_social;
    """

    execute_values(cur, insert_sql, registros, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()
    print(f"[INFO] Carga concluída para dim_empresa. Registros processados: {len(registros)}")

if __name__ == "__main__":
    carregar_dim_empresa()
