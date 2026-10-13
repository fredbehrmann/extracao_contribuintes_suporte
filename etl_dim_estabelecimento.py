import os
import pandas as pd
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

# =============================
# MAPEAMENTOS (de-para)
# =============================
MATRIZ_FILIAL_MAP = {
    "1": "Matriz",
    "2": "Filial"
}

SITUACAO_CADASTRAL_MAP = {
    "01": "Nula",
    "02": "Ativa",
    "03": "Suspensa",
    "04": "Inapta",
    "08": "Baixada"
}

MOTIVO_SITUACAO_MAP = {
    "00": "Sem motivo",
    "01": "Extinção por encerramento liquidação voluntária",
    "02": "Incorporação",
    "03": "Fusão",
    "04": "Cisão total",
    "05": "Encerramento da falência",
    "06": "Encerramento da liquidação",
    "07": "Elevação a matriz",
    "08": "Transpasse",
    "09": "Não início de atividade",
    "10": "Extinção pelo encerramento da liquidação judicial",
    "11": "Anulação por multiplicidade",
    "12": "Anulação online de ofício",
    "13": "Omissa contumaz",
    "14": "Omissa não localizada",
    "15": "Inexistência de fato",
    "16": "Anulação por vícios",
    "17": "Baixa iniciada em análise",
    "18": "Interrupção temporária das atividades",
    "21": "Pedido de baixa indeferida",
    "24": "Por emissão certidão negativa",
    "28": "Transferência filial condição matriz",
    "31": "Extinção-unificação da filial",
    "33": "Transferência órgão local para filial do regional",
    "34": "Anulação de inscrição indevida",
    "35": "Empresa estrangeira aguardando documentação",
    "36": "Prática irregular de operação de comércio exterior",
    "37": "Baixa de produtor rural",
    "38": "Baixa deferida pela RFB aguardando análise convenente",
    "39": "Baixa deferida pela RFB e indeferida pelo convenente",
    "40": "Baixa indeferida pela RFB e aguardando análise convenente",
    "41": "Baixa indeferida pela RFB e deferida pelo convenente",
    "42": "Baixa deferida pela RFB e Sefin, aguardando Sefaz",
    "43": "Baixa deferida pela RFB, Sefaz pendente e indeferida Sefin",
    "44": "Baixa deferida RFB e Sefaz, aguardando Sefin",
    "45": "Baixa indeferida RFB, deferida Sefin, aguardando Sefaz",
    "46": "Baixa deferida RFB e Sefaz, indeferida Sefin",
    "47": "Baixa deferida RFB, Sefaz e Sefin indeferida",
    "48": "Baixa indeferida RFB, Sefaz pendente, deferida Sefin",
    "49": "Baixa indeferida RFB, aguardando análise Sefaz e Sefin",
    "50": "Baixa indeferida RFB, deferida Sefaz, pendente Sefin",
    "51": "Baixa indeferida RFB e Sefaz, aguardando análise Sefin",
    "52": "Baixa indeferida RFB, deferida Sefaz e indeferida Sefin",
    "53": "Baixa indeferida RFB e Sefaz e deferida Sefin",
    "54": "Extinção - tratamento diferenciado Lei 123/2006",
    "55": "Deferido pelo convenente, aguardando análise RFB",
    "60": "Artigo 30, VI, da IN 748/2007",
    "61": "Indício interpos. fraudulenta",
    "62": "Falta de pluralidade de sócios",
    "63": "Omissão de declarações",
    "64": "Localização desconhecida",
    "66": "Inaptidão",
    "67": "Registro cancelado",
    "70": "Anulação por ato de registro não confirmado (MEI)",
    "71": "Inaptidão (Lei 11.941/2009 Art.54)",
    "72": "Determinação judicial",
    "73": "Comissão contumaz",
    "74": "Inconsistência cadastral",
    "75": "Óbito do MEI - titular falecido",
    "80": "Baixa registrada na junta, indeferida na RFB",
    "81": "Solicitação de órgão estadual/municipal",
    "82": "Suspenso perante a CVM",
    "93": "CNPJ - titular baixado"
}

# Carregar dicionário de municípios
df_municipios = pd.read_csv("municipios.csv")
MUNICIPIOS_MAP = dict(zip(df_municipios["codigo"].astype(str), df_municipios["descricao"]))

# =============================
# TRATAMENTO DE CAMPOS
# =============================
def tratar_string(valor, tamanho_max):
    if not valor or valor.strip() == "":
        return "Não Informado" if tamanho_max >= 12 else "NI"
    return valor

def tratar_data(valor):
    if not valor or valor in ("", "0"):
        return "1900-01-01"
    return valor

def carregar_dim_estabelecimento():
    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    # Garantir registro de exceção
    cur.execute("""
        INSERT INTO suporte_arrecadacao.dim_estabelecimento (
            id_estabelecimento, cnpj, cnpj_base, id_empresa, matriz_filial, nome_fantasia,
            situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
            data_inicio_atividades, cnae_fiscal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio
        )
        VALUES (-1,'-1','-1',-1,'NI','Não Informado','Não Informado',
                '1900-01-01','Não Informado','1900-01-01','NI','Não Informado',
                'Não Informado','Não Informado','NI','Não Informado','Não Informado','NI','NI','Não Informado')
        ON CONFLICT (cnpj) DO NOTHING;
    """)
    conn.commit()

    # Buscar dados da staging
    cur.execute("""
        SELECT 
            e.cnpj,
            e.cnpj_base,
            e.matriz_filial,
            e.nome_fantasia,
            e.situacao_cadastral,
            e.data_situacao_cadastral,
            e.motivo_situacao_cadastral,
            e.data_inicio_atividades,
            e.cnae_fiscal,
            e.cnae_fiscal_secundaria,
            e.tipo_logradouro,
            e.logradouro,
            e.numero,
            e.complemento,
            e.bairro,
            e.cep,
            e.uf,
            e.municipio_cod
        FROM suporte_arrecadacao.stg_rfb_estabelecimentos e
    """)
    rows = cur.fetchall()

    registros = []
    for r in rows:
        cnpj = r[0] if r[0] else "-1"
        cnpj_base = r[1] if r[1] else "-1"

        # Buscar id_empresa
        cur.execute("SELECT id_empresa FROM suporte_arrecadacao.dim_empresa WHERE cnpj_base = %s", (cnpj_base,))
        result = cur.fetchone()
        id_empresa = result[0] if result else -1 # id do registro de exceção

        matriz_filial = MATRIZ_FILIAL_MAP.get(str(r[2]).strip(), "NI")
        nome_fantasia = tratar_string(r[3], 255)
        situacao_cadastral = SITUACAO_CADASTRAL_MAP.get(str(r[4]).zfill(2), "Não Informado")
        data_situacao_cadastral = tratar_data(r[5])
        motivo_situacao_cadastral = MOTIVO_SITUACAO_MAP.get(str(r[6]).zfill(2), "Não Informado")
        data_inicio_atividades = tratar_data(r[7])
        cnae_fiscal = tratar_string(r[8], 7)
        cnae_fiscal_secundaria = tratar_string(r[9], 20)
        tipo_logradouro = tratar_string(r[10], 50)
        logradouro = tratar_string(r[11], 255)
        numero = tratar_string(r[12], 20)
        complemento = tratar_string(r[13], 100)
        bairro = tratar_string(r[14], 100)
        cep = tratar_string(r[15], 8)
        uf = tratar_string(r[16], 2)

        municipio_cod = str(r[17]) if r[17] else None
        municipio = MUNICIPIOS_MAP.get(municipio_cod, "Não Informado")
       # municipio = tratar_string(r[17], 200)

        registros.append((
            cnpj, cnpj_base, id_empresa, matriz_filial, nome_fantasia,
            situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
            data_inicio_atividades, cnae_fiscal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio
        ))

    # UPSERT
    insert_sql = """
        INSERT INTO suporte_arrecadacao.dim_estabelecimento (
            cnpj, cnpj_base, id_empresa, matriz_filial, nome_fantasia,
            situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
            data_inicio_atividades, cnae_fiscal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio
        )
        VALUES %s
        ON CONFLICT (cnpj) DO UPDATE SET
            cnpj_base = EXCLUDED.cnpj_base,
            id_empresa = EXCLUDED.id_empresa,
            matriz_filial = EXCLUDED.matriz_filial,
            nome_fantasia = EXCLUDED.nome_fantasia,
            situacao_cadastral = EXCLUDED.situacao_cadastral,
            data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
            motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
            data_inicio_atividades = EXCLUDED.data_inicio_atividades,
            cnae_fiscal = EXCLUDED.cnae_fiscal,
            cnae_fiscal_secundaria = EXCLUDED.cnae_fiscal_secundaria,
            tipo_logradouro = EXCLUDED.tipo_logradouro,
            logradouro = EXCLUDED.logradouro,
            numero = EXCLUDED.numero,
            complemento = EXCLUDED.complemento,
            bairro = EXCLUDED.bairro,
            cep = EXCLUDED.cep,
            uf = EXCLUDED.uf,
            municipio = EXCLUDED.municipio;
    """

    execute_values(cur, insert_sql, registros, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()
    print(f"[INFO] Carga concluída para dim_estabelecimento. Registros processados: {len(registros)}")

if __name__ == "__main__":
    carregar_dim_estabelecimento()
