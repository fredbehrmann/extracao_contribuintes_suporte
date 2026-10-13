import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from periodo_processamento import obter_periodo


PERIODO = obter_periodo(ano_fim_padrao=2025)

# ===============================
# Configurações de conexão
# ===============================
SAT_USER = os.getenv("SAT_USER", "GUAQUIM")
SAT_PASS = os.getenv("SAT_PASS", "")
SAT_DSN = os.getenv("SAT_DSN", "pituba:2118/SAT")
SAT_CONN = f"oracle+cx_oracle://{SAT_USER}:{SAT_PASS}@{SAT_DSN}"

# Oracle DWH - utilizado para consultas de NFSe e para validação cadastral SN/MEI/SUP
ORACLE_DWH_USER = "AFROCHA"
ORACLE_DWH_PASS = os.getenv("DWH_PASS", "")
ORACLE_DWH_DSN = "barroquinha:1521/DWH"
DWH_CONN = f"oracle+cx_oracle://{ORACLE_DWH_USER}:{ORACLE_DWH_PASS}@{ORACLE_DWH_DSN}"

# Banco destino (Postgres SEFAZ)
# PG_CONN deve ser configurada por variável de ambiente quando necessária.

# Conexão Postgres (RFB - consulta externa)
RFB_DBNAME = "db_rfb"
RFB_USER = "cif_user"
RFB_PASS = os.getenv("RFB_PG_PASS", "")
RFB_HOST = "valedosrios"
RFB_PORT = 5432

# ===============================
# Criação do schema e da tabela no Postgres
# ===============================
ddl_schema = "CREATE SCHEMA IF NOT EXISTS suporte_arrecadacao;"

ddl_table = """
CREATE TABLE IF NOT EXISTS suporte_arrecadacao.stg_contribuintes_suporte (
    ano INT NOT NULL,
    cnpj_base TEXT NOT NULL,
    valor_arrecadado NUMERIC,
    valor_servico NUMERIC,
    valor_iss NUMERIC,
    valor_arrecadado_medio NUMERIC,
    valor_servico_medio NUMERIC,
    valor_iss_medio NUMERIC,
    motivo_01 TEXT,
    motivo_02 TEXT,
    motivo_03 TEXT,
    PRIMARY KEY (ano, cnpj_base)
);
"""

# pg_engine = create_engine(PG_CONN)
pg_engine = create_engine(
    f"postgresql+psycopg2://{RFB_USER}:{RFB_PASS}@{RFB_HOST}:{RFB_PORT}/{RFB_DBNAME}"
)

with pg_engine.connect() as conn:
    conn.execute(text(ddl_schema))
    conn.execute(text(ddl_table))
    conn.commit()

# ===============================
# Funções auxiliares
# ===============================
def normalizar_cnpj_base_serie(serie: pd.Series) -> pd.Series:
    """
    Normaliza CNPJ_BASE de forma vetorizada:
      - remove espaços;
      - remove sufixo decimal eventual, como .0;
      - mantém apenas números;
      - rejeita valores com mais de 8 dígitos;
      - preenche com zeros à esquerda até 8 dígitos;
      - retorna NA quando não houver valor válido.
    """
    s = serie.astype("string").str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"\D", "", regex=True)
    s = s.mask(s.eq(""))
    s = s.mask(s.str.len().gt(8))
    return s.str.zfill(8)


def padronizar_colunas_upper(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza os nomes das colunas para maiúsculas, evitando diferenças entre drivers."""
    df.columns = df.columns.str.upper()
    return df


def aplicar_motivos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regras:
      - motivo_01 = "Sim" se valor_arrecadado >= 400000
                    OU valor_arrecadado_medio > 33333.33
      - motivo_02 = "Sim" se valor_servico >= 10000000
                    OU valor_servico_medio > 833333.33
      - motivo_03 = "Sim" se valor_iss >= 400000
                    OU valor_iss_medio > 33333.33

    Implementação vetorizada para melhor performance.
    """
    colunas_valor = [
        "VALOR_ARRECADADO", "VALOR_ARRECADADO_MEDIO",
        "VALOR_SERVICO", "VALOR_SERVICO_MEDIO",
        "VALOR_ISS", "VALOR_ISS_MEDIO"
    ]

    for col in colunas_valor:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["MOTIVO_01"] = np.where(
        (df["VALOR_ARRECADADO"] >= 400000) | (df["VALOR_ARRECADADO_MEDIO"] > 33333.33),
        "Sim",
        "Não"
    )
    df["MOTIVO_02"] = np.where(
        (df["VALOR_SERVICO"] >= 10000000) | (df["VALOR_SERVICO_MEDIO"] > 833333.33),
        "Sim",
        "Não"
    )
    df["MOTIVO_03"] = np.where(
        (df["VALOR_ISS"] >= 400000) | (df["VALOR_ISS_MEDIO"] > 33333.33),
        "Sim",
        "Não"
    )
    return df


def carregar_indicadores_regime(dwh_engine) -> pd.DataFrame:
    """
    Carrega uma única vez os indicadores cadastrais utilizados para exclusão.

    Regra:
      - Se o CNPJ_BASE existir na dimensão, somente permanece se todos os indicadores forem 'N'.
      - Se houver mais de um registro para o mesmo CNPJ_BASE, o agrupamento consolida os indicadores.
        Assim, se qualquer registro do CNPJ_BASE tiver 'S' para SN, MEI ou SUP, o indicador consolidado será 'S'.
    """
    # O campo COD_CPF_CNPJ_SINTETICO pode vir sem o último dígito da raiz.
    # A chave canônica é derivada dos oito primeiros dígitos do CNPJ completo.
    sql_indicadores = """
    SELECT
        SUBSTR(LPAD(co.COD_NRO_CGC_CPF, 14, '0'), 1, 8) AS CNPJ_BASE,
        MAX(CASE WHEN UPPER(TRIM(co.IND_SN)) = 'S' THEN 'S' ELSE 'N' END) AS INDICADOR_SIMPLES_NACIONAL,
        MAX(CASE WHEN UPPER(TRIM(co.IND_MEI)) = 'S' THEN 'S' ELSE 'N' END) AS INDICADOR_MEI,
        MAX(CASE WHEN UPPER(TRIM(co.ind_supf)) = 'S' THEN 'S' ELSE 'N' END) AS INDICADOR_SUP
    FROM odi_nfse.dim_contribuinte co
    WHERE co.COD_NRO_CGC_CPF IS NOT NULL
    GROUP BY SUBSTR(LPAD(co.COD_NRO_CGC_CPF, 14, '0'), 1, 8)
    """

    df_indicadores = pd.read_sql(sql_indicadores, dwh_engine)
    df_indicadores = padronizar_colunas_upper(df_indicadores)

    if df_indicadores.empty:
        return pd.DataFrame(columns=[
            "CNPJ_BASE",
            "INDICADOR_SIMPLES_NACIONAL",
            "INDICADOR_MEI",
            "INDICADOR_SUP",
        ])

    df_indicadores["CNPJ_BASE"] = normalizar_cnpj_base_serie(df_indicadores["CNPJ_BASE"])
    df_indicadores = df_indicadores[df_indicadores["CNPJ_BASE"].notna()].copy()

    for col in ["INDICADOR_SIMPLES_NACIONAL", "INDICADOR_MEI", "INDICADOR_SUP"]:
        df_indicadores[col] = (
            df_indicadores[col]
            .astype("string")
            .str.strip()
            .str.upper()
            .fillna("N")
        )

    return df_indicadores.drop_duplicates(subset=["CNPJ_BASE"])


def aplicar_filtro_regime(df: pd.DataFrame, df_indicadores: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o filtro de regime de forma vetorizada.

    Mantém o contribuinte quando:
      1. o CNPJ_BASE não é encontrado na dim_contribuinte; ou
      2. é encontrado e possui simultaneamente:
         INDICADOR_SIMPLES_NACIONAL = 'N', INDICADOR_MEI = 'N' e INDICADOR_SUP = 'N'.

    Exclui quando qualquer um dos indicadores for igual a 'S'.
    """
    if df.empty:
        return df

    df = df.merge(df_indicadores, on="CNPJ_BASE", how="left", indicator="_encontrado_dim_contribuinte")

    nao_encontrado = df["_encontrado_dim_contribuinte"].eq("left_only")
    encontrado_e_apto = (
        df["INDICADOR_SIMPLES_NACIONAL"].eq("N")
        & df["INDICADOR_MEI"].eq("N")
        & df["INDICADOR_SUP"].eq("N")
    )

    df = df[nao_encontrado | encontrado_e_apto].copy()

    return df.drop(
        columns=[
            "INDICADOR_SIMPLES_NACIONAL",
            "INDICADOR_MEI",
            "INDICADOR_SUP",
            "_encontrado_dim_contribuinte",
        ],
        errors="ignore"
    )


# ===============================
# Cada exercício usa os dados do ano anterior para identificar a carteira.
# ===============================
anos = range(PERIODO.inicio.year, PERIODO.fim.year + 1)


sat_engine = create_engine(SAT_CONN)
dwh_engine = create_engine(DWH_CONN)

# Consulta única dos indicadores SN/MEI/SUP para evitar repetição dentro do processamento anual.
print("[INFO] Carregando indicadores cadastrais SN/MEI/SUP no DWH...")
df_indicadores_regime = carregar_indicadores_regime(dwh_engine)
print(f"[OK] Indicadores carregados: {len(df_indicadores_regime)} CNPJ_BASE.")

for ano in anos:
    ref_ano = ano - 1
    print(f"[INFO] Processando exercício {ano} (dados de {ref_ano})...")

    # -------------------------------
    # Consulta SAT (Arrecadação)
    # -------------------------------
    sql_sat = f"""
    SELECT SUBSTR(TRIM(pj.NUCNPJBASE), -8) AS CNPJ_BASE,
           SUM(arr.VLPAGTO) AS VALOR_ARRECADADO,
           (SUM(arr.VLPAGTO) / count(distinct EXTRACT(MONTH FROM arr.DTPAGTO))) AS VALOR_ARRECADADO_MEDIO
    FROM DBASAT.ESTABELECIMENTO est
         LEFT JOIN DBASAT.MOVIMARRECADRECEITABANCO arr
                ON est.nuCGABase = arr.nuBase AND est.nuCGAFilial = arr.nuFilial
         LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
                ON est.NUCNPJBASE = pj.NUCNPJBASE AND est.NUCNPJFILIAL = pj.NUCNPJFILIAL
         LEFT JOIN DBASAT.RECEITA r ON r.CDRECEITA = arr.CDRECEITA
    WHERE EXTRACT(YEAR FROM arr.DTPAGTO) = {ref_ano}
          AND r.sgReceita = 'ISS'
    GROUP BY SUBSTR(TRIM(pj.NUCNPJBASE), -8)
    HAVING SUM(arr.VLPAGTO) >= 400000
    OR (SUM(arr.VLPAGTO) / count(distinct EXTRACT(MONTH FROM arr.DTPAGTO))) > 33333.33
    """
    df_sat = pd.read_sql(sql_sat, sat_engine)
    df_sat = padronizar_colunas_upper(df_sat)

    if df_sat.empty:
        df_sat = pd.DataFrame(columns=[
            "CNPJ_BASE",
            "VALOR_ARRECADADO",
            "VALOR_ARRECADADO_MEDIO",
        ])

    # -------------------------------
    # Consulta DWH (NFSe)
    # -------------------------------
    # Não usar COD_CPF_CNPJ_SINTETICO como CNPJ_BASE: há registros em que ele
    # contém apenas sete dígitos e o LPAD cria uma raiz artificial.
    sql_dwh = f"""
    SELECT SUBSTR(LPAD(co.COD_NRO_CGC_CPF, 14, '0'), 1, 8) AS CNPJ_BASE,
           SUM(fp.val_serv_nfe) AS VALOR_SERVICO,
           (SUM(fp.val_serv_nfe) / count(distinct pe.mes)) AS VALOR_SERVICO_MEDIO,
           SUM(fp.VL_ISS_DECLARADO) AS VALOR_ISS,
           (SUM(fp.VL_ISS_DECLARADO) / count(distinct pe.mes)) AS VALOR_ISS_MEDIO
    FROM odi_nfse.fato_prestador_agregada fp
         INNER JOIN odi_dw.dim_periodo pe ON fp.sk_dt_periodo = pe.sk_dt_periodo
         INNER JOIN odi_nfse.dim_contribuinte co ON fp.sk_contribuinte = co.sk_contribuinte
    WHERE pe.ano = {ref_ano}
          AND sk_status_nota = 1
          AND fp.sk_tp_tributacao = 4
    GROUP BY SUBSTR(LPAD(co.COD_NRO_CGC_CPF, 14, '0'), 1, 8)
    HAVING SUM(fp.val_serv_nfe) >= 10000000
    OR SUM(fp.VL_ISS_DECLARADO) >= 400000
    OR (SUM(fp.val_serv_nfe) / count(distinct pe.mes)) > 833333.33
    OR (SUM(fp.VL_ISS_DECLARADO) / count(distinct pe.mes)) > 33333.33
    """
    df_dwh = pd.read_sql(sql_dwh, dwh_engine)
    df_dwh = padronizar_colunas_upper(df_dwh)

    if df_dwh.empty:
        df_dwh = pd.DataFrame(columns=[
            "CNPJ_BASE",
            "VALOR_SERVICO",
            "VALOR_SERVICO_MEDIO",
            "VALOR_ISS",
            "VALOR_ISS_MEDIO",
        ])

    # -------------------------------
    # Padronização do CNPJ_BASE
    # -------------------------------
    df_sat["CNPJ_BASE"] = normalizar_cnpj_base_serie(df_sat["CNPJ_BASE"])
    df_dwh["CNPJ_BASE"] = normalizar_cnpj_base_serie(df_dwh["CNPJ_BASE"])

    df_sat = df_sat[df_sat["CNPJ_BASE"].notna()].copy()
    df_dwh = df_dwh[df_dwh["CNPJ_BASE"].notna()].copy()

    if df_sat.empty:
        raise RuntimeError(
            f"A consulta SAT do exercício {ano} não retornou nenhum CNPJ_BASE válido. "
            "A carteira existente não será substituída."
        )

    # -------------------------------
    # Merge e aplicação de regras
    # -------------------------------
    df = pd.merge(df_sat, df_dwh, on="CNPJ_BASE", how="outer").fillna(0)
    df = aplicar_motivos(df)
    df["ANO"] = ano

    # Filtrar somente contribuintes que atendem pelo menos um motivo
    mask_motivos = df[["MOTIVO_01", "MOTIVO_02", "MOTIVO_03"]].eq("Sim").any(axis=1)
    df = df[mask_motivos].copy()

    qtd_antes_filtro_regime = len(df)

    # Excluir contribuintes Simples Nacional, MEI ou SUP quando encontrados na dim_contribuinte.
    # Quando o CNPJ_BASE não for encontrado na dimensão, a validação é desconsiderada e o registro permanece.
    df = aplicar_filtro_regime(df, df_indicadores_regime)

    qtd_excluidos_regime = qtd_antes_filtro_regime - len(df)
    print(f"[INFO] Ano {ano}: {qtd_excluidos_regime} contribuintes excluídos por SN/MEI/SUP.")

    # -------------------------------
    # Gravar no Postgres SEFAZ (schema suporte_arrecadacao)
    # -------------------------------
    df.columns = df.columns.str.lower()

    colunas_destino = [
        "ano",
        "cnpj_base",
        "valor_arrecadado",
        "valor_servico",
        "valor_iss",
        "valor_arrecadado_medio",
        "valor_servico_medio",
        "valor_iss_medio",
        "motivo_01",
        "motivo_02",
        "motivo_03",
    ]
    df = df[colunas_destino]

    # Substitui o exercício de forma atômica para permitir reprocessamento.
    with pg_engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM suporte_arrecadacao.stg_contribuintes_suporte
                WHERE ano = :ano
            """),
            {"ano": ano},
        )
        df.to_sql(
            "stg_contribuintes_suporte",
            conn,
            schema="suporte_arrecadacao",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    print(f"[OK] Ano {ano}: {len(df)} contribuintes gravados.")

print("[FINALIZADO] ETL de identificação concluído.")
