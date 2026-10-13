# etl_stage_consolidados_municipio.py
# -----------------------------------------------------------------------------
# Objetivo:
#   Extrair dados consolidados do município, sem individualização por CNPJ/CGA,
#   e carregar as tabelas staging criadas para comparação com a carteira de
#   contribuintes suporte.
#
# Tabelas carregadas:
#   - stg_arrecadacao_municipio
#   - stg_nfse_municipio
#   - stg_inadimplencia_municipio
#   - stg_fiscalizacoes_municipio
#   - stg_valores_auto_municipio
#   - stg_rdt_municipio
#   - stg_rdt_inadimplencia_municipio
#
# Uso sugerido:
#   python etl_stage_consolidados_municipio.py 2026
#   python etl_stage_consolidados_municipio.py --ano 2026
#   python etl_stage_consolidados_municipio.py --inicio 2024-01-01 --fim 2025-12-31
#   python etl_stage_consolidados_municipio.py 2026 --somente arrecadacao nfse
#
# Observação:
#   Como a extração é municipal e não usa filtro por CNPJ, as consultas podem
#   retornar volumes grandes. Por padrão, quando informado um ano de referência,
#   o script extrai a janela de 2021-01-01 até 31/12 do ano informado.
#   O intervalo manual informado em --inicio/--fim continua tendo prioridade.
# -----------------------------------------------------------------------------

import argparse
import os
import re
from datetime import date, datetime, timedelta
from typing import Iterable

import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine, text


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

SCHEMA = os.getenv("PG_SCHEMA", "suporte_arrecadacao")

oracle_dwh_config = {
    "user": os.getenv("DWH_USER", "AFROCHA"),
    "password": os.getenv("DWH_PASS", ""),
    "dsn": os.getenv("DWH_DSN", "barroquinha:1521/DWH"),
}

oracle_sat_config = {
    "user": os.getenv("SAT_USER", "AFROCHA"),
    "password": os.getenv("SAT_PASS", ""),
    "dsn": os.getenv("SAT_DSN", "pituba:2118/SAT"),
}

pg_config_sefaz = {
    "dbname": os.getenv("PG_DBNAME", "db_rfb"),
    "user": os.getenv("PG_USER", "cif_user"),
    "password": os.getenv("RFB_PG_PASS") or os.getenv("PG_PASSWORD", ""),
    "host": os.getenv("PG_HOST", "valedosrios"),
    "port": int(os.getenv("PG_PORT", "5432")),
}

PG_URL = (
    f"postgresql+psycopg2://{pg_config_sefaz['user']}:{pg_config_sefaz['password']}"
    f"@{pg_config_sefaz['host']}:{pg_config_sefaz['port']}/{pg_config_sefaz['dbname']}"
)

pg_engine = create_engine(PG_URL)


# =============================================================================
# ARGUMENTOS / PARÂMETROS
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extrai consolidados municipais para staging."
    )
    parser.add_argument(
        "ano_posicional",
        nargs="?",
        type=int,
        default=None,
        help=(
            "Ano final de referência. Ex.: 2026 carrega de 2021-01-01 "
            "até 2026-12-31. Alternativa curta a --ano."
        ),
    )
    parser.add_argument(
        "--ano",
        type=int,
        default=None,
        help=(
            "Ano final de referência. Usado quando --inicio e --fim não forem "
            "informados. Ex.: --ano 2026 carrega de 2021-01-01 até 2026-12-31."
        ),
    )
    parser.add_argument(
        "--inicio",
        type=str,
        default=None,
        help="Data inicial no formato YYYY-MM-DD. Inclusiva.",
    )
    parser.add_argument(
        "--fim",
        type=str,
        default=None,
        help="Data final no formato YYYY-MM-DD. Inclusiva.",
    )
    parser.add_argument(
        "--somente",
        nargs="*",
        default=None,
        choices=[
            "arrecadacao",
            "nfse",
            "inadimplencia",
            "fiscalizacoes",
            "valores_auto",
            "rdt",
            "rdt_inadimplencia",
        ],
        help="Lista opcional de cargas a executar.",
    )
    parser.add_argument(
        "--manter-staging",
        action="store_true",
        help="Não trunca as tabelas staging antes da carga.",
    )
    return parser.parse_args()


def obter_janela(args) -> tuple[date, date]:
    """
    Define a janela temporal das consultas.

    Regra padrão do ETL:
      - Com ano de referência: de 2021-01-01 até 31/12 do ano informado.
        Ex.: python etl_stage_consolidados_municipio.py 2026
             => 2021-01-01 a 2026-12-31.

    Regra alternativa:
      - Se --inicio e --fim forem informados, o intervalo manual tem prioridade.
    """
    if bool(args.inicio) ^ bool(args.fim):
        raise ValueError("Informe --inicio e --fim juntos, ou não informe nenhum dos dois.")

    if args.inicio and args.fim:
        inicio = datetime.strptime(args.inicio, "%Y-%m-%d").date()
        fim = datetime.strptime(args.fim, "%Y-%m-%d").date()
    else:
        ano_referencia = (
            args.ano_posicional
            or args.ano
            or int(os.getenv("ANO", date.today().year))
        )
        inicio = date(2021, 1, 1)
        fim = date(ano_referencia, 12, 31)

    if fim < inicio:
        raise ValueError(f"Data final menor que data inicial: {inicio} > {fim}")
    return inicio, fim


def competencia_yyyymm(dt: date) -> int:
    return dt.year * 100 + dt.month


# =============================================================================
# HELPERS
# =============================================================================

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def conectar_oracle(config):
    return cx_Oracle.connect(config["user"], config["password"], config["dsn"])


def truncar_tabela(tabela: str):
    log(f"Truncando {SCHEMA}.{tabela}...")
    with pg_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{tabela}"))


def carregar_dataframe(df: pd.DataFrame, tabela: str):
    if df is None or df.empty:
        log(f"Nenhum registro retornado para {SCHEMA}.{tabela}.")
        return 0

    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    # Padronização simples para evitar inconsistências comuns nos textos.
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"None": None, "nan": None, "NaT": None, "": None})

    df.to_sql(
        tabela,
        pg_engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        chunksize=10_000,
        method="multi",
    )
    log(f"{len(df):,} registros inseridos em {SCHEMA}.{tabela}.")
    return len(df)


def filtrar_params_sql(sql: str, params: dict | None = None) -> dict:
    """
    Retorna apenas os parâmetros efetivamente usados no SQL Oracle.

    O cx_Oracle gera ORA-01036 quando o dicionário de binds contém chaves
    extras que não aparecem na consulta. Como este script reutiliza um único
    dicionário de parâmetros para consultas por data e por competência,
    precisamos remover os binds não utilizados antes de executar cada SELECT.
    """
    if not params:
        return {}

    binds_usados = set(re.findall(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)", sql))
    return {k: v for k, v in params.items() if k in binds_usados}


def executar_oracle_df(config: dict, sql: str, params: dict | None = None) -> pd.DataFrame:
    params_filtrados = filtrar_params_sql(sql, params)
    with conectar_oracle(config) as conn:
        return pd.read_sql(sql, conn, params=params_filtrados)


def deve_executar(nome: str, somente: Iterable[str] | None) -> bool:
    return somente is None or nome in somente


# =============================================================================
# SQLS DE EXTRAÇÃO - MUNICÍPIO
# =============================================================================

SQL_ARRECADACAO_MUNICIPIO = """
SELECT
    TRIM(r.sgReceita) || ' - ' || tel.nmEventoLancam AS RECEITA_EVENTO,
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado')        AS SEGMENTO_FISCAL,
    NVL(sm.DSSITUACAOMOVIMENTO, 'Não Informado')     AS SITUACAO_MOVIMENTO,
    EXTRACT(YEAR FROM arr.DTPAGTO)                   AS ANO,
    EXTRACT(MONTH FROM arr.DTPAGTO)                  AS MES,
    SUM(NVL(arr.VLPAGTO, 0))                         AS VALOR_PAGO,
    COUNT(*)                                         AS QTD_REGISTROS
FROM DBASAT.ESTABELECIMENTO est
LEFT JOIN DBASAT.MOVIMARRECADRECEITABANCO arr
       ON est.nuCGABase   = arr.nuBase
      AND est.nuCGAFilial = arr.nuFilial
LEFT JOIN DBASAT.CONTRIBUINTEPJ pj
       ON est.NUCNPJBASE   = pj.NUCNPJBASE
      AND est.NUCNPJFILIAL = pj.NUCNPJFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
       ON est.NUCGABASE   = cseg.NUCGABASE
      AND est.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.RECEITA r
       ON r.CDRECEITA = arr.CDRECEITA
LEFT JOIN DBASAT.EVENTOLANCAMENTO tel
       ON tel.cdReceita = r.cdReceita
      AND tel.cdEventoLancam = arr.cdTipoLancamento
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
       ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
LEFT JOIN DBASAT.SITUACAOMOVIMENTO sm
       ON arr.STMOVIMENTO = sm.CDSITUACAOMOVIMENTO
WHERE arr.DTPAGTO >= :dt_inicio
  AND arr.DTPAGTO <  :dt_fim_exclusivo
GROUP BY
    TRIM(r.sgReceita) || ' - ' || tel.nmEventoLancam,
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado'),
    NVL(sm.DSSITUACAOMOVIMENTO, 'Não Informado'),
    EXTRACT(YEAR FROM arr.DTPAGTO),
    EXTRACT(MONTH FROM arr.DTPAGTO)
"""

SQL_NFSE_MUNICIPIO = """
WITH periodo_filtrado AS (
    SELECT
        sk_dt_periodo,
        TO_NUMBER(TRIM(TO_CHAR(ano))) AS ano,
        TO_NUMBER(TRIM(TO_CHAR(mes))) AS mes
    FROM odi_dw.dim_periodo
    WHERE REGEXP_LIKE(TRIM(TO_CHAR(ano)), '^[0-9]{4}$')
      AND REGEXP_LIKE(TRIM(TO_CHAR(mes)), '^[0-9]{1,2}$')
      AND (TO_NUMBER(TRIM(TO_CHAR(ano))) * 100 + TO_NUMBER(TRIM(TO_CHAR(mes))))
          BETWEEN :comp_inicio AND :comp_fim
), nfse_base AS (
    SELECT
        NVL(co.NMSEGMENTOFISCAL, 'Não Informado')        AS SEGMENTO_FISCAL,
        pe.ano                                           AS ANO,
        pe.mes                                           AS MES,
        NVL(TO_CHAR(at.cod_atvd_rtnc), 'NI')             AS CODIGO_ATIVIDADE,
        NVL(bf.TXT_BENEFICIO_FISCAL, 'Não Informado')    AS BENEFICIO_FISCAL,
        NVL(tr.TXT_TIP_TRBC_NFE, 'Não Informado')        AS TIPO_TRIBUTACAO,
        NVL(rt.TXT_DCR_REGI_TRBC, 'Não Informado')       AS REGIME_TRIBUTARIO,
        CASE
            WHEN TRIM(TO_CHAR(fp.sk_status_nota)) = '1' THEN 'Ativo'
            WHEN TRIM(TO_CHAR(fp.sk_status_nota)) = '2' THEN 'Cancelado'
            ELSE 'Não Informado'
        END                                              AS STATUS_NOTA,
        CASE
            WHEN REGEXP_LIKE(TRIM(TO_CHAR(fp.sk_status_nota)), '^[0-9]+$')
            THEN TO_NUMBER(TRIM(TO_CHAR(fp.sk_status_nota)))
            ELSE NULL
        END                                              AS SK_STATUS_NOTA,
        NVL(fp.val_serv_nfe, 0)                          AS VALOR_SERVICO,
        NVL(fp.VL_ISS_DECLARADO, 0)                      AS VALOR_ISS,
        NVL(fp.vl_iss_proprio_declarado, 0)              AS VALOR_ISS_PROPRIO,
        NVL(fp.VL_ISS_RETIDO_DECLARADO, 0)               AS VALOR_ISS_RETIDO,
        NVL(fp.VAL_ISS_PROPRIO_PAGO, 0)                  AS VALOR_ISS_PROPRIO_PAGO,
        NVL(fp.VAL_ISS_RETIDO_PAGO, 0)                   AS VALOR_ISS_RETIDO_PAGO,
        NVL(fp.VAL_DEDC_NFE, 0)                          AS VALOR_DEDUCOES,
        NVL(fp.VAL_CRED_NFE, 0)                          AS VALOR_CREDITO,
        NVL(fp.qtd_notas, 0)                             AS QTD_NOTAS
    FROM odi_nfse.fato_prestador_agregada fp
    JOIN periodo_filtrado pe
      ON fp.sk_dt_periodo = pe.sk_dt_periodo
    JOIN odi_nfse.dim_contribuinte co
      ON fp.sk_contribuinte = co.sk_contribuinte
    JOIN odi_nfse.dim_atividade at
      ON fp.sk_atividade_nfe = at.sk_atividade
    JOIN odi_nfse.dim_beneficio_fiscal bf
      ON fp.sk_beneficio_fiscal = bf.sk_beneficio_fiscal
    JOIN odi_nfse.dim_tributacao tr
      ON fp.sk_tp_tributacao  = tr.sk_tributacao
    JOIN odi_nfse.dim_regime_tributario rt
      ON fp.sk_regime_tributario = rt.sk_regime_tributario
)
SELECT
    SEGMENTO_FISCAL,
    ANO,
    MES,
    CODIGO_ATIVIDADE,
    BENEFICIO_FISCAL,
    TIPO_TRIBUTACAO,
    REGIME_TRIBUTARIO,
    STATUS_NOTA,
    SK_STATUS_NOTA,
    SUM(VALOR_SERVICO)             AS VALOR_SERVICO,
    SUM(VALOR_ISS)                 AS VALOR_ISS,
    SUM(VALOR_ISS_PROPRIO)         AS VALOR_ISS_PROPRIO,
    SUM(VALOR_ISS_RETIDO)          AS VALOR_ISS_RETIDO,
    SUM(VALOR_ISS_PROPRIO_PAGO)    AS VALOR_ISS_PROPRIO_PAGO,
    SUM(VALOR_ISS_RETIDO_PAGO)     AS VALOR_ISS_RETIDO_PAGO,
    SUM(VALOR_DEDUCOES)            AS VALOR_DEDUCOES,
    SUM(VALOR_CREDITO)             AS VALOR_CREDITO,
    SUM(QTD_NOTAS)                 AS QTD_NOTAS
FROM nfse_base
GROUP BY
    SEGMENTO_FISCAL,
    ANO,
    MES,
    CODIGO_ATIVIDADE,
    BENEFICIO_FISCAL,
    TIPO_TRIBUTACAO,
    REGIME_TRIBUTARIO,
    STATUS_NOTA,
    SK_STATUS_NOTA
"""

SQL_INADIMPLENCIA_MUNICIPIO = """
SELECT
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado') AS SEGMENTO_FISCAL,
    inp.AAEXERCICIO                           AS ANO,
    NVL(rec.SGRECEITA, 'NI')                  AS RECEITA,
    SUM(NVL(inp.VLDEBITO, 0))                 AS VALOR_DEBITO,
    SUM(NVL(inp.VLTOTALDEBITO, 0))            AS VALOR_TOTAL_DEBITO,
    COUNT(*)                                  AS QTD_REGISTROS
FROM DBASAT.INADIMPLENCIA inp
LEFT JOIN DBASAT.ESTABELECIMENTO est
       ON inp.NUBASE = est.NUCGABASE
      AND inp.NUFILIAL = est.NUCGAFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
       ON est.NUCGABASE = cseg.NUCGABASE
      AND est.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
       ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
LEFT JOIN DBASAT.RECEITA rec
       ON rec.CDRECEITA = inp.CDRECEITA
LEFT JOIN DBASAT.TIPOCONTRIBUINTE08 tpc
       ON tpc.CDTIPOCONTRIBUINTE = inp.CDTIPOCONTRIBUINTE
WHERE inp.AAEXERCICIO BETWEEN :ano_inicio AND :ano_fim
  AND tpc.CDTIPOCONTRIBUINTE = 'E'
GROUP BY
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado'),
    inp.AAEXERCICIO,
    NVL(rec.SGRECEITA, 'NI')
"""

SQL_FISCALIZACOES_MUNICIPIO = """
SELECT
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado')        AS SEGMENTO_FISCAL,
    EXTRACT(YEAR FROM fs.DTSITUACAOFISCALIZACAO)     AS ANO,
    EXTRACT(MONTH FROM fs.DTSITUACAOFISCALIZACAO)    AS MES,
    NVL(sfisc.DSSITUACAOFISCALIZACAO, 'Não Informado') AS SITUACAO_FISCALIZACAO,
    NVL(tp.DSTIPOPROGRAMACAO, 'Não Informado')       AS TIPO_PROGRAMACAO,
    NVL(e.DSEXTENSAO, 'Não Informado')               AS EXTENSAO,
    COUNT(*)                                         AS QTD_EVENTOS
FROM DBASAT.PROGRAMACAO p
LEFT JOIN DBASAT.PROGRAMACAOCONTRIBUINTEAUDITOR pca
       ON p.CDPROGRAMACAO = pca.CDPROGRAMACAO
LEFT JOIN DBASAT.FISCALIZACAOSITUACAOCONTRIB fs
       ON p.CDPROGRAMACAO = fs.CDPROGRAMACAO
LEFT JOIN DBASAT.SITUACAOFISCALIZACAO sfisc
       ON sfisc.CDSITUACAOFISCALIZACAO = fs.CDSITUACAOFISCALIZACAO
LEFT JOIN DBASAT.ESTABELECIMENTO et
       ON pca.NUBASE = et.NUCGABASE
      AND pca.NUFILIAL = et.NUCGAFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
       ON et.NUCGABASE = cseg.NUCGABASE
      AND et.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
       ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
LEFT JOIN DBASAT.EXTENSAO e
       ON p.CDEXTENSAO = e.CDEXTENSAO
LEFT JOIN DBASAT.TIPOPROGRAMACAO tp
       ON p.CDTIPOPROGRAMACAO = tp.CDTIPOPROGRAMACAO
WHERE fs.DTSITUACAOFISCALIZACAO >= :dt_inicio
  AND fs.DTSITUACAOFISCALIZACAO <  :dt_fim_exclusivo
GROUP BY
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado'),
    EXTRACT(YEAR FROM fs.DTSITUACAOFISCALIZACAO),
    EXTRACT(MONTH FROM fs.DTSITUACAOFISCALIZACAO),
    NVL(sfisc.DSSITUACAOFISCALIZACAO, 'Não Informado'),
    NVL(tp.DSTIPOPROGRAMACAO, 'Não Informado'),
    NVL(e.DSEXTENSAO, 'Não Informado')
"""

SQL_VALORES_AUTO_MUNICIPIO = """
SELECT
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado') AS SEGMENTO_FISCAL,
    nai.AANOTAUTO                             AS ANO,
    vna.NUCOTA                                AS MES,
    NVL(r.SGRECEITA, 'NI')                    AS SIGLA_RECEITA,
    SUM(NVL(vna.VLMOEDAORIGINAL, 0))          AS VL_MOEDA_ORIGINAL,
    SUM(NVL(vna.VLCORRECAOMONETARIA, 0))      AS VL_CORRECAO_MONETARIA,
    SUM(NVL(vna.VLMULTAINFRACAO, 0))          AS VL_MULTA_INFRACAO,
    SUM(NVL(vna.VLMULTAMORA, 0))              AS VL_MULTA_MORA,
    SUM(NVL(vna.VLJUROSMORA, 0))              AS VL_JUROS_MORA,
    COUNT(DISTINCT TO_CHAR(nai.NUNOTAUTO) || '/' || TO_CHAR(nai.AANOTAUTO)) AS QTD_AUTOS
FROM DBASAT.NOTIFICACAOAUTODEINFRACAO nai
LEFT JOIN DBASAT.VALORNOTAUTO vna
       ON nai.NUNOTAUTO = vna.NUNOTAUTO
      AND nai.AANOTAUTO = vna.AANOTAUTO
LEFT JOIN DBASAT.RECEITA r
       ON vna.CDRECEITA = r.CDRECEITA
LEFT JOIN DBASAT.PROGRAMACAO p
       ON nai.CDPROGRAMACAO = p.CDPROGRAMACAO
LEFT JOIN DBASAT.ESTABELECIMENTO et
       ON nai.NUBASEPROG = et.NUCGABASE
      AND nai.NUFILIALPROG = et.NUCGAFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
       ON et.NUCGABASE = cseg.NUCGABASE
      AND et.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
       ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
WHERE (nai.AANOTAUTO * 100 + vna.NUCOTA) BETWEEN :comp_inicio AND :comp_fim
  AND vna.NUCOTA BETWEEN 1 AND 12
GROUP BY
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado'),
    nai.AANOTAUTO,
    vna.NUCOTA,
    NVL(r.SGRECEITA, 'NI')
"""

SQL_RDT_MUNICIPIO = """
SELECT
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado') AS SEGMENTO_FISCAL,
    rdt.AACOMPETENCIA                         AS ANO,
    rdt.MMCOMPETENCIA                         AS MES,
    NVL(tipotrib.DSTIPOTRIBUTACAO, 'Não Informado') AS TIPO_TRIBUTACAO,
    CASE
        WHEN est.FLSUBSTITUTOTRIBUT = 'S' THEN 'Substituto Tributário'
        WHEN est.FLSUBSTITUTOTRIBUT = 'N' THEN 'Não Substituto Tributário'
        ELSE 'Não Informado'
    END AS SUBSTITUTO_TRIBUTARIO_DESC,
    NVL(RTRIM(rec.SGRECEITA), 'NI')            AS RECEITA,
    NVL(evento.NMEVENTOLANCAM, 'Não Informado') AS EVENTO_LANCAMENTO,
    NVL(loc.DSLOCALIZACAORDT, 'Não Informado') AS LOCALIZACAO_RDT,
    CASE
        WHEN rdt.FLRDTBLOQUEADO = 1 THEN 'Bloqueado'
        WHEN rdt.FLRDTBLOQUEADO = 0 THEN 'Desbloqueado'
        ELSE 'Não Informado'
    END AS RDT_BLOQUEIO_DESC,
    CASE
        WHEN rdt.FLEXIGIBILIDADESUSPENSA = 1 THEN 'Inexigível'
        WHEN rdt.FLEXIGIBILIDADESUSPENSA = 0 THEN 'Exigível'
        ELSE 'Não Informado'
    END AS EXIGIBILIDADE_DESC,
    CASE
        WHEN rdt.FLRDTLIQUIDADO = 1 THEN 'Liquidado'
        WHEN rdt.FLRDTLIQUIDADO = 2 THEN 'Parcialmente Liquidado'
        WHEN rdt.FLRDTLIQUIDADO = 0 OR rdt.FLRDTLIQUIDADO IS NULL THEN 'Sem Liquidação'
        ELSE 'Não Informado'
    END AS LIQUIDACAO_DESC,
    CASE
        WHEN rdt.FLPAGO = 1 THEN 'Pago'
        WHEN rdt.FLPAGO = 0 THEN 'Não Pago'
        ELSE 'Não Informado'
    END AS PAGAMENTO_DESC,
    CASE
        WHEN rdt.CDSISTEMARDT = 1 THEN 'NOTA SALVADOR'
        WHEN rdt.CDSISTEMARDT = 2 THEN 'DMS EDUCAÇÃO'
        ELSE 'Outro/Não Informado'
    END AS SISTEMA_RDT_DESC,
    SUM(NVL(rdt.VLRESUMODECLARACAOTRIBUTARIA, 0)) AS VALOR_RDT,
    COUNT(*) AS QTD_RDTS
FROM DBASATFIN.RESUMODECLARACAOTRIBUTARIA rdt
LEFT JOIN DBASAT.ESTABELECIMENTO est
       ON rdt.NUCGABASE = est.NUCGABASE
      AND rdt.NUCGAFILIAL = est.NUCGAFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
       ON est.NUCGABASE = cseg.NUCGABASE
      AND est.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
       ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
LEFT JOIN DBASAT.TIPOTRIBUTACAO tipotrib
       ON est.CDTIPOTRIBUTACAO = tipotrib.CDTIPOTRIBUTACAO
LEFT JOIN DBASAT.RECEITA rec
       ON rdt.CDRECEITA = rec.CDRECEITA
LEFT JOIN DBASAT.EVENTOLANCAMENTO evento
       ON rdt.CDEVENTOLANCAM = evento.CDEVENTOLANCAM
      AND rdt.CDRECEITA = evento.CDRECEITA
LEFT JOIN DBASATFIN.LOCALIZACAORDT loc
       ON rdt.CDSITUACAOLOCALIZACAORDT = loc.CDLOCALIZACAORDT
WHERE (rdt.AACOMPETENCIA * 100 + rdt.MMCOMPETENCIA) BETWEEN :comp_inicio AND :comp_fim
GROUP BY
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado'),
    rdt.AACOMPETENCIA,
    rdt.MMCOMPETENCIA,
    NVL(tipotrib.DSTIPOTRIBUTACAO, 'Não Informado'),
    CASE
        WHEN est.FLSUBSTITUTOTRIBUT = 'S' THEN 'Substituto Tributário'
        WHEN est.FLSUBSTITUTOTRIBUT = 'N' THEN 'Não Substituto Tributário'
        ELSE 'Não Informado'
    END,
    NVL(RTRIM(rec.SGRECEITA), 'NI'),
    NVL(evento.NMEVENTOLANCAM, 'Não Informado'),
    NVL(loc.DSLOCALIZACAORDT, 'Não Informado'),
    CASE
        WHEN rdt.FLRDTBLOQUEADO = 1 THEN 'Bloqueado'
        WHEN rdt.FLRDTBLOQUEADO = 0 THEN 'Desbloqueado'
        ELSE 'Não Informado'
    END,
    CASE
        WHEN rdt.FLEXIGIBILIDADESUSPENSA = 1 THEN 'Inexigível'
        WHEN rdt.FLEXIGIBILIDADESUSPENSA = 0 THEN 'Exigível'
        ELSE 'Não Informado'
    END,
    CASE
        WHEN rdt.FLRDTLIQUIDADO = 1 THEN 'Liquidado'
        WHEN rdt.FLRDTLIQUIDADO = 2 THEN 'Parcialmente Liquidado'
        WHEN rdt.FLRDTLIQUIDADO = 0 OR rdt.FLRDTLIQUIDADO IS NULL THEN 'Sem Liquidação'
        ELSE 'Não Informado'
    END,
    CASE
        WHEN rdt.FLPAGO = 1 THEN 'Pago'
        WHEN rdt.FLPAGO = 0 THEN 'Não Pago'
        ELSE 'Não Informado'
    END,
    CASE
        WHEN rdt.CDSISTEMARDT = 1 THEN 'NOTA SALVADOR'
        WHEN rdt.CDSISTEMARDT = 2 THEN 'DMS EDUCAÇÃO'
        ELSE 'Outro/Não Informado'
    END
"""

SQL_RDT_INADIMPLENCIA_MUNICIPIO = """
SELECT
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado') AS SEGMENTO_FISCAL,
    rdt.AACOMPETENCIA                         AS ANO,
    rdt.MMCOMPETENCIA                         AS MES,
    SUM(NVL(inad.VLSALDOORIGINAL, 0))         AS VALOR_SALDO_ORIGINAL_INAD,
    SUM(NVL(inad.VLMULTAMORA, 0))             AS VALOR_MULTA_MORA_INAD,
    SUM(NVL(inad.VLJUROSMORA, 0))             AS VALOR_JUROS_MORA_INAD,
    SUM(NVL(inad.VLCORRECAOMONETARIA, 0))     AS VALOR_CORRECAO_INAD,
    SUM(NVL(inad.VLTOTALDEBITO, 0))           AS VALOR_TOTAL_DEBITO_INAD,
    SUM(NVL(inad.VLORIGINAL, 0))              AS VALOR_ORIGINAL_INAD,
    COUNT(*)                                  AS QTD_REGISTROS
FROM DBASATFIN.INADIMPLENCIADEBITOSRDT inad
JOIN DBASATFIN.RESUMODECLARACAOTRIBUTARIA rdt
  ON rdt.SQRESUMODECLARACAOTRIBUTARIA = inad.SQRESUMODECLARACAOTRIBUTARIA
LEFT JOIN DBASAT.ESTABELECIMENTO est
       ON rdt.NUCGABASE = est.NUCGABASE
      AND rdt.NUCGAFILIAL = est.NUCGAFILIAL
LEFT JOIN DBASAT.CONTRIBUINTESEGMENTACAO cseg
       ON est.NUCGABASE = cseg.NUCGABASE
      AND est.NUCGAFILIAL = cseg.NUCGAFILIAL
LEFT JOIN DBASAT.SEGMENTOFISCAL sf
       ON cseg.SQSEGMENTOFISCAL = sf.SQSEGMENTOFISCAL
WHERE inad.DTENTRADA IS NOT NULL
  AND (rdt.AACOMPETENCIA * 100 + rdt.MMCOMPETENCIA) BETWEEN :comp_inicio AND :comp_fim
GROUP BY
    NVL(sf.NMSEGMENTOFISCAL, 'Não Informado'),
    rdt.AACOMPETENCIA,
    rdt.MMCOMPETENCIA
"""


# =============================================================================
# CARGAS
# =============================================================================

def carregar_arrecadacao(params, manter_staging: bool):
    tabela = "stg_arrecadacao_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo arrecadação municipal...")
    df = executar_oracle_df(oracle_sat_config, SQL_ARRECADACAO_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


def carregar_nfse(params, manter_staging: bool):
    tabela = "stg_nfse_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo NFSe / atividade econômica municipal...")
    df = executar_oracle_df(oracle_dwh_config, SQL_NFSE_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


def carregar_inadimplencia(params, manter_staging: bool):
    tabela = "stg_inadimplencia_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo inadimplência municipal...")
    df = executar_oracle_df(oracle_sat_config, SQL_INADIMPLENCIA_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


def carregar_fiscalizacoes(params, manter_staging: bool):
    tabela = "stg_fiscalizacoes_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo fiscalizações municipais...")
    df = executar_oracle_df(oracle_sat_config, SQL_FISCALIZACOES_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


def carregar_valores_auto(params, manter_staging: bool):
    tabela = "stg_valores_auto_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo valores de autos municipais...")
    df = executar_oracle_df(oracle_sat_config, SQL_VALORES_AUTO_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


def carregar_rdt(params, manter_staging: bool):
    tabela = "stg_rdt_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo RDT municipal...")
    df = executar_oracle_df(oracle_sat_config, SQL_RDT_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


def carregar_rdt_inadimplencia(params, manter_staging: bool):
    tabela = "stg_rdt_inadimplencia_municipio"
    if not manter_staging:
        truncar_tabela(tabela)
    log("Extraindo inadimplência de RDT municipal...")
    df = executar_oracle_df(oracle_sat_config, SQL_RDT_INADIMPLENCIA_MUNICIPIO, params)
    return carregar_dataframe(df, tabela)


# =============================================================================
# MAIN
# =============================================================================

def main():
    args = parse_args()
    inicio, fim = obter_janela(args)
    fim_exclusivo = fim + timedelta(days=1)

    params = {
        "dt_inicio": inicio,
        "dt_fim_exclusivo": fim_exclusivo,
        "comp_inicio": competencia_yyyymm(inicio),
        "comp_fim": competencia_yyyymm(fim),
        "ano_inicio": inicio.year,
        "ano_fim": fim.year,
    }

    log("Iniciando extração dos consolidados municipais.")
    log(f"Janela: {inicio} a {fim} | competências {params['comp_inicio']} a {params['comp_fim']}")

    cargas = [
        ("arrecadacao", carregar_arrecadacao),
        ("nfse", carregar_nfse),
        ("inadimplencia", carregar_inadimplencia),
        ("fiscalizacoes", carregar_fiscalizacoes),
        ("valores_auto", carregar_valores_auto),
        ("rdt", carregar_rdt),
        ("rdt_inadimplencia", carregar_rdt_inadimplencia),
    ]

    total_por_carga = {}
    for nome, func in cargas:
        if not deve_executar(nome, args.somente):
            continue
        try:
            total_por_carga[nome] = func(params, args.manter_staging)
        except Exception as exc:
            log(f"[ERRO] Falha na carga '{nome}': {exc}")
            raise

    log("Extração concluída.")
    for nome, total in total_por_carga.items():
        log(f"Resumo - {nome}: {total:,} linhas")


if __name__ == "__main__":
    main()
