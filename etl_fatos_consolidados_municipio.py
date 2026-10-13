# etl_fatos_consolidados_municipio.py
# Carga das tabelas destino de consolidados municipais a partir das stagings.
#
# Objetivo:
#   Ler as tabelas stg_*_municipio e carregar/upsert nas respectivas fato_*_municipio,
#   sem individualização por CNPJ/CGA/contribuinte, permitindo comparação com a carteira
#   de contribuintes suporte.
#
# Padrão adotado:
#   - Carga set-based no PostgreSQL via INSERT ... SELECT ... ON CONFLICT.
#   - Lookup de tempo diretamente na dim_tempo por make_date(ano, mes, 1).
#   - Uso de COALESCE para padronizar dimensões textuais nulas.
#   - Idempotência: reprocessar a mesma staging atualiza os registros já existentes.

import os
import time
import argparse
import psycopg2
from psycopg2.extras import DictCursor


# =========================================================
# CONFIGURAÇÕES
# =========================================================
SCHEMA = os.getenv("PG_SCHEMA", "suporte_arrecadacao")

pg_config_sefaz = {
    "dbname": os.getenv("PG_DBNAME", "db_rfb"),
    "user": os.getenv("PG_USER", "cif_user"),
    "password": os.getenv("RFB_PG_PASS") or os.getenv("PG_PASSWORD", ""),
    "host": os.getenv("PG_HOST", "valedosrios"),
    "port": int(os.getenv("PG_PORT", "5432")),
}

CARGAS_DISPONIVEIS = [
    "arrecadacao",
    "nfse",
    "inadimplencia",
    "fiscalizacoes",
    "valores_auto",
    "rdt",
    "rdt_inadimplencia",
]


# =========================================================
# UTILITÁRIOS
# =========================================================
def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def conectar_pg():
    return psycopg2.connect(**pg_config_sefaz)


def executar(conn, sql: str, descricao: str | None = None) -> int:
    if descricao:
        log(descricao)

    t0 = time.time()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql)
        rowcount = cur.rowcount

    conn.commit()

    if descricao:
        log(f"Concluído em {time.time() - t0:.1f}s | rowcount={rowcount}")

    return rowcount


def tabela_tem_registros(conn, tabela: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.{tabela} LIMIT 1)")
        return bool(cur.fetchone()[0])


def validar_staging(conn, tabela: str) -> None:
    if not tabela_tem_registros(conn, tabela):
        log(f"[AVISO] {SCHEMA}.{tabela} está vazia. Carga ignorada.")
        raise RuntimeError("STAGING_VAZIA")


def tratar_carga(nome: str, func, conn) -> int:
    try:
        return func(conn)
    except RuntimeError as exc:
        if str(exc) == "STAGING_VAZIA":
            return 0
        raise
    except Exception as exc:
        conn.rollback()
        log(f"[ERRO] Falha na carga '{nome}': {exc}")
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Carrega fatos consolidados municipais a partir das tabelas staging."
    )
    parser.add_argument(
        "--somente",
        nargs="+",
        choices=CARGAS_DISPONIVEIS,
        help="Executa apenas as cargas informadas. Ex.: --somente arrecadacao nfse",
    )
    parser.add_argument(
        "--truncar-destino",
        action="store_true",
        help=(
            "Trunca as tabelas destino das cargas selecionadas antes da carga. "
            "Use com cuidado. O padrão é upsert idempotente sem truncar."
        ),
    )
    return parser.parse_args()


# =========================================================
# LIMPEZA OPCIONAL DE DESTINO
# =========================================================
TABELA_DESTINO_POR_CARGA = {
    "arrecadacao": "fato_arrecadacao_municipio",
    "nfse": "fato_nfse_municipio",
    "inadimplencia": "fato_inadimplencia_municipio",
    "fiscalizacoes": "fato_fiscalizacoes_municipio",
    "valores_auto": "fato_valores_auto_municipio",
    "rdt": "fato_rdt_municipio",
    "rdt_inadimplencia": "fato_rdt_inadimplencia_municipio",
}


def truncar_destinos(conn, cargas: list[str]) -> None:
    for carga in cargas:
        tabela = TABELA_DESTINO_POR_CARGA[carga]
        executar(conn, f"TRUNCATE TABLE {SCHEMA}.{tabela} RESTART IDENTITY;", f"Truncando {SCHEMA}.{tabela}...")


# =========================================================
# CARGAS SET-BASED
# =========================================================
def carregar_arrecadacao(conn) -> int:
    validar_staging(conn, "stg_arrecadacao_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_arrecadacao_municipio (
        id_tempo,
        receita_evento,
        segmento_fiscal,
        situacao_movimento,
        valor_pago,
        qtd_registros,
        dt_carga
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.receita_evento), ''), 'Não Informado') AS receita_evento,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        COALESCE(NULLIF(BTRIM(s.situacao_movimento), ''), 'Não Informado') AS situacao_movimento,
        ROUND(SUM(COALESCE(s.valor_pago, 0)), 2) AS valor_pago,
        SUM(COALESCE(s.qtd_registros, 0))::integer AS qtd_registros,
        NOW() AS dt_carga
    FROM {SCHEMA}.stg_arrecadacao_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, s.mes, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.receita_evento), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.situacao_movimento), ''), 'Não Informado')
    ON CONFLICT (id_tempo, receita_evento, segmento_fiscal, situacao_movimento)
    DO UPDATE SET
        valor_pago = EXCLUDED.valor_pago,
        qtd_registros = EXCLUDED.qtd_registros,
        dt_carga = NOW();
    """
    return executar(conn, sql, "Carregando fato_arrecadacao_municipio...")


def carregar_nfse(conn) -> int:
    validar_staging(conn, "stg_nfse_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_nfse_municipio (
        id_tempo,
        segmento_fiscal,
        codigo_atividade,
        beneficio_fiscal,
        tipo_tributacao,
        regime_tributario,
        status_nota,
        valor_servico,
        valor_iss,
        valor_iss_proprio,
        valor_iss_retido,
        valor_iss_proprio_pago,
        valor_iss_retido_pago,
        valor_deducoes,
        valor_credito,
        qtd_notas,
        dt_carga
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        COALESCE(NULLIF(BTRIM(s.codigo_atividade), ''), 'NI') AS codigo_atividade,
        COALESCE(NULLIF(BTRIM(s.beneficio_fiscal), ''), 'Não Informado') AS beneficio_fiscal,
        COALESCE(NULLIF(BTRIM(s.tipo_tributacao), ''), 'Não Informado') AS tipo_tributacao,
        COALESCE(NULLIF(BTRIM(s.regime_tributario), ''), 'Não Informado') AS regime_tributario,
        COALESCE(NULLIF(BTRIM(s.status_nota), ''), 'Não Informado') AS status_nota,
        ROUND(SUM(COALESCE(s.valor_servico, 0)), 2) AS valor_servico,
        ROUND(SUM(COALESCE(s.valor_iss, 0)), 2) AS valor_iss,
        ROUND(SUM(COALESCE(s.valor_iss_proprio, 0)), 2) AS valor_iss_proprio,
        ROUND(SUM(COALESCE(s.valor_iss_retido, 0)), 2) AS valor_iss_retido,
        ROUND(SUM(COALESCE(s.valor_iss_proprio_pago, 0)), 2) AS valor_iss_proprio_pago,
        ROUND(SUM(COALESCE(s.valor_iss_retido_pago, 0)), 2) AS valor_iss_retido_pago,
        ROUND(SUM(COALESCE(s.valor_deducoes, 0)), 2) AS valor_deducoes,
        ROUND(SUM(COALESCE(s.valor_credito, 0)), 2) AS valor_credito,
        SUM(COALESCE(s.qtd_notas, 0))::integer AS qtd_notas,
        NOW() AS dt_carga
    FROM {SCHEMA}.stg_nfse_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, s.mes, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.codigo_atividade), ''), 'NI'),
        COALESCE(NULLIF(BTRIM(s.beneficio_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.tipo_tributacao), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.regime_tributario), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.status_nota), ''), 'Não Informado')
    ON CONFLICT (
        id_tempo,
        segmento_fiscal,
        codigo_atividade,
        beneficio_fiscal,
        tipo_tributacao,
        regime_tributario,
        status_nota
    )
    DO UPDATE SET
        valor_servico = EXCLUDED.valor_servico,
        valor_iss = EXCLUDED.valor_iss,
        valor_iss_proprio = EXCLUDED.valor_iss_proprio,
        valor_iss_retido = EXCLUDED.valor_iss_retido,
        valor_iss_proprio_pago = EXCLUDED.valor_iss_proprio_pago,
        valor_iss_retido_pago = EXCLUDED.valor_iss_retido_pago,
        valor_deducoes = EXCLUDED.valor_deducoes,
        valor_credito = EXCLUDED.valor_credito,
        qtd_notas = EXCLUDED.qtd_notas,
        dt_carga = NOW();
    """
    return executar(conn, sql, "Carregando fato_nfse_municipio...")


def carregar_inadimplencia(conn) -> int:
    validar_staging(conn, "stg_inadimplencia_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_inadimplencia_municipio (
        id_tempo,
        segmento_fiscal,
        receita,
        valor_debito,
        valor_total_debito,
        qtd_registros,
        dt_carga,
        registro_ativo
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        COALESCE(NULLIF(BTRIM(s.receita), ''), 'NI') AS receita,
        ROUND(SUM(COALESCE(s.valor_debito, 0)), 2) AS valor_debito,
        ROUND(SUM(COALESCE(s.valor_total_debito, 0)), 2) AS valor_total_debito,
        SUM(COALESCE(s.qtd_registros, 0))::integer AS qtd_registros,
        NOW() AS dt_carga,
        TRUE AS registro_ativo
    FROM {SCHEMA}.stg_inadimplencia_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, 1, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.receita), ''), 'NI')
    ON CONFLICT (id_tempo, segmento_fiscal, receita) WHERE registro_ativo
    DO UPDATE SET
        valor_debito = EXCLUDED.valor_debito,
        valor_total_debito = EXCLUDED.valor_total_debito,
        qtd_registros = EXCLUDED.qtd_registros,
        dt_carga = NOW(),
        registro_ativo = TRUE;
    """
    return executar(conn, sql, "Carregando fato_inadimplencia_municipio...")


def carregar_fiscalizacoes(conn) -> int:
    validar_staging(conn, "stg_fiscalizacoes_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_fiscalizacoes_municipio (
        id_tempo,
        segmento_fiscal,
        situacao_fiscalizacao,
        tipo_programacao,
        extensao,
        qtd_eventos,
        dt_carga
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        COALESCE(NULLIF(BTRIM(s.situacao_fiscalizacao), ''), 'Não Informado') AS situacao_fiscalizacao,
        COALESCE(NULLIF(BTRIM(s.tipo_programacao), ''), 'Não Informado') AS tipo_programacao,
        COALESCE(NULLIF(BTRIM(s.extensao), ''), 'Não Informado') AS extensao,
        SUM(COALESCE(s.qtd_eventos, 0))::integer AS qtd_eventos,
        NOW() AS dt_carga
    FROM {SCHEMA}.stg_fiscalizacoes_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, s.mes, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.situacao_fiscalizacao), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.tipo_programacao), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.extensao), ''), 'Não Informado')
    ON CONFLICT (id_tempo, segmento_fiscal, situacao_fiscalizacao, tipo_programacao, extensao)
    DO UPDATE SET
        qtd_eventos = EXCLUDED.qtd_eventos,
        dt_carga = NOW();
    """
    return executar(conn, sql, "Carregando fato_fiscalizacoes_municipio...")


def carregar_valores_auto(conn) -> int:
    validar_staging(conn, "stg_valores_auto_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_valores_auto_municipio (
        id_tempo,
        segmento_fiscal,
        sigla_receita,
        vl_moeda_original,
        vl_correcao_monetaria,
        vl_multa_infracao,
        vl_multa_mora,
        vl_juros_mora,
        qtd_autos,
        dt_carga
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        COALESCE(NULLIF(BTRIM(s.sigla_receita), ''), 'NI') AS sigla_receita,
        ROUND(SUM(COALESCE(s.vl_moeda_original, 0)), 2) AS vl_moeda_original,
        ROUND(SUM(COALESCE(s.vl_correcao_monetaria, 0)), 2) AS vl_correcao_monetaria,
        ROUND(SUM(COALESCE(s.vl_multa_infracao, 0)), 2) AS vl_multa_infracao,
        ROUND(SUM(COALESCE(s.vl_multa_mora, 0)), 2) AS vl_multa_mora,
        ROUND(SUM(COALESCE(s.vl_juros_mora, 0)), 2) AS vl_juros_mora,
        SUM(COALESCE(s.qtd_autos, 0))::integer AS qtd_autos,
        NOW() AS dt_carga
    FROM {SCHEMA}.stg_valores_auto_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, s.mes, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.sigla_receita), ''), 'NI')
    ON CONFLICT (id_tempo, segmento_fiscal, sigla_receita)
    DO UPDATE SET
        vl_moeda_original = EXCLUDED.vl_moeda_original,
        vl_correcao_monetaria = EXCLUDED.vl_correcao_monetaria,
        vl_multa_infracao = EXCLUDED.vl_multa_infracao,
        vl_multa_mora = EXCLUDED.vl_multa_mora,
        vl_juros_mora = EXCLUDED.vl_juros_mora,
        qtd_autos = EXCLUDED.qtd_autos,
        dt_carga = NOW();
    """
    return executar(conn, sql, "Carregando fato_valores_auto_municipio...")


def carregar_rdt(conn) -> int:
    validar_staging(conn, "stg_rdt_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_rdt_municipio (
        id_tempo,
        segmento_fiscal,
        tipo_tributacao,
        substituto_tributario_desc,
        receita,
        evento_lancamento,
        localizacao_rdt,
        rdt_bloqueio_desc,
        exigibilidade_desc,
        liquidacao_desc,
        pagamento_desc,
        sistema_rdt_desc,
        valor_rdt,
        qtd_rdts,
        dt_carga
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        COALESCE(NULLIF(BTRIM(s.tipo_tributacao), ''), 'Não Informado') AS tipo_tributacao,
        COALESCE(NULLIF(BTRIM(s.substituto_tributario_desc), ''), 'Não Informado') AS substituto_tributario_desc,
        COALESCE(NULLIF(BTRIM(s.receita), ''), 'NI') AS receita,
        COALESCE(NULLIF(BTRIM(s.evento_lancamento), ''), 'Não Informado') AS evento_lancamento,
        COALESCE(NULLIF(BTRIM(s.localizacao_rdt), ''), 'Não Informado') AS localizacao_rdt,
        COALESCE(NULLIF(BTRIM(s.rdt_bloqueio_desc), ''), 'Não Informado') AS rdt_bloqueio_desc,
        COALESCE(NULLIF(BTRIM(s.exigibilidade_desc), ''), 'Não Informado') AS exigibilidade_desc,
        COALESCE(NULLIF(BTRIM(s.liquidacao_desc), ''), 'Não Informado') AS liquidacao_desc,
        COALESCE(NULLIF(BTRIM(s.pagamento_desc), ''), 'Não Informado') AS pagamento_desc,
        COALESCE(NULLIF(BTRIM(s.sistema_rdt_desc), ''), 'Não Informado') AS sistema_rdt_desc,
        ROUND(SUM(COALESCE(s.valor_rdt, 0)), 2) AS valor_rdt,
        SUM(COALESCE(s.qtd_rdts, 0))::integer AS qtd_rdts,
        NOW() AS dt_carga
    FROM {SCHEMA}.stg_rdt_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, s.mes, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.tipo_tributacao), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.substituto_tributario_desc), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.receita), ''), 'NI'),
        COALESCE(NULLIF(BTRIM(s.evento_lancamento), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.localizacao_rdt), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.rdt_bloqueio_desc), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.exigibilidade_desc), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.liquidacao_desc), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.pagamento_desc), ''), 'Não Informado'),
        COALESCE(NULLIF(BTRIM(s.sistema_rdt_desc), ''), 'Não Informado')
    ON CONFLICT (
        id_tempo,
        segmento_fiscal,
        tipo_tributacao,
        substituto_tributario_desc,
        receita,
        evento_lancamento,
        localizacao_rdt,
        rdt_bloqueio_desc,
        exigibilidade_desc,
        liquidacao_desc,
        pagamento_desc,
        sistema_rdt_desc
    )
    DO UPDATE SET
        valor_rdt = EXCLUDED.valor_rdt,
        qtd_rdts = EXCLUDED.qtd_rdts,
        dt_carga = NOW();
    """
    return executar(conn, sql, "Carregando fato_rdt_municipio...")


def carregar_rdt_inadimplencia(conn) -> int:
    validar_staging(conn, "stg_rdt_inadimplencia_municipio")

    sql = f"""
    INSERT INTO {SCHEMA}.fato_rdt_inadimplencia_municipio (
        id_tempo,
        segmento_fiscal,
        valor_saldo_original_inad,
        valor_multa_mora_inad,
        valor_juros_mora_inad,
        valor_correcao_inad,
        valor_total_debito_inad,
        valor_original_inad,
        qtd_registros,
        dt_carga
    )
    SELECT
        COALESCE(dt.id_tempo, -1) AS id_tempo,
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado') AS segmento_fiscal,
        ROUND(SUM(COALESCE(s.valor_saldo_original_inad, 0)), 2) AS valor_saldo_original_inad,
        ROUND(SUM(COALESCE(s.valor_multa_mora_inad, 0)), 2) AS valor_multa_mora_inad,
        ROUND(SUM(COALESCE(s.valor_juros_mora_inad, 0)), 2) AS valor_juros_mora_inad,
        ROUND(SUM(COALESCE(s.valor_correcao_inad, 0)), 2) AS valor_correcao_inad,
        ROUND(SUM(COALESCE(s.valor_total_debito_inad, 0)), 2) AS valor_total_debito_inad,
        ROUND(SUM(COALESCE(s.valor_original_inad, 0)), 2) AS valor_original_inad,
        SUM(COALESCE(s.qtd_registros, 0))::integer AS qtd_registros,
        NOW() AS dt_carga
    FROM {SCHEMA}.stg_rdt_inadimplencia_municipio s
    LEFT JOIN {SCHEMA}.dim_tempo dt
      ON dt.data = make_date(s.ano, s.mes, 1)
    GROUP BY
        COALESCE(dt.id_tempo, -1),
        COALESCE(NULLIF(BTRIM(s.segmento_fiscal), ''), 'Não Informado')
    ON CONFLICT (id_tempo, segmento_fiscal)
    DO UPDATE SET
        valor_saldo_original_inad = EXCLUDED.valor_saldo_original_inad,
        valor_multa_mora_inad = EXCLUDED.valor_multa_mora_inad,
        valor_juros_mora_inad = EXCLUDED.valor_juros_mora_inad,
        valor_correcao_inad = EXCLUDED.valor_correcao_inad,
        valor_total_debito_inad = EXCLUDED.valor_total_debito_inad,
        valor_original_inad = EXCLUDED.valor_original_inad,
        qtd_registros = EXCLUDED.qtd_registros,
        dt_carga = NOW();
    """
    return executar(conn, sql, "Carregando fato_rdt_inadimplencia_municipio...")


FUNCOES_CARGA = {
    "arrecadacao": carregar_arrecadacao,
    "nfse": carregar_nfse,
    "inadimplencia": carregar_inadimplencia,
    "fiscalizacoes": carregar_fiscalizacoes,
    "valores_auto": carregar_valores_auto,
    "rdt": carregar_rdt,
    "rdt_inadimplencia": carregar_rdt_inadimplencia,
}


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    args = parse_args()
    cargas = args.somente if args.somente else CARGAS_DISPONIVEIS

    log("Iniciando carga dos fatos consolidados municipais.")
    log(f"Cargas selecionadas: {', '.join(cargas)}")

    total_por_carga: dict[str, int] = {}

    with conectar_pg() as conn:
        if args.truncar_destino:
            truncar_destinos(conn, cargas)

        for nome in cargas:
            total_por_carga[nome] = tratar_carga(nome, FUNCOES_CARGA[nome], conn)

    log("Resumo da carga:")
    for nome, total in total_por_carga.items():
        log(f" - {nome}: rowcount={total}")

    log("Carga finalizada.")


if __name__ == "__main__":
    main()
