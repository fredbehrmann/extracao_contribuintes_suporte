# etl_fatos_fiscalizacao_otimizado.py
# Versão otimizada para carga set-based das tabelas:
#   - suporte_arrecadacao.fato_fiscalizacoes
#   - suporte_arrecadacao.fato_valores_auto
#
# Principais melhorias:
#   1. Remove loops linha a linha com SELECTs individuais.
#   2. Faz carga por INSERT ... SELECT com JOINs no próprio PostgreSQL.
#   3. Resolve dimensões em massa.
#   4. Insere membros faltantes das dimensões de fiscalização em lote.
#   5. Usa upsert idempotente, evitando somar qtd_registros em reprocessamentos.
#   6. Cria índices auxiliares para acelerar os JOINs, quando ainda não existirem.

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


def obter_ano(default: int | None = None) -> int | None:
    """
    Mantido para compatibilidade com o orquestrador.
    Este script não filtra diretamente por ano, pois trabalha sobre as stagings já carregadas.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--ano", type=int, default=None, help="Ano de referência da execução.")
    args, _ = parser.parse_known_args()
    ano = args.ano or os.getenv("ANO") or default
    return int(ano) if ano is not None else None


ANO = obter_ano(default=None)


# =========================================================
# UTILITÁRIOS
# =========================================================

def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def executar(conn, sql: str, descricao: str | None = None) -> int:
    """
    Executa um SQL e retorna rowcount quando disponível.
    """
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


def consultar_valor(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


# =========================================================
# ÍNDICES AUXILIARES
# =========================================================

def criar_indices_auxiliares(conn):
    """
    Índices voltados para acelerar os JOINs das cargas set-based.
    São seguros com IF NOT EXISTS.
    """
    sql = f"""
    CREATE INDEX IF NOT EXISTS ix_stg_fisc_situacoes_num_prog
        ON {SCHEMA}.stg_fisc_situacoes (numero_programacao);

    CREATE INDEX IF NOT EXISTS ix_stg_fisc_situacoes_data
        ON {SCHEMA}.stg_fisc_situacoes (dtsituacaofiscalizacao);

    CREATE INDEX IF NOT EXISTS ix_stg_fisc_situacoes_desc
        ON {SCHEMA}.stg_fisc_situacoes (dssituacaofiscalizacao);

    CREATE INDEX IF NOT EXISTS ix_stg_fisc_programacoes_num_prog
        ON {SCHEMA}.stg_fisc_programacoes (numero_programacao);

    CREATE INDEX IF NOT EXISTS ix_stg_fisc_programacoes_inscricao
        ON {SCHEMA}.stg_fisc_programacoes (inscricao_municipal);

    CREATE INDEX IF NOT EXISTS ix_stg_fisc_valores_autos_prog
        ON {SCHEMA}.stg_fisc_valores_autos (numero_programacao);

    CREATE INDEX IF NOT EXISTS ix_stg_fisc_valores_autos_notif
        ON {SCHEMA}.stg_fisc_valores_autos (numero_notificacao, ano);

    CREATE INDEX IF NOT EXISTS ix_dim_contribuinte_municipal_cga
        ON {SCHEMA}.dim_contribuinte_municipal (cga);

    CREATE INDEX IF NOT EXISTS ix_dim_tempo_data
        ON {SCHEMA}.dim_tempo (data);

    CREATE INDEX IF NOT EXISTS ix_dim_tempo_ano_mes_dia
        ON {SCHEMA}.dim_tempo (ano, mes, dia);

    CREATE INDEX IF NOT EXISTS ix_dim_programacao_fiscal_num_prog
        ON {SCHEMA}.dim_programacao_fiscal (numero_programacao);

    CREATE INDEX IF NOT EXISTS ix_dim_situacao_fiscalizacao_desc
        ON {SCHEMA}.dim_situacao_fiscalizacao (descricao);

    CREATE INDEX IF NOT EXISTS ix_dim_notificacao_auto_num_ano
        ON {SCHEMA}.dim_notificacao_auto (numero_notificacao, ano_notificacao);
    """
    executar(conn, sql, "[1/7] Criando índices auxiliares, se necessário...")


# =========================================================
# DIMENSÕES FALTANTES EM LOTE
# =========================================================

def garantir_dim_programacao_fiscal(conn):
    """
    Garante programações fiscais faltantes a partir das duas stagings usadas nas fatos.
    """
    sql = f"""
    INSERT INTO {SCHEMA}.dim_programacao_fiscal (
        numero_programacao,
        nome_auditor,
        descricao_programacao,
        extensao,
        mes_ano_referencia,
        tipo_programacao
    )
    SELECT DISTINCT
        x.numero_programacao::bigint AS numero_programacao,
        'Não Informado' AS nome_auditor,
        'Não Informado' AS descricao_programacao,
        'Não Informado' AS extensao,
        'NI' AS mes_ano_referencia,
        'Não Informado' AS tipo_programacao
    FROM (
        SELECT NULLIF(numero_programacao::text, '')::bigint AS numero_programacao
        FROM {SCHEMA}.stg_fisc_situacoes
        WHERE numero_programacao IS NOT NULL

        UNION

        SELECT NULLIF(numero_programacao::text, '')::bigint AS numero_programacao
        FROM {SCHEMA}.stg_fisc_valores_autos
        WHERE numero_programacao IS NOT NULL
    ) x
    LEFT JOIN {SCHEMA}.dim_programacao_fiscal d
           ON d.numero_programacao = x.numero_programacao
    WHERE x.numero_programacao IS NOT NULL
      AND d.id_programacao_fiscal IS NULL
    ON CONFLICT (numero_programacao) DO NOTHING;
    """
    executar(conn, sql, "[2/7] Garantindo dim_programacao_fiscal em lote...")


def garantir_dim_situacao_fiscalizacao(conn):
    sql = f"""
    INSERT INTO {SCHEMA}.dim_situacao_fiscalizacao (
        descricao,
        situacao_normalizada
    )
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(s.dssituacaofiscalizacao::text), ''), 'Não Informado') AS descricao,
        'OUTRAS' AS situacao_normalizada
    FROM {SCHEMA}.stg_fisc_situacoes s
    LEFT JOIN {SCHEMA}.dim_situacao_fiscalizacao d
           ON d.descricao = COALESCE(NULLIF(TRIM(s.dssituacaofiscalizacao::text), ''), 'Não Informado')
    WHERE d.id_situacao_fiscalizacao IS NULL
    ON CONFLICT (descricao) DO NOTHING;
    """
    executar(conn, sql, "[3/7] Garantindo dim_situacao_fiscalizacao em lote...")


def garantir_dim_notificacao_auto(conn):
    sql = f"""
    INSERT INTO {SCHEMA}.dim_notificacao_auto (
        numero_notificacao,
        ano_notificacao,
        tipo_notificacao,
        situacao_auto,
        fato,
        data_registro_auto
    )
    SELECT DISTINCT
        COALESCE(NULLIF(v.numero_notificacao::text, '')::bigint, 0) AS numero_notificacao,
        COALESCE(NULLIF(v.ano::text, '')::int, 1900) AS ano_notificacao,
        'Não Informado' AS tipo_notificacao,
        'Não Informado' AS situacao_auto,
        'Não Informado' AS fato,
        DATE '1900-01-01' AS data_registro_auto
    FROM {SCHEMA}.stg_fisc_valores_autos v
    LEFT JOIN {SCHEMA}.dim_notificacao_auto d
           ON d.numero_notificacao = COALESCE(NULLIF(v.numero_notificacao::text, '')::bigint, 0)
          AND d.ano_notificacao = COALESCE(NULLIF(v.ano::text, '')::int, 1900)
    WHERE d.id_notificacao_auto IS NULL
    ON CONFLICT (numero_notificacao, ano_notificacao) DO NOTHING;
    """
    executar(conn, sql, "[4/7] Garantindo dim_notificacao_auto em lote...")


# =========================================================
# CARGA DAS FATOS
# =========================================================

def carregar_fato_fiscalizacoes(conn):
    """
    Carga set-based da fato_fiscalizacoes.

    Observação:
    - A versão antiga fazia vários SELECTs por linha para resolver programação, contribuinte, tempo e situação.
    - Esta versão resolve tudo com JOINs em massa.
    - O update de qtd_eventos é idempotente: recebe o valor agregado da execução atual.
    """
    qtd_stg = consultar_valor(conn, f"SELECT COUNT(*) FROM {SCHEMA}.stg_fisc_situacoes;")
    log(f"[5/7] stg_fisc_situacoes possui {qtd_stg:,} registros.")

    sql = f"""
    WITH prog_map AS (
        SELECT distinct
            p.numero_programacao::bigint AS numero_programacao,
            NULLIF(TRIM(p.inscricao_municipal::text), '') AS inscricao_municipal
        FROM {SCHEMA}.stg_fisc_programacoes p
        WHERE p.numero_programacao IS NOT NULL
        --GROUP BY p.numero_programacao::bigint
    ),
    base AS (
        SELECT
            dpf.id_programacao_fiscal,
            COALESCE(dt.id_tempo, -1) AS id_tempo,
            dsf.id_situacao_fiscalizacao,
            COALESCE(dcm.id_contribuinte_municipal, -1) AS id_contribuinte_municipal,
            COUNT(*)::integer AS qtd_eventos
        FROM {SCHEMA}.stg_fisc_situacoes s
        JOIN {SCHEMA}.dim_programacao_fiscal dpf
          ON dpf.numero_programacao = NULLIF(s.numero_programacao::text, '')::bigint
        JOIN {SCHEMA}.dim_situacao_fiscalizacao dsf
          ON dsf.descricao = COALESCE(NULLIF(TRIM(s.dssituacaofiscalizacao::text), ''), 'Não Informado')
        LEFT JOIN {SCHEMA}.dim_tempo dt
          ON dt.data = COALESCE(s.dtsituacaofiscalizacao::date, DATE '1900-01-01')
        LEFT JOIN prog_map pm
          ON pm.numero_programacao = NULLIF(s.numero_programacao::text, '')::bigint
        LEFT JOIN {SCHEMA}.dim_contribuinte_municipal dcm
          ON dcm.cga = pm.inscricao_municipal
        WHERE s.numero_programacao IS NOT NULL
        GROUP BY
            dpf.id_programacao_fiscal,
            COALESCE(dt.id_tempo, -1),
            dsf.id_situacao_fiscalizacao,
            COALESCE(dcm.id_contribuinte_municipal, -1)
    )
    INSERT INTO {SCHEMA}.fato_fiscalizacoes (
        id_programacao_fiscal,
        id_tempo,
        id_situacao_fiscalizacao,
        id_contribuinte_municipal,
        qtd_eventos
    )
    SELECT
        id_programacao_fiscal,
        id_tempo,
        id_situacao_fiscalizacao,
        id_contribuinte_municipal,
        qtd_eventos
    FROM base
    ON CONFLICT (id_programacao_fiscal, id_tempo, id_situacao_fiscalizacao, id_contribuinte_municipal)
    DO UPDATE SET
    --    id_contribuinte_municipal = EXCLUDED.id_contribuinte_municipal,
        qtd_eventos = EXCLUDED.qtd_eventos;
    """
    executar(conn, sql, "[5/7] Carregando fato_fiscalizacoes em modo set-based...")


def carregar_fato_valores_auto(conn):
    """
    Carga set-based da fato_valores_auto.

    A staging é agregada pela chave de conflito:
        id_notificacao_auto, id_tempo, sigla_receita

    Isso evita erro de múltiplas linhas tentando atualizar a mesma linha no mesmo INSERT.
    """
    qtd_stg = consultar_valor(conn, f"SELECT COUNT(*) FROM {SCHEMA}.stg_fisc_valores_autos;")
    log(f"[6/7] stg_fisc_valores_autos possui {qtd_stg:,} registros.")

    sql = f"""
    WITH prog_map AS (
        SELECT
            p.numero_programacao::bigint AS numero_programacao,
            MAX(NULLIF(TRIM(p.inscricao_municipal::text), '')) AS inscricao_municipal
        FROM {SCHEMA}.stg_fisc_programacoes p
        WHERE p.numero_programacao IS NOT NULL
        GROUP BY p.numero_programacao::bigint
    ),
    base AS (
        SELECT
            dna.id_notificacao_auto,
            dpf.id_programacao_fiscal,
            COALESCE(dcm.id_contribuinte_municipal, -1) AS id_contribuinte_municipal,
            COALESCE(dt.id_tempo, -1) AS id_tempo,
            COALESCE(NULLIF(TRIM(v.sigla_receita::text), ''), 'NI') AS sigla_receita,
            SUM(COALESCE(v.vl_moeda_original, 0))::numeric(18,2) AS vl_moeda_original,
            SUM(COALESCE(v.vl_correcao_monetaria, 0))::numeric(18,2) AS vl_correcao_monetaria,
            SUM(COALESCE(v.vl_multa_infracao, 0))::numeric(18,2) AS vl_multa_infracao,
            SUM(COALESCE(v.vl_multa_mora, 0))::numeric(18,2) AS vl_multa_mora,
            SUM(COALESCE(v.vl_juros_mora, 0))::numeric(18,2) AS vl_juros_mora
        FROM {SCHEMA}.stg_fisc_valores_autos v
        JOIN {SCHEMA}.dim_programacao_fiscal dpf
          ON dpf.numero_programacao = NULLIF(v.numero_programacao::text, '')::bigint
        JOIN {SCHEMA}.dim_notificacao_auto dna
          ON dna.numero_notificacao = COALESCE(NULLIF(v.numero_notificacao::text, '')::bigint, 0)
         AND dna.ano_notificacao = COALESCE(NULLIF(v.ano::text, '')::int, 1900)
        LEFT JOIN {SCHEMA}.dim_tempo dt
          ON dt.ano = COALESCE(NULLIF(v.ano::text, '')::int, 1900)
         AND dt.mes = COALESCE(NULLIF(v.mes::text, '')::int, 1)
         AND dt.dia = 1
        LEFT JOIN prog_map pm
          ON pm.numero_programacao = NULLIF(v.numero_programacao::text, '')::bigint
        LEFT JOIN {SCHEMA}.dim_contribuinte_municipal dcm
          ON dcm.cga = pm.inscricao_municipal
        WHERE v.numero_programacao IS NOT NULL
        GROUP BY
            dna.id_notificacao_auto,
            dpf.id_programacao_fiscal,
            COALESCE(dcm.id_contribuinte_municipal, -1),
            COALESCE(dt.id_tempo, -1),
            COALESCE(NULLIF(TRIM(v.sigla_receita::text), ''), 'NI')
    ),
    base_unica AS (
        SELECT
            id_notificacao_auto,
            MAX(id_programacao_fiscal) AS id_programacao_fiscal,
            MAX(id_contribuinte_municipal) AS id_contribuinte_municipal,
            id_tempo,
            sigla_receita,
            SUM(vl_moeda_original)::numeric(18,2) AS vl_moeda_original,
            SUM(vl_correcao_monetaria)::numeric(18,2) AS vl_correcao_monetaria,
            SUM(vl_multa_infracao)::numeric(18,2) AS vl_multa_infracao,
            SUM(vl_multa_mora)::numeric(18,2) AS vl_multa_mora,
            SUM(vl_juros_mora)::numeric(18,2) AS vl_juros_mora
        FROM base
        GROUP BY
            id_notificacao_auto,
            id_tempo,
            sigla_receita
    )
    INSERT INTO {SCHEMA}.fato_valores_auto (
        id_notificacao_auto,
        id_programacao_fiscal,
        id_contribuinte_municipal,
        id_tempo,
        sigla_receita,
        vl_moeda_original,
        vl_correcao_monetaria,
        vl_multa_infracao,
        vl_multa_mora,
        vl_juros_mora
    )
    SELECT
        id_notificacao_auto,
        id_programacao_fiscal,
        id_contribuinte_municipal,
        id_tempo,
        sigla_receita,
        vl_moeda_original,
        vl_correcao_monetaria,
        vl_multa_infracao,
        vl_multa_mora,
        vl_juros_mora
    FROM base_unica
    ON CONFLICT (id_notificacao_auto, id_tempo, sigla_receita)
    DO UPDATE SET
        id_programacao_fiscal = EXCLUDED.id_programacao_fiscal,
        id_contribuinte_municipal = EXCLUDED.id_contribuinte_municipal,
        vl_moeda_original = EXCLUDED.vl_moeda_original,
        vl_correcao_monetaria = EXCLUDED.vl_correcao_monetaria,
        vl_multa_infracao = EXCLUDED.vl_multa_infracao,
        vl_multa_mora = EXCLUDED.vl_multa_mora,
        vl_juros_mora = EXCLUDED.vl_juros_mora;
    """
    executar(conn, sql, "[6/7] Carregando fato_valores_auto em modo set-based...")


# =========================================================
# VALIDAÇÕES
# =========================================================

def validar_excecoes(conn):
    sql = f"""
    SELECT 'fato_fiscalizacoes.id_tempo = -1' AS alerta, COUNT(*) AS qtd
    FROM {SCHEMA}.fato_fiscalizacoes
    WHERE id_tempo = -1

    UNION ALL

    SELECT 'fato_fiscalizacoes.id_contribuinte_municipal = -1' AS alerta, COUNT(*) AS qtd
    FROM {SCHEMA}.fato_fiscalizacoes
    WHERE id_contribuinte_municipal = -1

    UNION ALL

    SELECT 'fato_valores_auto.id_tempo = -1' AS alerta, COUNT(*) AS qtd
    FROM {SCHEMA}.fato_valores_auto
    WHERE id_tempo = -1

    UNION ALL

    SELECT 'fato_valores_auto.id_contribuinte_municipal = -1' AS alerta, COUNT(*) AS qtd
    FROM {SCHEMA}.fato_valores_auto
    WHERE id_contribuinte_municipal = -1;
    """
    log("[7/7] Validando registros carregados com membros técnicos -1...")
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        for alerta, qtd in rows:
            log(f"       {alerta}: {qtd:,}")


# =========================================================
# MAIN
# =========================================================

def main():
    log("Iniciando carga otimizada das fatos de fiscalização.")
    if ANO:
        log(f"Ano recebido por parâmetro: {ANO} (usado apenas para compatibilidade com orquestrador).")

    t0 = time.time()

    with psycopg2.connect(**pg_config_sefaz) as conn:
        criar_indices_auxiliares(conn)
        garantir_dim_programacao_fiscal(conn)
        garantir_dim_situacao_fiscalizacao(conn)
        garantir_dim_notificacao_auto(conn)
        carregar_fato_fiscalizacoes(conn)
        carregar_fato_valores_auto(conn)
        validar_excecoes(conn)

    log(f"Carga otimizada concluída em {time.time() - t0:.1f}s.")


if __name__ == "__main__":
    main()
