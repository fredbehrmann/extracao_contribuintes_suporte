# etl_dim_tempo.py — Carga da dimensão tempo (Postgres)
# - Gera calendário (2010-2040)
# - Não depende de locale do SO para nomes/siglas PT-BR
# - Insere/atualiza via ON CONFLICT(data)
# - Valida comprimentos para evitar truncation em varchar(3)

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import date, timedelta

# ==============================
# Configuração da conexão
# ==============================
pg_config_sefaz = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}
SCHEMA = "suporte_arrecadacao"
TABELA = "dim_tempo"

# ==============================
# Dicionários PT-BR (não dependem de locale)
# ==============================
MESES = {
    1: ("Janeiro", "Jan"),
    2: ("Fevereiro", "Fev"),
    3: ("Março", "Mar"),
    4: ("Abril", "Abr"),
    5: ("Maio", "Mai"),
    6: ("Junho", "Jun"),
    7: ("Julho", "Jul"),
    8: ("Agosto", "Ago"),
    9: ("Setembro", "Set"),
    10: ("Outubro", "Out"),
    11: ("Novembro", "Nov"),
    12: ("Dezembro", "Dez"),
}

# dia_semana no seu modelo: 1=Dom, 2=Seg, ..., 7=Sáb
DIAS = {
    1: ("Domingo", "Dom"),
    2: ("Segunda-feira", "Seg"),
    3: ("Terça-feira", "Ter"),
    4: ("Quarta-feira", "Qua"),
    5: ("Quinta-feira", "Qui"),
    6: ("Sexta-feira", "Sex"),
    7: ("Sábado", "Sáb"),
}

# ==============================
# Função para gerar calendário
# ==============================
def gerar_calendario(inicio: date, fim: date) -> pd.DataFrame:
    datas = []
    delta = timedelta(days=1)
    atual = inicio

    while atual <= fim:
        nome_mes, sigla_mes = MESES[atual.month]

        # isoweekday: 1=Seg ... 7=Dom
        # seu padrão: 1=Dom ... 7=Sáb
        dia_semana = atual.isoweekday() % 7 + 1
        nome_dia, sigla_dia = DIAS[dia_semana]

        datas.append(
            {
                "data": atual,
                "ano": atual.year,
                "semestre": 1 if atual.month <= 6 else 2,
                "trimestre": (atual.month - 1) // 3 + 1,
                "mes": atual.month,
                "nome_mes": nome_mes,
                "sigla_mes": sigla_mes,  # garantido 3
                "dia": atual.day,
                "nome_dia": nome_dia,
                "sigla_dia": sigla_dia,  # garantido 3
                "dia_semana": dia_semana,
                "dia_ano": atual.timetuple().tm_yday,
                "semana_ano": atual.isocalendar()[1],
                "eh_final_semana": atual.weekday() >= 5,
            }
        )
        atual += delta

    return pd.DataFrame(datas)


# ==============================
# Validação de tamanho (varchar(3))
# ==============================
def validar_tamanhos(df: pd.DataFrame) -> None:
    cols_3 = ["sigla_mes", "sigla_dia"]
    for c in cols_3:
        lens = df[c].astype(str).map(len)
        max_len = int(lens.max())
        if max_len > 3:
            idx = int(lens.idxmax())
            val = df.loc[idx, c]
            raise ValueError(
                f"[ERRO] Coluna '{c}' excedeu 3 caracteres (max={max_len}). Exemplo: '{val}'"
            )


# ==============================
# Função de carga
# ==============================
def carregar_dim_tempo():
    print("[INFO] Gerando calendário de 2010 a 2040...")
    df = gerar_calendario(date(1990, 1, 1), date(2040, 12, 31))
    print(f"[INFO] {len(df)} registros gerados.")

    # valida antes de enviar ao banco
    validar_tamanhos(df)

    conn = psycopg2.connect(**pg_config_sefaz)
    cur = conn.cursor()

    sql = f"""
        INSERT INTO {SCHEMA}.{TABELA} (
            data, ano, semestre, trimestre, mes, nome_mes, sigla_mes,
            dia, nome_dia, sigla_dia, dia_semana, dia_ano,
            semana_ano, eh_final_semana
        ) VALUES (
            %(data)s, %(ano)s, %(semestre)s, %(trimestre)s, %(mes)s, %(nome_mes)s, %(sigla_mes)s,
            %(dia)s, %(nome_dia)s, %(sigla_dia)s, %(dia_semana)s, %(dia_ano)s,
            %(semana_ano)s, %(eh_final_semana)s
        )
        ON CONFLICT (data) DO UPDATE SET
            ano = EXCLUDED.ano,
            semestre = EXCLUDED.semestre,
            trimestre = EXCLUDED.trimestre,
            mes = EXCLUDED.mes,
            nome_mes = EXCLUDED.nome_mes,
            sigla_mes = EXCLUDED.sigla_mes,
            dia = EXCLUDED.dia,
            nome_dia = EXCLUDED.nome_dia,
            sigla_dia = EXCLUDED.sigla_dia,
            dia_semana = EXCLUDED.dia_semana,
            dia_ano = EXCLUDED.dia_ano,
            semana_ano = EXCLUDED.semana_ano,
            eh_final_semana = EXCLUDED.eh_final_semana;
    """

    registros = df.to_dict(orient="records")

    try:
        execute_batch(cur, sql, registros, page_size=1000)
        conn.commit()
        print(
            f"[INFO] Carga concluída: {len(df)} registros inseridos/atualizados em {SCHEMA}.{TABELA}."
        )
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    carregar_dim_tempo()
