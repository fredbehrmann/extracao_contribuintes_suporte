# -*- coding: utf-8 -*-
"""
FASE 1 - Montagem da base de monitoramento de notícias com persistência no Postgres.

Desenho desta versão:
- Lê os CNPJs-base alvo no banco SEFAZ.
- Busca dados cadastrais e societários no banco RFB.
- Monta localmente:
    * noticias_empresas_alvo
    * noticias_vinculos_societarios
    * noticias_empresas_relacionadas
    * noticias_consultas_busca
    * noticias_parametros_monitoramento
- Mantém cache local para reduzir acessos desnecessários ao banco.
- O banco passa a ser a fonte principal para as próximas fases.
"""
import os
import re
import json
import time
from pathlib import Path
from typing import List, Optional, Set
import unicodedata

import pandas as pd

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.exc import OperationalError



# =========================================================
# CONFIGURAÇÕES
# =========================================================

pg_config_sefaz = {
    "dbname": "sefaz",
    "user": "postgres",
    "password": os.getenv("SEFAZ_PG_PASS"),
    "host": "localhost",
    "port": 5432
}

pg_config_rfb = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": os.getenv("RFB_PG_HOST", "valedosrios"),
    "port": int(os.getenv("RFB_PG_PORT", "5432"))
}

ANO_REFERENCIA = 2026
ORIGEM_MONITORAMENTO = f"CONTRIBUINTE_SUPORTE_{ANO_REFERENCIA}"

SCHEMA_DESTINO = "suporte_arrecadacao"
TB_EMPRESAS_ALVO = "noticias_empresas_alvo"
TB_VINCULOS = "noticias_vinculos_societarios"
TB_EMPRESAS_REL = "noticias_empresas_relacionadas"
TB_CONSULTAS = "noticias_consultas_busca"
TB_PARAMS = "noticias_parametros_monitoramento"
TB_EXECUCOES = "noticias_execucoes_fase1"

BASE_DIR = Path("cache_monitoramento_noticias")
BASE_DIR.mkdir(exist_ok=True)

ARQUIVO_META = BASE_DIR / "metadata_execucao_fase1.json"

LOTE_CNPJ_BASE = 400
LOTE_CNPJ = 700
LOTE_DOC_SOCIO = 80

PERMITE_BUSCA_POR_CNPJ = True
PERMITE_BUSCA_POR_SOCIO = True
PERMITE_EXPANSAO_SOCIETARIA = True
LIMITE_EMPRESAS_RELACIONADAS_POR_SOCIO = 5

USAR_CACHE = True
FORCAR_ATUALIZACAO_ALVOS = False
FORCAR_ATUALIZACAO_EMPRESAS = False
FORCAR_ATUALIZACAO_ESTAB = False
FORCAR_ATUALIZACAO_SOCIOS_BASE = False
FORCAR_ATUALIZACAO_SOCIOS_REL = False
FORCAR_ATUALIZACAO_EMPRESAS_ALVO = False
FORCAR_ATUALIZACAO_VINCULOS = False
FORCAR_ATUALIZACAO_EMPRESAS_REL = False
FORCAR_ATUALIZACAO_CONSULTAS = False
VERSAO_CACHE_CONSULTAS = "2026-04-16-v2"

DDL_DESTINO = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_DESTINO};

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_EXECUCOES} (
    id_execucao text PRIMARY KEY,
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    inicio_execucao timestamp,
    fim_execucao timestamp,
    status_execucao text,
    total_empresas_alvo integer,
    total_vinculos_societarios integer,
    total_empresas_relacionadas integer,
    total_consultas_busca integer,
    observacao text
);

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_EMPRESAS_ALVO} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    id_empresa_alvo text NOT NULL,
    cnpj_base text,
    cnpj text,
    razao_social text,
    nome_fantasia text,
    matriz_filial text,
    situacao_cadastral text,
    data_situacao_cadastral text,
    motivo_situacao_cadastral text,
    data_inicio_atividades text,
    natureza_juridica text,
    qualificacao_responsavel text,
    porte_empresa text,
    ente_federativo_responsavel text,
    capital_social double precision,
    cnae_fiscal text,
    cnae_fiscal_secundaria text,
    tipo_logradouro text,
    logradouro text,
    numero text,
    complemento text,
    bairro text,
    cep text,
    uf text,
    municipio text,
    ddd1 text,
    telefone1 text,
    ddd2 text,
    telefone2 text,
    correio_eletronico text,
    monitorar text,
    PRIMARY KEY (ano_referencia, origem_monitoramento, id_empresa_alvo)
);

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_VINCULOS} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    id_vinculo text NOT NULL,
    id_empresa_alvo text,
    cnpj_base_alvo text,
    cnpj_alvo text,
    razao_social_alvo text,
    nome_fantasia_alvo text,
    nome_socio text,
    documento_socio text,
    tipo_socio text,
    qualificacao_socio text,
    data_entrada_sociedade text,
    nome_representante text,
    documento_normalizado text,
    PRIMARY KEY (ano_referencia, origem_monitoramento, id_vinculo)
);

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_EMPRESAS_REL} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    id_relacao text NOT NULL,
    id_empresa_alvo text,
    cnpj_base_alvo text,
    razao_social_alvo text,
    documento_socio text,
    nome_socio text,
    cnpj_base_relacionada text,
    cnpj_relacionado text,
    razao_social_relacionada text,
    nome_fantasia_relacionada text,
    situacao_cadastral_relacionada text,
    uf_relacionada text,
    municipio_relacionada text,
    relacao_no_universo_alvo text,
    grau_relacao text,
    PRIMARY KEY (ano_referencia, origem_monitoramento, id_relacao)
);

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_CONSULTAS} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    id_consulta text NOT NULL,
    id_empresa_alvo text,
    tipo_origem text,
    subtipo_consulta text,
    entidade_referencia text,
    consulta_texto text,
    consulta_texto_limpa text,
    prioridade integer,
    ativa text,
    exige_aspas text,
    observacao text,
    PRIMARY KEY (ano_referencia, origem_monitoramento, id_consulta)
);

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_PARAMS} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    parametro text NOT NULL,
    valor text,
    PRIMARY KEY (ano_referencia, origem_monitoramento, parametro)
);

CREATE INDEX IF NOT EXISTS ix_{TB_EMPRESAS_ALVO}_cnpj_base ON {SCHEMA_DESTINO}.{TB_EMPRESAS_ALVO}(cnpj_base);
CREATE INDEX IF NOT EXISTS ix_{TB_VINCULOS}_documento_socio ON {SCHEMA_DESTINO}.{TB_VINCULOS}(documento_socio);
CREATE INDEX IF NOT EXISTS ix_{TB_EMPRESAS_REL}_cnpj_base_rel ON {SCHEMA_DESTINO}.{TB_EMPRESAS_REL}(cnpj_base_relacionada);
CREATE INDEX IF NOT EXISTS ix_{TB_CONSULTAS}_subtipo ON {SCHEMA_DESTINO}.{TB_CONSULTAS}(subtipo_consulta);
"""

# =========================================================
# AUXILIARES
# =========================================================

def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def build_pg_url(cfg: dict) -> str:
    return (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )



def strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(ch)
    )


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)
    value = value.upper()
    return value if value else None


def normalize_text_for_compare(value: Optional[str]) -> Optional[str]:
    value = normalize_text(value)
    if value is None:
        return None
    value = strip_accents(value)
    value = re.sub(r"\s*([()\-/,])\s*", r" \1 ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def normalize_municipio_code(value: Optional[str]) -> Optional[str]:
    digits = only_digits(value)
    if not digits:
        return None
    return str(int(digits))


def is_salvador_municipio(value: Optional[str]) -> bool:
    return normalize_municipio_code(value) == "3849"


UF_SIGLAS: Set[str] = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO"
}

TOKENS_LOCALIZACAO: Set[str] = {
    "AC", "AG", "AGENCIA", "AREA", "BLOCO", "BRASIL", "CAMPUS", "CASA", "CD",
    "CENTRAL", "CENTRO", "CIDADE", "CONDOMINIO", "CONJUNTO", "DISTRITO", "ED",
    "EDIFICIO", "ESTACAO", "FILIAL", "GALPAO", "JARDIM", "LOJA", "MATRIZ",
    "MODULO", "NUCLEO", "PARQUE", "PATIO", "PAVILHAO", "PCA", "POLO", "POSTO",
    "PRACA", "PREDIO", "QUADRA", "REGIONAL", "SALA", "SEDE", "SETOR", "SHOPPING",
    "TORRE", "UBS", "UC", "UN", "UNID", "UNIDADE", "VILA"
}

STOPWORDS_GEOGRAFICAS: Set[str] = {
    "A", "AL", "AO", "AS", "BEIRA", "DA", "DAS", "DE", "DO", "DOS", "E", "EM",
    "KM", "NA", "NO", "NOS", "NOVA", "NOVO", "O", "OS", "SANTO", "SANTA", "SAO",
    "DEL", "LA"
}

MARCADORES_LOCALIZACAO_REGEX = re.compile(
    r"\b(?:AC|AGENCIA|CAMPUS|CENTRO|CIDADE|FILIAL|LOJA|MATRIZ|POSTO|PRACA|REGIONAL|SEDE|SETOR|UNID(?:ADE)?)\b"
)


def is_generic_geographic_fantasy_name(nome_fantasia: Optional[str]) -> bool:
    texto = normalize_text_for_compare(nome_fantasia)
    if not texto:
        return True

    if len(texto) <= 3:
        return True

    tokens = [t for t in re.split(r"\s+", re.sub(r"[()/,\-]", " ", texto)) if t]
    if not tokens:
        return True

    if len(tokens) == 1 and tokens[0] in UF_SIGLAS:
        return True

    if re.fullmatch(r"[A-Z][A-Z\s'.-]+\(([A-Z]{2})\)", texto):
        return True

    if re.fullmatch(r"[A-Z][A-Z\s'.-]+\-\s*[A-Z]{2}", texto):
        return True

    if re.fullmatch(r"[A-Z][A-Z\s'.-]+\s+[A-Z]{2}", texto) and len(tokens) <= 4:
        return True

    uf_tokens = sum(1 for t in tokens if t in UF_SIGLAS)
    loc_tokens = sum(1 for t in tokens if t in TOKENS_LOCALIZACAO)
    semantic_tokens = [t for t in tokens if t not in UF_SIGLAS and t not in TOKENS_LOCALIZACAO and t not in STOPWORDS_GEOGRAFICAS]

    if uf_tokens and not semantic_tokens:
        return True

    if uf_tokens and len(semantic_tokens) <= 2 and loc_tokens:
        return True

    if MARCADORES_LOCALIZACAO_REGEX.search(texto) and len(semantic_tokens) <= 2:
        return True

    if len(tokens) >= 2 and len(tokens) <= 5 and len(semantic_tokens) <= 2 and (uf_tokens or loc_tokens):
        return True

    return False


def is_meaningful_search_text(value: Optional[str]) -> bool:
    texto = normalize_text_for_compare(value)
    if not texto:
        return False
    if len(re.sub(r"[^A-Z0-9]", "", texto)) < 3:
        return False
    return True


def only_digits(value: Optional[str]) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = re.sub(r"\D", "", str(value))
    return value if value else None


def safe_zfill(value: Optional[str], size: int) -> Optional[str]:
    value = only_digits(value)
    if not value:
        return None
    return value.zfill(size)


def compose_cnpj(cnpj_basico, cnpj_ordem, cnpj_dv) -> Optional[str]:
    b = safe_zfill(cnpj_basico, 8)
    o = safe_zfill(cnpj_ordem, 4) or "0000"
    d = safe_zfill(cnpj_dv, 2) or "00"
    if not b:
        return None
    cnpj = f"{b}{o}{d}"
    return cnpj if len(cnpj) == 14 else None


def infer_tipo_socio(doc: Optional[str]) -> Optional[str]:
    doc = only_digits(doc)
    if not doc:
        return None
    if len(doc) == 11:
        return "PF"
    if len(doc) == 14:
        return "PJ"
    return None


def chunk_list(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def testar_conexao(engine, nome_conexao: str) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log(f"Conexão OK: {nome_conexao}")
        return True
    except OperationalError as e:
        log(f"ERRO de conexão em {nome_conexao}")
        print(str(e))
        return False


def write_metadata(data: dict) -> None:
    with open(ARQUIVO_META, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# =========================================================
# CACHE LOCAL COM FALLBACK
# =========================================================

def get_cache_path(base_path: Path) -> Path:
    """
    Retorna o caminho real do cache.
    Prioriza parquet se pyarrow/fastparquet estiver disponível.
    Caso contrário, usa pickle.
    """
    try:
        import pyarrow  # noqa: F401
        return base_path.with_suffix(".parquet")
    except ImportError:
        try:
            import fastparquet  # noqa: F401
            return base_path.with_suffix(".parquet")
        except ImportError:
            return base_path.with_suffix(".pkl")


def save_df(df: pd.DataFrame, base_path: Path) -> Path:
    """
    Salva DataFrame em parquet quando disponível; caso contrário, salva em pickle.
    Retorna o caminho efetivamente usado.
    """
    path = get_cache_path(base_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_pickle(path)

    return path


def load_df(base_path: Path) -> pd.DataFrame:
    """
    Carrega DataFrame do cache, aceitando parquet ou pickle.
    """
    parquet_path = base_path.with_suffix(".parquet")
    pkl_path = base_path.with_suffix(".pkl")

    if parquet_path.exists() and parquet_path.stat().st_size > 0:
        return pd.read_parquet(parquet_path)

    if pkl_path.exists() and pkl_path.stat().st_size > 0:
        return pd.read_pickle(pkl_path)

    raise FileNotFoundError(f"Nenhum cache encontrado para {base_path}")


def cache_exists(base_path: Path) -> bool:
    parquet_path = base_path.with_suffix(".parquet")
    pkl_path = base_path.with_suffix(".pkl")

    return (
        (parquet_path.exists() and parquet_path.stat().st_size > 0) or
        (pkl_path.exists() and pkl_path.stat().st_size > 0)
    )


# =========================================================
# CAMINHOS DE CACHE
# =========================================================

P_ALVOS = BASE_DIR / f"01_cnpj_base_alvo_{ANO_REFERENCIA}"
P_EMPRESAS = BASE_DIR / f"02_empresas_{ANO_REFERENCIA}"
P_ESTAB = BASE_DIR / f"03_estabelecimentos_{ANO_REFERENCIA}"
P_EMPRESAS_ALVO = BASE_DIR / f"04_empresas_alvo_{ANO_REFERENCIA}"
P_SOCIOS_BASE = BASE_DIR / f"05_socios_base_{ANO_REFERENCIA}"
P_VINCULOS = BASE_DIR / f"06_vinculos_societarios_{ANO_REFERENCIA}"
P_SOCIOS_REL = BASE_DIR / f"07_socios_relacionados_{ANO_REFERENCIA}"
P_EMPRESAS_REL = BASE_DIR / f"08_empresas_relacionadas_{ANO_REFERENCIA}"
P_CONSULTAS = BASE_DIR / f"09_consultas_busca_{ANO_REFERENCIA}"
P_PARAMS = BASE_DIR / f"10_parametros_monitoramento_{ANO_REFERENCIA}"



def ensure_pickle_path(path: Path) -> Path:
    return path.with_suffix(".pkl")


def save_df(df: pd.DataFrame, path: Path) -> None:
    path = ensure_pickle_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(path)


def load_df(path: Path) -> pd.DataFrame:
    return pd.read_pickle(ensure_pickle_path(path))


def cache_exists(path: Path) -> bool:
    return ensure_pickle_path(path).exists()


def write_metadata(metadata: dict) -> None:
    ARQUIVO_META.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def get_stage_metadata_path(path: Path) -> Path:
    return path.with_suffix(".meta.json")


def read_stage_metadata(path: Path) -> dict:
    meta_path = get_stage_metadata_path(path)
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def write_stage_metadata(path: Path, metadata: dict) -> None:
    meta_path = get_stage_metadata_path(path)
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def consultas_cache_valido(path: Path) -> bool:
    if not cache_exists(path):
        return False
    metadata = read_stage_metadata(path)
    return metadata.get("versao_cache_consultas") == VERSAO_CACHE_CONSULTAS


def ensure_schema_and_tables(engine) -> None:
    with engine.begin() as conn:
        for stmt in [s.strip() for s in DDL_DESTINO.split(";") if s.strip()]:
            conn.execute(text(stmt))


def delete_ano_origem(conn, table_name: str) -> None:
    conn.execute(
        text(f"DELETE FROM {SCHEMA_DESTINO}.{table_name} WHERE ano_referencia = :ano AND origem_monitoramento = :origem"),
        {"ano": ANO_REFERENCIA, "origem": ORIGEM_MONITORAMENTO}
    )


def persistir_dataframe(engine, table_name: str, df: pd.DataFrame) -> None:
    df = df.copy()
    if "ano_referencia" not in df.columns:
        df.insert(0, "ano_referencia", ANO_REFERENCIA)
    if "origem_monitoramento" not in df.columns:
        df.insert(1, "origem_monitoramento", ORIGEM_MONITORAMENTO)

    with engine.begin() as conn:
        delete_ano_origem(conn, table_name)
    if not df.empty:
        df.to_sql(table_name, engine, schema=SCHEMA_DESTINO, if_exists="append", index=False, method="multi", chunksize=1000)


def registrar_execucao_fase1(engine, metadata: dict, status: str = "SUCESSO", observacao: str = "") -> None:
    from uuid import uuid4
    row = {
        "id_execucao": str(uuid4()),
        "ano_referencia": ANO_REFERENCIA,
        "origem_monitoramento": ORIGEM_MONITORAMENTO,
        "inicio_execucao": metadata.get("inicio_execucao"),
        "fim_execucao": metadata.get("fim_execucao"),
        "status_execucao": status,
        "total_empresas_alvo": metadata.get("total_empresas_alvo"),
        "total_vinculos_societarios": metadata.get("total_vinculos_societarios"),
        "total_empresas_relacionadas": metadata.get("total_empresas_relacionadas"),
        "total_consultas_busca": metadata.get("total_consultas_busca"),
        "observacao": observacao[:2000] if observacao else None,
    }
    pd.DataFrame([row]).to_sql(TB_EXECUCOES, engine, schema=SCHEMA_DESTINO, if_exists="append", index=False)

# =========================================================
# SQL
# =========================================================

SQL_CNPJ_BASE_ALVO_SEFAZ = """
SELECT DISTINCT
       lpad(regexp_replace(cnpj_base, '\D', '', 'g'), 8, '0') AS cnpj_base
FROM suporte_arrecadacao.stg_contribuintes_suporte
WHERE cnpj_base IS NOT NULL
ORDER BY 1
"""

SQL_EMPRESAS_LOTE = text("""
SELECT
    lpad(regexp_replace(cnpj_basico, '\D', '', 'g'), 8, '0') AS cnpj_base,
    razao_social,
    natureza_juridica,
    qualificacao_responsavel,
    porte_empresa,
    ente_federativo_responsavel,
    capital_social
FROM public.empresas
WHERE lpad(regexp_replace(cnpj_basico, '\D', '', 'g'), 8, '0') IN :bases
""").bindparams(bindparam("bases", expanding=True))

SQL_ESTAB_LOTE = text("""
SELECT
    lpad(regexp_replace(cnpj_basico, '\D', '', 'g'), 8, '0') AS cnpj_base,
    lpad(regexp_replace(cnpj_ordem, '\D', '', 'g'), 4, '0') AS cnpj_ordem,
    lpad(regexp_replace(cnpj_dv, '\D', '', 'g'), 2, '0') AS cnpj_dv,
    regexp_replace(cnpj, '\D', '', 'g') AS cnpj,
    matriz_filial,
    nome_fantasia,
    situacao_cadastral,
    data_situacao_cadastral,
    motivo_situacao_cadastral,
    data_inicio_atividades,
    cnae_fiscal,
    cnae_fiscal_secundaria,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    cep,
    uf,
    municipio,
    ddd1,
    telefone1,
    ddd2,
    telefone2,
    correio_eletronico
FROM public.estabelecimento
WHERE lpad(regexp_replace(cnpj_basico, '\D', '', 'g'), 8, '0') IN :bases
""").bindparams(bindparam("bases", expanding=True))

SQL_SOCIOS_POR_CNPJ = text("""
SELECT
    regexp_replace(cnpj, '\D', '', 'g') AS cnpj,
    lpad(regexp_replace(cnpj_basico, '\D', '', 'g'), 8, '0') AS cnpj_base,
    identificador_de_socio,
    upper(trim(nome_socio)) AS nome_socio,
    regexp_replace(cnpj_cpf_socio, '\D', '', 'g') AS cnpj_cpf_socio,
    upper(trim(qualificacao_socio)) AS qualificacao_socio,
    data_entrada_sociedade,
    upper(trim(pais)) AS pais,
    representante_legal,
    upper(trim(nome_representante)) AS nome_representante,
    upper(trim(qualificacao_representante_legal)) AS qualificacao_representante_legal,
    upper(trim(faixa_etaria)) AS faixa_etaria
FROM public.socios
WHERE regexp_replace(cnpj, '\D', '', 'g') IN :cnpjs
""").bindparams(bindparam("cnpjs", expanding=True))

SQL_SOCIOS_POR_DOCUMENTO = text("""
SELECT
    regexp_replace(cnpj, '\D', '', 'g') AS cnpj,
    lpad(regexp_replace(cnpj_basico, '\D', '', 'g'), 8, '0') AS cnpj_base,
    identificador_de_socio,
    upper(trim(nome_socio)) AS nome_socio,
    regexp_replace(cnpj_cpf_socio, '\D', '', 'g') AS cnpj_cpf_socio,
    upper(trim(qualificacao_socio)) AS qualificacao_socio,
    data_entrada_sociedade,
    upper(trim(pais)) AS pais,
    representante_legal,
    upper(trim(nome_representante)) AS nome_representante,
    upper(trim(qualificacao_representante_legal)) AS qualificacao_representante_legal,
    upper(trim(faixa_etaria)) AS faixa_etaria
FROM public.socios
WHERE regexp_replace(cnpj_cpf_socio, '\D', '', 'g') IN :docs
""").bindparams(bindparam("docs", expanding=True))


# =========================================================
# EXTRAÇÃO LOCAL-FIRST
# =========================================================

def obter_cnpj_base_alvo(engine_sefaz) -> pd.DataFrame:
    if USAR_CACHE and cache_exists(P_ALVOS) and not FORCAR_ATUALIZACAO_ALVOS:
        log("Usando cache local de cnpj_base alvo")
        return load_df(P_ALVOS)

    log("Consultando cnpj_base alvo no banco SEFAZ")
    with engine_sefaz.connect() as conn:
        df = pd.read_sql(text(SQL_CNPJ_BASE_ALVO_SEFAZ), conn, params={"ano": ANO_REFERENCIA})

    df = df.drop_duplicates().reset_index(drop=True)
    save_df(df, P_ALVOS)
    return df


def obter_empresas(engine_rfb, cnpj_bases: List[str]) -> pd.DataFrame:
    if USAR_CACHE and cache_exists(P_EMPRESAS) and not FORCAR_ATUALIZACAO_EMPRESAS:
        log("Usando cache local de empresas")
        return load_df(P_EMPRESAS)

    frames = []
    with engine_rfb.connect() as conn:
        for i, lote in enumerate(chunk_list(cnpj_bases, LOTE_CNPJ_BASE), start=1):
            log(f"Consultando empresas - lote {i} com {len(lote)} cnpj_base")
            frames.append(pd.read_sql(SQL_EMPRESAS_LOTE, conn, params={"bases": lote}))

    df = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
    save_df(df, P_EMPRESAS)
    return df


def obter_estabelecimentos(engine_rfb, cnpj_bases: List[str]) -> pd.DataFrame:
    if USAR_CACHE and cache_exists(P_ESTAB) and not FORCAR_ATUALIZACAO_ESTAB:
        log("Usando cache local de estabelecimentos")
        return load_df(P_ESTAB)

    frames = []
    with engine_rfb.connect() as conn:
        for i, lote in enumerate(chunk_list(cnpj_bases, LOTE_CNPJ_BASE), start=1):
            log(f"Consultando estabelecimentos - lote {i} com {len(lote)} cnpj_base")
            frames.append(pd.read_sql(SQL_ESTAB_LOTE, conn, params={"bases": lote}))

    df = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()

    if not df.empty:
        df["cnpj"] = df["cnpj"].apply(only_digits)
        missing = df["cnpj"].isna() | (df["cnpj"].str.len() != 14)
        if missing.any():
            df.loc[missing, "cnpj"] = df.loc[missing].apply(
                lambda r: compose_cnpj(r["cnpj_base"], r["cnpj_ordem"], r["cnpj_dv"]),
                axis=1
            )

    save_df(df, P_ESTAB)
    return df


def obter_socios_base(engine_rfb, cnpjs: List[str]) -> pd.DataFrame:
    if USAR_CACHE and cache_exists(P_SOCIOS_BASE) and not FORCAR_ATUALIZACAO_SOCIOS_BASE:
        log("Usando cache local de sócios base")
        return load_df(P_SOCIOS_BASE)

    frames = []
    with engine_rfb.connect() as conn:
        for i, lote in enumerate(chunk_list(cnpjs, LOTE_CNPJ), start=1):
            log(f"Consultando sócios base - lote {i} com {len(lote)} CNPJs")
            frames.append(pd.read_sql(SQL_SOCIOS_POR_CNPJ, conn, params={"cnpjs": lote}))

    df = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
    save_df(df, P_SOCIOS_BASE)
    return df


def obter_socios_relacionados(engine_rfb, docs_socios: List[str]) -> pd.DataFrame:
    if USAR_CACHE and cache_exists(P_SOCIOS_REL) and not FORCAR_ATUALIZACAO_SOCIOS_REL:
        log("Usando cache local de sócios relacionados")
        return load_df(P_SOCIOS_REL)

    frames = []
    with engine_rfb.connect() as conn:
        for i, lote in enumerate(chunk_list(docs_socios, LOTE_DOC_SOCIO), start=1):
            log(f"Consultando sócios relacionados - lote {i} com {len(lote)} documentos")
            df_lote = pd.read_sql(SQL_SOCIOS_POR_DOCUMENTO, conn, params={"docs": lote})
            if not df_lote.empty:
                df_lote["lote_origem"] = i
                frames.append(df_lote)
                save_df(df_lote, BASE_DIR / "lotes_socios_rel" / f"lote_{i:04d}")

    df = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
    save_df(df, P_SOCIOS_REL)
    return df


# =========================================================
# TRANSFORMAÇÕES LOCAIS
# =========================================================

def preparar_empresas_alvo(df_alvos, df_empresas, df_estab) -> pd.DataFrame:
    df = df_alvos.merge(df_empresas, on="cnpj_base", how="left")
    df = df.merge(df_estab, on="cnpj_base", how="left", suffixes=("", "_est"))

    df["id_empresa_alvo"] = df.apply(
        lambda r: f"{r['cnpj_base']}_{r['cnpj']}" if pd.notna(r["cnpj"]) else f"{r['cnpj_base']}_SEM_CNPJ",
        axis=1
    )
    df["ano_referencia"] = ANO_REFERENCIA
    df["origem_monitoramento"] = ORIGEM_MONITORAMENTO
    df["monitorar"] = "S"

    cols = [
        "id_empresa_alvo", "ano_referencia", "cnpj_base", "cnpj", "razao_social", "nome_fantasia",
        "matriz_filial", "situacao_cadastral", "data_situacao_cadastral", "motivo_situacao_cadastral",
        "data_inicio_atividades", "natureza_juridica", "qualificacao_responsavel", "porte_empresa",
        "ente_federativo_responsavel", "capital_social", "cnae_fiscal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio",
        "ddd1", "telefone1", "ddd2", "telefone2", "correio_eletronico", "origem_monitoramento", "monitorar"
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].drop_duplicates().reset_index(drop=True)

    if "matriz_filial" in df.columns:
        df["ordem_matriz"] = df["matriz_filial"].astype(str).eq("1").astype(int)
        df = df.sort_values(["cnpj_base", "ordem_matriz", "cnpj"], ascending=[True, False, True]).drop(columns=["ordem_matriz"])

    return df.reset_index(drop=True)


def preparar_vinculos_societarios(df_socios_base, df_empresas_alvo) -> pd.DataFrame:
    ref = df_empresas_alvo[["id_empresa_alvo", "cnpj_base", "cnpj", "razao_social", "nome_fantasia"]].drop_duplicates()

    df = df_socios_base.merge(ref, on=["cnpj_base", "cnpj"], how="left")
    df["documento_socio"] = df["cnpj_cpf_socio"].apply(only_digits)
    df["tipo_socio"] = df["documento_socio"].apply(infer_tipo_socio)
    df["documento_normalizado"] = df["documento_socio"]
    df["id_vinculo"] = (
        df["id_empresa_alvo"].fillna("SEM_EMPRESA") + "_" +
        df["documento_normalizado"].fillna("SEM_DOC") + "_" +
        df["nome_socio"].fillna("SEM_NOME")
    )

    df = df.rename(columns={
        "cnpj_base": "cnpj_base_alvo",
        "cnpj": "cnpj_alvo",
        "razao_social": "razao_social_alvo",
        "nome_fantasia": "nome_fantasia_alvo"
    })

    cols = [
        "id_vinculo", "id_empresa_alvo", "cnpj_base_alvo", "cnpj_alvo", "razao_social_alvo",
        "nome_fantasia_alvo", "nome_socio", "documento_socio", "tipo_socio", "qualificacao_socio",
        "data_entrada_sociedade", "nome_representante", "documento_normalizado"
    ]
    return df[cols].drop_duplicates().reset_index(drop=True)


def preparar_empresas_relacionadas(df_vinculos, df_socios_rel, df_empresas, df_estab, df_empresas_alvo) -> pd.DataFrame:
    if df_vinculos.empty or df_socios_rel.empty:
        return pd.DataFrame(columns=[
            "id_relacao", "id_empresa_alvo", "cnpj_base_alvo", "razao_social_alvo",
            "documento_socio", "nome_socio", "cnpj_base_relacionada", "cnpj_relacionado",
            "razao_social_relacionada", "nome_fantasia_relacionada", "situacao_cadastral_relacionada",
            "uf_relacionada", "municipio_relacionada", "relacao_no_universo_alvo", "grau_relacao"
        ])

    origem = df_vinculos[
        ["id_empresa_alvo", "cnpj_base_alvo", "cnpj_alvo", "razao_social_alvo", "documento_socio", "nome_socio"]
    ].drop_duplicates()

    rel = df_socios_rel.copy()
    rel["documento_socio"] = rel["cnpj_cpf_socio"].apply(only_digits)

    pares = origem.merge(
        rel[["cnpj_base", "cnpj", "documento_socio", "nome_socio"]],
        on="documento_socio",
        how="inner",
        suffixes=("_origem", "_rel")
    )

    pares = pares[
        ~(
            (pares["cnpj_base_alvo"] == pares["cnpj_base"]) &
            (pares["cnpj_alvo"] == pares["cnpj"])
        )
    ].copy()

    pares["ord"] = pares.groupby(["id_empresa_alvo", "documento_socio"]).cumcount() + 1
    pares = pares[pares["ord"] <= LIMITE_EMPRESAS_RELACIONADAS_POR_SOCIO].drop(columns=["ord"])

    emp_rel = df_empresas[["cnpj_base", "razao_social"]].drop_duplicates().rename(columns={
        "cnpj_base": "cnpj_base_relacionada",
        "razao_social": "razao_social_relacionada"
    })

    estab_ref = df_estab.copy()
    estab_ref["ordem_matriz"] = estab_ref["matriz_filial"].astype(str).eq("1").astype(int)
    estab_ref = estab_ref.sort_values(["cnpj_base", "ordem_matriz", "cnpj"], ascending=[True, False, True])
    estab_ref = estab_ref.drop_duplicates(subset=["cnpj_base"], keep="first")[[
        "cnpj_base", "cnpj", "nome_fantasia", "situacao_cadastral", "uf", "municipio"
    ]].rename(columns={
        "cnpj_base": "cnpj_base_relacionada",
        "cnpj": "cnpj_relacionado_pref",
        "nome_fantasia": "nome_fantasia_relacionada",
        "situacao_cadastral": "situacao_cadastral_relacionada",
        "uf": "uf_relacionada",
        "municipio": "municipio_relacionada"
    })

    pares = pares.rename(columns={"cnpj_base": "cnpj_base_relacionada", "cnpj": "cnpj_relacionado_raw"})
    pares = pares.merge(emp_rel, on="cnpj_base_relacionada", how="left")
    pares = pares.merge(estab_ref, on="cnpj_base_relacionada", how="left")

    pares["cnpj_relacionado"] = pares["cnpj_relacionado_pref"].fillna(pares["cnpj_relacionado_raw"])

    universo = set(df_empresas_alvo["cnpj_base"].dropna().astype(str).tolist())
    pares["relacao_no_universo_alvo"] = pares["cnpj_base_relacionada"].isin(universo).map({True: "S", False: "N"})
    pares["grau_relacao"] = "EMPRESA_RELACIONADA"
    pares["id_relacao"] = (
        pares["id_empresa_alvo"].fillna("SEM_EMPRESA") + "_" +
        pares["documento_socio"].fillna("SEM_DOC") + "_" +
        pares["cnpj_base_relacionada"].fillna("SEM_REL")
    )

    cols = [
        "id_relacao", "id_empresa_alvo", "cnpj_base_alvo", "razao_social_alvo", "documento_socio",
        "nome_socio_origem", "cnpj_base_relacionada", "cnpj_relacionado", "razao_social_relacionada",
        "nome_fantasia_relacionada", "situacao_cadastral_relacionada", "uf_relacionada",
        "municipio_relacionada", "relacao_no_universo_alvo", "grau_relacao"
    ]

    df_final = pares[cols].rename(columns={"nome_socio_origem": "nome_socio"}).drop_duplicates().reset_index(drop=True)
    return df_final



def preparar_consultas_busca(df_empresas_alvo, df_vinculos, df_empresas_rel) -> pd.DataFrame:
    registros = []

    def add(**kwargs):
        if not is_meaningful_search_text(kwargs.get("consulta_texto_limpa")):
            return
        registros.append(kwargs)

    if df_empresas_alvo.empty:
        return pd.DataFrame(columns=[
            "id_consulta", "id_empresa_alvo", "tipo_origem", "subtipo_consulta",
            "entidade_referencia", "consulta_texto", "consulta_texto_limpa",
            "prioridade", "ativa", "exige_aspas", "observacao"
        ])

    base_emp = df_empresas_alvo.copy()
    base_emp["municipio_normalizado"] = base_emp["municipio"].apply(normalize_municipio_code)
    base_emp = base_emp[base_emp["municipio_normalizado"].eq("3849")].copy()

    if base_emp.empty:
        return pd.DataFrame(columns=[
            "id_consulta", "id_empresa_alvo", "tipo_origem", "subtipo_consulta",
            "entidade_referencia", "consulta_texto", "consulta_texto_limpa",
            "prioridade", "ativa", "exige_aspas", "observacao"
        ])

    base_emp = base_emp[[
        "id_empresa_alvo", "cnpj", "razao_social", "nome_fantasia", "monitorar"
    ]].drop_duplicates()

    ids_empresas_validas = set(base_emp["id_empresa_alvo"].dropna().astype(str).tolist())

    for _, row in base_emp.iterrows():
        if row["monitorar"] != "S":
            continue

        textos_utilizados = set()
        razao_social = normalize_text(row["razao_social"])
        nome_fantasia = normalize_text(row["nome_fantasia"])

        if razao_social:
            add(
                id_empresa_alvo=row["id_empresa_alvo"],
                tipo_origem="EMPRESA",
                subtipo_consulta="RAZAO_SOCIAL",
                entidade_referencia=row["razao_social"],
                consulta_texto=f'"{row["razao_social"]}"',
                consulta_texto_limpa=row["razao_social"],
                prioridade=1,
                ativa="S",
                exige_aspas="S",
                observacao="Consulta exata pela razão social"
            )
            textos_utilizados.add(normalize_text_for_compare(razao_social))

        if nome_fantasia:
            fantasia_norm = normalize_text_for_compare(nome_fantasia)
            if (
                fantasia_norm and
                fantasia_norm not in textos_utilizados and
                not is_generic_geographic_fantasy_name(nome_fantasia)
            ):
                add(
                    id_empresa_alvo=row["id_empresa_alvo"],
                    tipo_origem="EMPRESA",
                    subtipo_consulta="NOME_FANTASIA",
                    entidade_referencia=row["nome_fantasia"],
                    consulta_texto=f'"{row["nome_fantasia"]}"',
                    consulta_texto_limpa=row["nome_fantasia"],
                    prioridade=2,
                    ativa="S",
                    exige_aspas="S",
                    observacao="Consulta exata pelo nome fantasia"
                )
                textos_utilizados.add(fantasia_norm)

        if row["cnpj"]:
            add(
                id_empresa_alvo=row["id_empresa_alvo"],
                tipo_origem="EMPRESA",
                subtipo_consulta="CNPJ",
                entidade_referencia=row["razao_social"],
                consulta_texto=row["cnpj"],
                consulta_texto_limpa=row["cnpj"],
                prioridade=1,
                ativa="S",
                exige_aspas="N",
                observacao="Consulta pelo CNPJ completo"
            )

    if not df_vinculos.empty:
        vinc = df_vinculos[df_vinculos["id_empresa_alvo"].astype(str).isin(ids_empresas_validas)].copy()
        vinc = vinc[["id_empresa_alvo", "nome_socio", "razao_social_alvo", "nome_fantasia_alvo"]].drop_duplicates()

        for _, row in vinc.iterrows():
            if row["nome_socio"]:
                add(
                    id_empresa_alvo=row["id_empresa_alvo"],
                    tipo_origem="SOCIO",
                    subtipo_consulta="NOME_SOCIO",
                    entidade_referencia=row["razao_social_alvo"],
                    consulta_texto=f'"{row["nome_socio"]}"',
                    consulta_texto_limpa=row["nome_socio"],
                    prioridade=3,
                    ativa="S",
                    exige_aspas="S",
                    observacao="Consulta exata pelo nome do sócio"
                )

                if row["razao_social_alvo"]:
                    add(
                        id_empresa_alvo=row["id_empresa_alvo"],
                        tipo_origem="SOCIO_EMPRESA",
                        subtipo_consulta="SOCIO_RAZAO",
                        entidade_referencia=f'{row["nome_socio"]} / {row["razao_social_alvo"]}',
                        consulta_texto=f'"{row["nome_socio"]}" "{row["razao_social_alvo"]}"',
                        consulta_texto_limpa=f'{row["nome_socio"]} {row["razao_social_alvo"]}',
                        prioridade=5,
                        ativa="S",
                        exige_aspas="S",
                        observacao="Consulta combinando sócio e razão social"
                    )

                nome_fantasia_alvo = row.get("nome_fantasia_alvo")
                if (
                    nome_fantasia_alvo and
                    normalize_text_for_compare(nome_fantasia_alvo) != normalize_text_for_compare(row["razao_social_alvo"]) and
                    not is_generic_geographic_fantasy_name(nome_fantasia_alvo)
                ):
                    add(
                        id_empresa_alvo=row["id_empresa_alvo"],
                        tipo_origem="SOCIO_EMPRESA",
                        subtipo_consulta="SOCIO_FANTASIA",
                        entidade_referencia=f'{row["nome_socio"]} / {row["nome_fantasia_alvo"]}',
                        consulta_texto=f'"{row["nome_socio"]}" "{row["nome_fantasia_alvo"]}"',
                        consulta_texto_limpa=f'{row["nome_socio"]} {row["nome_fantasia_alvo"]}',
                        prioridade=6,
                        ativa="S",
                        exige_aspas="S",
                        observacao="Consulta combinando sócio e nome fantasia"
                    )

    if not df_empresas_rel.empty:
        rel = df_empresas_rel[df_empresas_rel["id_empresa_alvo"].astype(str).isin(ids_empresas_validas)].copy()
        rel = rel[["id_empresa_alvo", "cnpj_relacionado", "razao_social_relacionada"]].drop_duplicates()

        for _, row in rel.iterrows():
            if row["razao_social_relacionada"]:
                add(
                    id_empresa_alvo=row["id_empresa_alvo"],
                    tipo_origem="EMPRESA_RELACIONADA",
                    subtipo_consulta="RAZAO_SOCIAL_RELACIONADA",
                    entidade_referencia=row["razao_social_relacionada"],
                    consulta_texto=f'"{row["razao_social_relacionada"]}"',
                    consulta_texto_limpa=row["razao_social_relacionada"],
                    prioridade=4,
                    ativa="S",
                    exige_aspas="S",
                    observacao="Consulta exata pela razão social da empresa relacionada"
                )

            if row["cnpj_relacionado"]:
                add(
                    id_empresa_alvo=row["id_empresa_alvo"],
                    tipo_origem="EMPRESA_RELACIONADA",
                    subtipo_consulta="CNPJ_RELACIONADO",
                    entidade_referencia=row["razao_social_relacionada"],
                    consulta_texto=row["cnpj_relacionado"],
                    consulta_texto_limpa=row["cnpj_relacionado"],
                    prioridade=4,
                    ativa="S",
                    exige_aspas="N",
                    observacao="Consulta pelo CNPJ da empresa relacionada"
                )

    df = pd.DataFrame(registros)
    if df.empty:
        return pd.DataFrame(columns=[
            "id_consulta", "id_empresa_alvo", "tipo_origem", "subtipo_consulta",
            "entidade_referencia", "consulta_texto", "consulta_texto_limpa",
            "prioridade", "ativa", "exige_aspas", "observacao"
        ])

    df["consulta_texto_norm"] = df["consulta_texto_limpa"].apply(normalize_text_for_compare)
    df = df.drop_duplicates(
        subset=["id_empresa_alvo", "subtipo_consulta", "consulta_texto_norm"],
        keep="first"
    ).copy()

    df["ordem"] = df.groupby(["id_empresa_alvo"]).cumcount() + 1
    df["id_consulta"] = df["id_empresa_alvo"].fillna("SEM_EMPRESA") + "_" + df["ordem"].astype(str)
    df = df.drop(columns=["ordem", "consulta_texto_norm"])

    cols = [
        "id_consulta", "id_empresa_alvo", "tipo_origem", "subtipo_consulta",
        "entidade_referencia", "consulta_texto", "consulta_texto_limpa",
        "prioridade", "ativa", "exige_aspas", "observacao"
    ]
    return df[cols].reset_index(drop=True)


def preparar_parametros_monitoramento(df_empresas_alvo, df_vinculos, df_empresas_rel, df_consultas) -> pd.DataFrame:
    return pd.DataFrame([
        {"parametro": "ano_referencia", "valor": str(ANO_REFERENCIA)},
        {"parametro": "data_geracao_base", "valor": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")},
        {"parametro": "origem_monitoramento", "valor": ORIGEM_MONITORAMENTO},
        {"parametro": "usar_cache", "valor": "S" if USAR_CACHE else "N"},
        {"parametro": "permite_busca_por_cnpj", "valor": "S" if PERMITE_BUSCA_POR_CNPJ else "N"},
        {"parametro": "permite_busca_por_socio", "valor": "S" if PERMITE_BUSCA_POR_SOCIO else "N"},
        {"parametro": "permite_expansao_societaria", "valor": "S" if PERMITE_EXPANSAO_SOCIETARIA else "N"},
        {"parametro": "lote_cnpj_base", "valor": str(LOTE_CNPJ_BASE)},
        {"parametro": "lote_cnpj", "valor": str(LOTE_CNPJ)},
        {"parametro": "lote_doc_socio", "valor": str(LOTE_DOC_SOCIO)},
        {"parametro": "limite_empresas_relacionadas_por_socio", "valor": str(LIMITE_EMPRESAS_RELACIONADAS_POR_SOCIO)},
        {"parametro": "total_linhas_empresas_alvo", "valor": str(len(df_empresas_alvo))},
        {"parametro": "total_linhas_vinculos_societarios", "valor": str(len(df_vinculos))},
        {"parametro": "total_linhas_empresas_relacionadas", "valor": str(len(df_empresas_rel))},
        {"parametro": "total_linhas_consultas_busca", "valor": str(len(df_consultas))},
        {"parametro": "versao_cache_consultas", "valor": VERSAO_CACHE_CONSULTAS}
    ])


# =========================================================
# MAIN
# =========================================================

def main():
    inicio = time.time()

 #   engine_sefaz = create_engine(
 #       build_pg_url(pg_config_sefaz),
 #       pool_pre_ping=True,
 #       connect_args={"connect_timeout": 10}
 #   )

    engine_rfb = create_engine(
        build_pg_url(pg_config_rfb),
        pool_pre_ping=True,
        connect_args={"connect_timeout": 15}
    )

 #   if not testar_conexao(engine_sefaz, "Banco SEFAZ"):
 #       print("Não foi possível acessar o banco SEFAZ. Processo encerrado.")
 #       return

    if not testar_conexao(engine_rfb, "Banco RFB"):
        print("Não foi possível acessar o banco RFB. Processo encerrado.")
        raise SystemExit(1)

    df_alvos = obter_cnpj_base_alvo(engine_rfb)
    log(f"cnpj_base alvo: {len(df_alvos):,}")

    cnpj_bases = df_alvos["cnpj_base"].dropna().astype(str).drop_duplicates().tolist()

    df_empresas = obter_empresas(engine_rfb, cnpj_bases)
    log(f"empresas: {len(df_empresas):,}")

    df_estab = obter_estabelecimentos(engine_rfb, cnpj_bases)
    log(f"estabelecimentos: {len(df_estab):,}")

    if USAR_CACHE and cache_exists(P_EMPRESAS_ALVO) and not FORCAR_ATUALIZACAO_EMPRESAS_ALVO:
        log("Usando cache local de empresas_alvo")
        df_empresas_alvo = load_df(P_EMPRESAS_ALVO)
    else:
        log("Montando empresas_alvo localmente")
        df_empresas_alvo = preparar_empresas_alvo(df_alvos, df_empresas, df_estab)
        save_df(df_empresas_alvo, P_EMPRESAS_ALVO)

    log(f"empresas_alvo: {len(df_empresas_alvo):,}")

    cnpjs_monitorados = (
        df_empresas_alvo["cnpj"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    )

    df_socios_base = obter_socios_base(engine_rfb, cnpjs_monitorados)
    log(f"socios_base: {len(df_socios_base):,}")

    if USAR_CACHE and cache_exists(P_VINCULOS) and not FORCAR_ATUALIZACAO_VINCULOS:
        log("Usando cache local de vinculos_societarios")
        df_vinculos = load_df(P_VINCULOS)
    else:
        log("Montando vinculos_societarios localmente")
        df_vinculos = preparar_vinculos_societarios(df_socios_base, df_empresas_alvo)
        save_df(df_vinculos, P_VINCULOS)

    log(f"vinculos_societarios: {len(df_vinculos):,}")

    if PERMITE_EXPANSAO_SOCIETARIA:
        docs_socios = (
            df_vinculos["documento_socio"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )

        df_socios_rel = obter_socios_relacionados(engine_rfb, docs_socios)
        log(f"socios_relacionados: {len(df_socios_rel):,}")

        if USAR_CACHE and cache_exists(P_EMPRESAS_REL) and not FORCAR_ATUALIZACAO_EMPRESAS_REL:
            log("Usando cache local de empresas_relacionadas")
            df_empresas_rel = load_df(P_EMPRESAS_REL)
        else:
            log("Montando empresas_relacionadas localmente")
            df_empresas_rel = preparar_empresas_relacionadas(
                df_vinculos=df_vinculos,
                df_socios_rel=df_socios_rel,
                df_empresas=df_empresas,
                df_estab=df_estab,
                df_empresas_alvo=df_empresas_alvo
            )
            save_df(df_empresas_rel, P_EMPRESAS_REL)
    else:
        df_empresas_rel = pd.DataFrame()

    log(f"empresas_relacionadas: {len(df_empresas_rel):,}")

    usar_cache_consultas = (
        USAR_CACHE and
        not FORCAR_ATUALIZACAO_CONSULTAS and
        consultas_cache_valido(P_CONSULTAS)
    )

    if usar_cache_consultas:
        log("Usando cache local de consultas_busca")
        df_consultas = load_df(P_CONSULTAS)
    else:
        if USAR_CACHE and cache_exists(P_CONSULTAS) and not FORCAR_ATUALIZACAO_CONSULTAS:
            log("Cache de consultas_busca desatualizado para a nova regra; reconstruindo somente esta etapa")
        else:
            log("Montando consultas_busca localmente")
        df_consultas = preparar_consultas_busca(df_empresas_alvo, df_vinculos, df_empresas_rel)
        save_df(df_consultas, P_CONSULTAS)
        write_stage_metadata(P_CONSULTAS, {"versao_cache_consultas": VERSAO_CACHE_CONSULTAS})

    log(f"consultas_busca: {len(df_consultas):,}")

    df_params = preparar_parametros_monitoramento(df_empresas_alvo, df_vinculos, df_empresas_rel, df_consultas)
    save_df(df_params, P_PARAMS)

    for df, cols in [
        (df_empresas_alvo, [
            "razao_social", "nome_fantasia", "situacao_cadastral", "motivo_situacao_cadastral",
            "natureza_juridica", "qualificacao_responsavel", "porte_empresa",
            "ente_federativo_responsavel", "cnae_fiscal", "cnae_fiscal_secundaria",
            "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
            "cep", "uf", "municipio", "correio_eletronico"
        ]),
        (df_vinculos, [
            "razao_social_alvo", "nome_fantasia_alvo", "nome_socio",
            "qualificacao_socio", "nome_representante"
        ]),
        (df_empresas_rel, [
            "razao_social_alvo", "nome_socio", "razao_social_relacionada",
            "nome_fantasia_relacionada", "situacao_cadastral_relacionada",
            "uf_relacionada", "municipio_relacionada"
        ])
    ]:
        if not df.empty:
            for col in cols:
                if col in df.columns:
                    df[col] = df[col].apply(normalize_text)

    
    log("Persistindo tabelas de saída no Postgres")
    ensure_schema_and_tables(engine_rfb)
    persistir_dataframe(engine_rfb, TB_EMPRESAS_ALVO, df_empresas_alvo)
    persistir_dataframe(engine_rfb, TB_VINCULOS, df_vinculos)
    persistir_dataframe(engine_rfb, TB_EMPRESAS_REL, df_empresas_rel)
    persistir_dataframe(engine_rfb, TB_CONSULTAS, df_consultas)
    persistir_dataframe(engine_rfb, TB_PARAMS, df_params)

    metadata = {
        "ano_referencia": ANO_REFERENCIA,
        "origem_monitoramento": ORIGEM_MONITORAMENTO,
        "inicio_execucao": pd.Timestamp.fromtimestamp(inicio).strftime("%Y-%m-%d %H:%M:%S"),
        "total_empresas_alvo": int(len(df_empresas_alvo)),
        "total_vinculos_societarios": int(len(df_vinculos)),
        "total_empresas_relacionadas": int(len(df_empresas_rel)),
        "total_consultas_busca": int(len(df_consultas)),
        "fim_execucao": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    write_metadata(metadata)
    registrar_execucao_fase1(engine_rfb, metadata)


    fim = time.time()
    log(f"Concluído em {(fim - inicio):.2f} segundos")


if __name__ == "__main__":
    main()
