# -*- coding: utf-8 -*-
"""
FASE 2 V3 - Monitoramento OSINT incremental de contribuintes.

Principais propriedades:
  * I/O de rede assíncrono com limites globais/por host, retry e backoff;
  * leitura PostgreSQL por cursor server-side e escrita em lote via COPY + upsert;
  * pesquisa por razão social, fantasia, aliases/marcas, CNPJ e QSA;
  * extração do corpo do artigo e metadados estruturados quando disponíveis;
  * relevância contextual, classificação de risco e evidência auditável;
  * normalização de URL, hash de conteúdo e SimHash para desduplicação;
  * documentos separados das menções empresa-documento, preservando relações N:N;
  * estado incremental por empresa e fonte, com sobreposição configurável.

Dependências: Python 3.11+, httpx, feedparser, beautifulsoup4 e psycopg 3.

O script cria somente tabelas com sufixo ``_v3``. A V2 e suas tabelas não são
alteradas. Senhas não possuem fallback no código: use RFB_PG_PASS, .pgpass ou
outro mecanismo suportado pelo libpq.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from uuid import uuid4

import feedparser
import httpx
import psycopg
from bs4 import BeautifulSoup
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


UTC = timezone.utc
BASE_DIR = Path(__file__).resolve().parent

SCHEMA = "suporte_arrecadacao"
TB_EMPRESAS = "noticias_empresas_alvo"
TB_VINCULOS = "noticias_vinculos_societarios"
TB_ALIASES = "noticias_alias_empresas_v3"
TB_DOCUMENTOS = "noticias_documentos_v3"
TB_MENCOES = "noticias_mencoes_v3"
TB_ESTADO = "noticias_estado_busca_v3"
TB_EXECUCOES = "noticias_execucoes_v3"

SOURCE_QD_CNPJ = "QUERIDO_DIARIO_CNPJ"
SOURCE_QD_NOME = "QUERIDO_DIARIO_NOME"
SOURCE_RSS_LOCAL = "RSS_LOCAL_BA"
SOURCE_RSS_NACIONAL = "RSS_NACIONAL"
SOURCE_GOOGLE = "GOOGLE_NEWS"
SOURCE_GDELT = "GDELT"
SOURCE_WEB = "WEB_OSINT"

TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid", "ref",
    "ref_src", "source", "campaign", "cmpid", "igshid", "srsltid",
}

LEGAL_SUFFIXES = {
    "LTDA", "LIMITADA", "S/A", "S.A", "SA", "EIRELI", "ME", "MEI",
    "EPP", "S/S", "SS", "CIA", "COMPANHIA", "EMPRESARIO INDIVIDUAL",
}

NAME_STOPWORDS = {
    "a", "ao", "aos", "as", "com", "da", "das", "de", "do", "dos",
    "e", "em", "na", "nas", "no", "nos", "o", "os", "para", "pela",
    "pelo", "por",
}

GENERIC_NAME_TOKENS = {
    "bahia", "brasil", "brasileira", "comercio", "comercial", "empresa",
    "grupo", "nacional", "nordeste", "salvador", "servicos", "solucoes",
}

RISK_PATTERNS: dict[str, tuple[str, ...]] = {
    "FRAUDE_COMPLIANCE": (
        r"\bfraude(?:s)?\b", r"\bcorrup(?:cao|ção)\b", r"\bsonega(?:cao|ção)\b",
        r"\blavagem de dinheiro\b", r"\bnota fiscal fria\b", r"\bempresa de fachada\b",
    ),
    "OPERACAO_POLICIAL": (
        r"\bopera(?:cao|ção) policial\b", r"\balvo da opera(?:cao|ção)\b",
        r"\bmandado de busca\b", r"\bpris(?:ao|ão) preventiva\b",
        r"\bpolicia federal (?:deflagra|investiga|prende)\b",
    ),
    "BLOQUEIO_BENS": (
        r"\bbloqueio de bens\b", r"\bindisponibilidade de bens\b", r"\bsequestro de bens\b",
        r"\barresto\b", r"\bpenhora\b",
    ),
    "INSOLVENCIA": (
        r"\bfalencia\b", r"\brecuperacao judicial\b", r"\brecuperacao extrajudicial\b",
        r"\binsolvencia\b", r"\bliquidacao judicial\b",
    ),
    "JUDICIAL": (
        r"\bprocesso judicial\b", r"\bacao civil publica\b", r"\bcondenad[ao]s?\b",
        r"\bdecisao judicial\b", r"\btribunal de justica\b", r"\bliminar\b",
    ),
    "DIVIDA_ATIVA": (
        r"\bdivida ativa\b", r"\bexecucao fiscal\b", r"\bcertidao de divida\b",
        r"\binadimplencia tributaria\b",
    ),
    "AUTUACAO_FISCAL": (
        r"\bautua(?:cao|ção|do|da)\b", r"\binfracao tributaria\b", r"\bmulta fiscal\b",
        r"\bcredito tributario\b", r"\bauto de infracao\b",
    ),
    "LICITACAO_CONTRATO": (
        r"\blicita(?:cao|ção)\b", r"\bpregao\b", r"\bcontrato administrativo\b",
        r"\bdispensa de licitacao\b", r"\binexigibilidade\b", r"\badjudicacao\b",
    ),
    "ALTERACAO_SOCIETARIA": (
        r"\bfusao\b", r"\bincorporacao\b", r"\bcisao\b", r"\baquisicao\b",
        r"\balteracao societaria\b", r"\bmudanca de controle\b", r"\bnovo controlador\b",
    ),
    "REGULATORIO": (
        r"\bsancao administrativa\b", r"\bprocesso administrativo sancionador\b",
        r"\bmulta regulatoria\b", r"\bmedida cautelar\b", r"\bsuspensao de registro\b",
        r"\binabilitad[ao]s?\b", r"\binidone[ao]s?\b",
    ),
    "AMBIENTAL_TRABALHISTA": (
        r"\bembargo ambiental\b", r"\bcrime ambiental\b", r"\btrabalho escravo\b",
        r"\bministerio publico do trabalho\b", r"\bacao trabalhista\b",
    ),
}

RISK_WEIGHTS = {
    "FRAUDE_COMPLIANCE": 30,
    "OPERACAO_POLICIAL": 30,
    "BLOQUEIO_BENS": 28,
    "INSOLVENCIA": 28,
    "DIVIDA_ATIVA": 24,
    "AUTUACAO_FISCAL": 22,
    "JUDICIAL": 18,
    "REGULATORIO": 18,
    "AMBIENTAL_TRABALHISTA": 16,
    "LICITACAO_CONTRATO": 12,
    "ALTERACAO_SOCIETARIA": 12,
}

RSS_LOCAL_DEFAULT = (
    "https://www.correio24horas.com.br/rss/bahia.xml",
    "https://atarde.com.br/feed",
    "https://www.bahianoticias.com.br/rss.xml",
    "https://www.acordacidade.com.br/feed",
    "https://bahia.ba.gov.br/rss.xml",
)

RSS_NATIONAL_DEFAULT = (
    "https://g1.globo.com/rss/g1/",
    "https://www.cnnbrasil.com.br/rss/",
    "https://exame.com/feed/",
    "https://www.infomoney.com.br/feed/",
)

GOOGLE_NEWS_HOSTS = {"news.google.com", "google.com", "www.google.com"}

PUBLIC_AUTHORITY_MARKERS = (
    "banco central do brasil", "banco central", "comissao de valores mobiliarios",
    "cvm", "conselho administrativo de defesa economica", "cade",
    "receita federal", "secretaria da fazenda", "sefaz", "ministerio publico",
    "tribunal de justica", "tribunal de contas", "prefeitura", "governo do estado",
    "agencia nacional", "aneel", "anatel", "anvisa",
)

ADVERSE_TARGET_VERBS = (
    "investiga", "investigou", "apura", "apurou", "denuncia", "denunciou",
    "condena", "condenou", "autua", "autuou", "multa", "multou", "prende",
    "prendeu", "bloqueia", "bloqueou", "processa", "processou", "detecta",
    "detectou", "acusa", "acusou",
)

AUTHORITY_ACTION_VERBS = (
    "investiga", "apura", "regulamenta", "fiscaliza", "determina", "divulga",
    "informa", "alerta", "combate", "autoriza", "proibe", "estabelece",
    "publica", "anuncia", "endurece", "reduz", "eleva", "mantem", "decide",
    "investigar", "apurar", "regulamentar", "fiscalizar", "determinar", "divulgar",
    "informar", "alertar", "combater", "autorizar", "proibir", "estabelecer",
    "publicar", "anunciar", "endurecer", "reduzir", "elevar", "manter", "decidir",
)


@dataclass(slots=True)
class Settings:
    year: int
    origin: str
    output_dir: Path
    pg_dbname: str = "db_rfb"
    pg_user: str = "cif_user"
    pg_password: str | None = None
    pg_host: str = "valedosrios"
    pg_port: int = 5432
    target_batch_size: int = 200
    entity_concurrency: int = 8
    http_concurrency: int = 20
    per_host_concurrency: int = 2
    per_host_interval_seconds: float = 0.35
    gdelt_interval_seconds: float = 6.0
    gdelt_cooldown_seconds: float = 900.0
    gdelt_retries: int = 2
    gdelt_batch_size: int = 15
    request_timeout_seconds: float = 25.0
    http_retries: int = 4
    initial_lookback_days: int = 30
    official_lookback_days: int = 90
    overlap_days: int = 3
    max_results_per_entity: int = 30
    max_results_per_source: int = 12
    max_queries_per_entity: int = 12
    max_article_fetch_per_entity: int = 25
    max_article_bytes: int = 2_500_000
    minimum_relevance_score: int = 38
    dedup_lookback_days: int = 180
    dedup_recent_limit: int = 100_000
    simhash_max_distance: int = 3
    fetch_articles: bool = True
    full_refresh: bool = False
    dry_run: bool = False
    searxng_url: str | None = None
    qd_base_url: str = "https://api.queridodiario.org.br/gazettes"
    qd_cooldown_seconds: float = 300.0
    territory_salvador: str = "2927408"
    user_agent: str = "SEFAZ-Salvador-OSINT/3.0 (+monitoramento institucional)"

    @property
    def pg_kwargs(self) -> dict[str, Any]:
        values: dict[str, Any] = {
            "dbname": self.pg_dbname,
            "user": self.pg_user,
            "host": self.pg_host,
            "port": self.pg_port,
            "connect_timeout": 15,
            "application_name": "scrapper_noticias_v3",
        }
        if self.pg_password:
            values["password"] = self.pg_password
        return values


@dataclass(slots=True, frozen=True)
class QuerySpec:
    text: str
    kind: str
    match_text: str
    priority: int


@dataclass(slots=True)
class Entity:
    entity_id: str
    cnpj_base: str
    cnpj: str
    legal_name: str
    trade_name: str
    city: str
    state: str
    partners: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    state_by_source: dict[str, datetime] = field(default_factory=dict)


@dataclass(slots=True)
class Candidate:
    source: str
    title: str
    url: str
    published_at: datetime | None
    summary: str = ""
    site: str = ""
    query_text: str = ""
    query_kind: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Article:
    final_url: str
    canonical_url: str
    body: str
    published_at: datetime | None = None
    modified_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extraction_quality: int = 0


@dataclass(slots=True)
class Assessment:
    relevant: bool
    score: int
    risk_score: int
    risk_level: str
    entity_role: str
    categories: list[str]
    triggers: list[str]
    evidence: str
    reasons: dict[str, Any]
    event_date: date | None


@dataclass(slots=True)
class DocumentRecord:
    document_id: str
    original_url: str
    canonical_url: str
    url_hash: str
    source: str
    observed_sources: list[str]
    site: str
    title: str
    normalized_title: str
    summary: str
    body: str
    published_at: datetime | None
    modified_at: datetime | None
    event_date: date | None
    first_seen_at: datetime
    last_seen_at: datetime
    content_hash: str
    simhash_hex: str
    extraction_quality: int
    metadata: dict[str, Any]
    aggregator_url: str = ""


@dataclass(slots=True)
class MentionRecord:
    document_id: str
    entity_id: str
    cnpj_base: str
    cnpj: str
    legal_name: str
    trade_name: str
    first_seen_at: datetime
    last_seen_at: datetime
    query_kind: str
    query_text: str
    score: int
    risk_score: int
    risk_level: str
    entity_role: str
    risk_categories: list[str]
    triggers: list[str]
    evidence: str
    matched_terms: list[str]
    score_reasons: dict[str, Any]


@dataclass(slots=True)
class SourceOutcome:
    source: str
    candidates: list[Candidate]
    success: bool
    error: str = ""
    degraded: bool = False


@dataclass(slots=True)
class EntityOutcome:
    documents: list[DocumentRecord]
    mentions: list[MentionRecord]
    source_outcomes: list[SourceOutcome]


@dataclass(slots=True)
class StateRecord:
    entity_id: str
    source: str
    attempted_at: datetime
    success: bool
    error: str = ""
    degraded: bool = False


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_space(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(ch)
    )


def normalize_for_match(value: Any) -> str:
    value = strip_accents(normalize_space(value).lower())
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_space(value)


def significant_tokens(value: str) -> set[str]:
    return set(significant_token_list(value))


def significant_token_list(value: str) -> list[str]:
    return [
        token for token in normalize_for_match(value).split()
        if len(token) >= 3 and token not in NAME_STOPWORDS
    ]


def ordered_terms_within(text_value: str, term_value: str, max_span: int = 160) -> bool:
    """Confirma os termos significativos, na ordem, dentro de uma janela curta."""
    tokens = significant_token_list(term_value)
    if len(tokens) < 2:
        return False
    words = [
        (match.group(0), match.start(), match.end())
        for match in re.finditer(r"[a-z0-9]+", normalize_for_match(text_value))
    ]
    for start_index, (word, start, _end) in enumerate(words):
        if word != tokens[0]:
            continue
        token_index = 1
        last_end = start
        for next_word, _next_start, next_end in words[start_index + 1:]:
            if next_end - start > max_span:
                break
            if next_word == tokens[token_index]:
                token_index += 1
                last_end = next_end
                if token_index == len(tokens):
                    return last_end - start <= max_span
    return False


def escape_simple_query_phrase(value: str) -> str:
    """Gera frase exata segura para o simple_query_string do OpenSearch."""
    clean = normalize_space(value).replace("\\", "\\\\").replace('"', '\\"')
    clean = re.sub(r"([+|()~*\-])", r"\\\1", clean)
    return f'"{clean}"'


def only_digits(value: Any) -> str:
    return re.sub(r"\D", "", normalize_space(value))


def format_cnpj(value: str) -> str:
    digits = only_digits(value)
    if len(digits) != 14:
        return normalize_space(value)
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"


def remove_legal_suffix(value: str) -> str:
    result = normalize_space(value)
    result = re.sub(r"\bS\s*[./-]?\s*A\.?\b", " ", result, flags=re.IGNORECASE)
    parts = result.split()
    while parts and parts[-1].upper().rstrip(".,-/") in LEGAL_SUFFIXES:
        parts.pop()
    return normalize_space(" ".join(parts))


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    text_value = normalize_space(value)
    if not text_value:
        return None
    candidates = (text_value, text_value.replace("Z", "+00:00"))
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
        except ValueError:
            pass
    try:
        parsed = parsedate_to_datetime(text_value)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        return None


def site_from_url(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


def is_google_news_url(url: str) -> bool:
    host = site_from_url(url)
    return host in GOOGLE_NEWS_HOSTS or host.endswith(".google.com")


def registrable_host(host: str) -> str:
    """Normaliza host para comparação prática, sem depender de uma PSL externa."""
    value = normalize_space(host).lower().split(":", 1)[0].removeprefix("www.")
    return value


def parse_retry_after(value: str | None) -> float:
    if not value:
        return 0.0
    clean = normalize_space(value)
    try:
        return max(0.0, float(clean))
    except ValueError:
        parsed = parse_datetime(clean)
        return max(0.0, (parsed - utc_now()).total_seconds()) if parsed else 0.0


def normalize_url(url: str, base_url: str = "") -> str:
    value = normalize_space(url)
    if not value:
        return ""
    if base_url:
        value = urljoin(base_url, value)
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError:
        return value
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        return value
    scheme = parts.scheme.lower()
    host = parts.hostname.lower()
    netloc = host
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if path != "/":
        path = path.rstrip("/")
    query_items = []
    for key, value_item in parse_qsl(parts.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower.startswith("utm_") or key_lower in TRACKING_QUERY_KEYS:
            continue
        query_items.append((key, value_item))
    return urlunsplit((scheme, netloc, path, urlencode(sorted(query_items)), ""))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def tokenize_for_simhash(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]{3,}", normalize_for_match(value))


def simhash64(value: str) -> int:
    vector = [0] * 64
    tokens = tokenize_for_simhash(value)
    if not tokens:
        return 0
    for token in tokens:
        digest = int.from_bytes(hashlib.blake2b(token.encode(), digest_size=8).digest(), "big")
        for bit in range(64):
            vector[bit] += 1 if digest & (1 << bit) else -1
    fingerprint = 0
    for bit, weight in enumerate(vector):
        if weight >= 0:
            fingerprint |= 1 << bit
    return fingerprint


def simhash_hex(value: int) -> str:
    return f"{value & ((1 << 64) - 1):016x}"


def hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def token_jaccard(left: str, right: str) -> float:
    a, b = significant_tokens(left), significant_tokens(right)
    return len(a & b) / len(a | b) if a and b else 0.0


def truncate(value: str, maximum: int) -> str:
    value = normalize_space(value)
    return value if len(value) <= maximum else value[: maximum - 1].rstrip() + "…"


def compile_risk_patterns() -> dict[str, tuple[re.Pattern[str], ...]]:
    return {
        category: tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
        for category, patterns in RISK_PATTERNS.items()
    }


COMPILED_RISK_PATTERNS = compile_risk_patterns()


DDL_V3 = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TB_ALIASES} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    id_empresa_alvo text NOT NULL,
    alias text NOT NULL,
    tipo_alias text NOT NULL DEFAULT 'MARCA',
    ativo boolean NOT NULL DEFAULT true,
    criado_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ano_referencia, origem_monitoramento, id_empresa_alvo, alias)
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TB_DOCUMENTOS} (
    id_documento text PRIMARY KEY,
    url_original text,
    url_agregador text,
    url_canonica text,
    url_hash text,
    fonte_busca text NOT NULL,
    fontes_observadas text[] NOT NULL DEFAULT ARRAY[]::text[],
    site text,
    titulo text NOT NULL,
    titulo_normalizado text,
    resumo text,
    corpo_texto text,
    data_publicacao timestamptz,
    data_modificacao timestamptz,
    data_evento date,
    primeira_captura timestamptz NOT NULL,
    ultima_captura timestamptz NOT NULL,
    hash_conteudo text NOT NULL,
    simhash_hex text,
    qualidade_extracao smallint NOT NULL DEFAULT 0,
    metadados jsonb NOT NULL DEFAULT '{{}}'::jsonb
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TB_MENCOES} (
    id_documento text NOT NULL REFERENCES {SCHEMA}.{TB_DOCUMENTOS}(id_documento),
    id_empresa_alvo text NOT NULL,
    cnpj_base text,
    cnpj text,
    razao_social text,
    nome_fantasia text,
    primeira_captura timestamptz NOT NULL,
    ultima_captura timestamptz NOT NULL,
    tipo_query text,
    query_texto text,
    score_relevancia integer NOT NULL,
    score_risco integer NOT NULL DEFAULT 0,
    nivel_risco text NOT NULL,
    papel_entidade text NOT NULL DEFAULT 'CITADA',
    categorias_risco text[] NOT NULL DEFAULT ARRAY[]::text[],
    gatilhos text[] NOT NULL DEFAULT ARRAY[]::text[],
    trecho_evidencia text,
    termos_match text[] NOT NULL DEFAULT ARRAY[]::text[],
    motivos_score jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    status_triagem text NOT NULL DEFAULT 'NOVO',
    relevante boolean,
    revisado_em timestamptz,
    revisado_por text,
    observacao_analista text,
    PRIMARY KEY (id_documento, id_empresa_alvo)
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TB_ESTADO} (
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    id_empresa_alvo text NOT NULL,
    fonte_busca text NOT NULL,
    ultima_execucao_sucesso timestamptz,
    ultima_tentativa timestamptz,
    status_ultima_tentativa text,
    ultimo_erro text,
    atualizado_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ano_referencia, origem_monitoramento, id_empresa_alvo, fonte_busca)
);

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TB_EXECUCOES} (
    id_execucao text PRIMARY KEY,
    ano_referencia integer NOT NULL,
    origem_monitoramento text NOT NULL,
    inicio_execucao timestamptz NOT NULL,
    fim_execucao timestamptz,
    status_execucao text NOT NULL,
    empresas_processadas integer NOT NULL DEFAULT 0,
    documentos_coletados integer NOT NULL DEFAULT 0,
    mencoes_relevantes integer NOT NULL DEFAULT 0,
    fontes_com_erro integer NOT NULL DEFAULT 0,
    metricas jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    observacao text
);

ALTER TABLE {SCHEMA}.{TB_DOCUMENTOS}
    ADD COLUMN IF NOT EXISTS fontes_observadas text[] NOT NULL DEFAULT ARRAY[]::text[];

ALTER TABLE {SCHEMA}.{TB_DOCUMENTOS}
    ADD COLUMN IF NOT EXISTS url_agregador text;

ALTER TABLE {SCHEMA}.{TB_MENCOES}
    ADD COLUMN IF NOT EXISTS score_risco integer NOT NULL DEFAULT 0;

ALTER TABLE {SCHEMA}.{TB_MENCOES}
    ADD COLUMN IF NOT EXISTS papel_entidade text NOT NULL DEFAULT 'CITADA';

UPDATE {SCHEMA}.{TB_MENCOES}
SET motivos_score = motivos_score || jsonb_build_object(
        'classificacao_legada', true,
        'nivel_risco_legado', nivel_risco,
        'categorias_risco_legadas', to_jsonb(categorias_risco),
        'score_risco', 0,
        'versao_classificacao', 'v3-contextual-1'
    ),
    score_risco = 0,
    nivel_risco = 'PENDENTE_RECLASSIFICACAO',
    papel_entidade = 'NAO_AVALIADO',
    categorias_risco = ARRAY[]::text[],
    gatilhos = ARRAY[]::text[],
    status_triagem = 'REPROCESSAR'
WHERE status_triagem = 'NOVO'
  AND NOT (motivos_score ? 'score_risco');

CREATE INDEX IF NOT EXISTS ix_{TB_DOCUMENTOS}_publicacao
    ON {SCHEMA}.{TB_DOCUMENTOS}(data_publicacao DESC);
CREATE INDEX IF NOT EXISTS ix_{TB_DOCUMENTOS}_url_hash
    ON {SCHEMA}.{TB_DOCUMENTOS}(url_hash);
CREATE INDEX IF NOT EXISTS ix_{TB_DOCUMENTOS}_hash_conteudo
    ON {SCHEMA}.{TB_DOCUMENTOS}(hash_conteudo);
CREATE INDEX IF NOT EXISTS ix_{TB_DOCUMENTOS}_simhash
    ON {SCHEMA}.{TB_DOCUMENTOS}(simhash_hex);
CREATE INDEX IF NOT EXISTS ix_{TB_MENCOES}_empresa_data
    ON {SCHEMA}.{TB_MENCOES}(id_empresa_alvo, ultima_captura DESC);
CREATE INDEX IF NOT EXISTS ix_{TB_MENCOES}_risco
    ON {SCHEMA}.{TB_MENCOES}(nivel_risco, score_relevancia DESC);
CREATE INDEX IF NOT EXISTS ix_{TB_MENCOES}_score_risco
    ON {SCHEMA}.{TB_MENCOES}(score_risco DESC, nivel_risco);
CREATE INDEX IF NOT EXISTS ix_{TB_MENCOES}_categorias_gin
    ON {SCHEMA}.{TB_MENCOES} USING gin(categorias_risco);
"""


class Database:
    """Acesso PostgreSQL com streaming de entrada e COPY para escrita em lote."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self) -> psycopg.Connection[Any]:
        return psycopg.connect(**self.settings.pg_kwargs, row_factory=dict_row)

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                for statement in (part.strip() for part in DDL_V3.split(";")):
                    if statement:
                        cur.execute(statement)

    def iter_entity_batches(self, limit: int | None = None) -> Iterator[list[Entity]]:
        limit_sql = sql.SQL(" LIMIT %s") if limit else sql.SQL("")
        query = sql.SQL(
            """
            SELECT
                e.id_empresa_alvo,
                COALESCE(e.cnpj_base, '') AS cnpj_base,
                COALESCE(e.cnpj, '') AS cnpj,
                COALESCE(e.razao_social, '') AS razao_social,
                COALESCE(e.nome_fantasia, '') AS nome_fantasia,
                COALESCE(e.municipio, '') AS municipio,
                COALESCE(e.uf, '') AS uf,
                COALESCE(v.socios, ARRAY[]::text[]) AS socios,
                COALESCE(a.aliases, ARRAY[]::text[]) AS aliases,
                COALESCE(s.estado, '{{}}'::jsonb) AS estado
            FROM (
                SELECT DISTINCT ON (
                    COALESCE(NULLIF(e0.cnpj_base, ''), e0.id_empresa_alvo)
                ) e0.*
                FROM {}.{} e0
                WHERE e0.ano_referencia = %s
                  AND e0.origem_monitoramento = %s
                  AND COALESCE(e0.monitorar, 'S') = 'S'
                ORDER BY
                    COALESCE(NULLIF(e0.cnpj_base, ''), e0.id_empresa_alvo),
                    CASE WHEN upper(COALESCE(e0.matriz_filial, '')) IN ('1', 'MATRIZ')
                         THEN 0 ELSE 1 END,
                    e0.id_empresa_alvo
            ) e
            LEFT JOIN LATERAL (
                SELECT array_agg(x.nome_socio ORDER BY x.prioridade, x.nome_socio) AS socios
                FROM (
                    SELECT v.nome_socio,
                           min(CASE
                               WHEN lower(COALESCE(v.qualificacao_socio, '')) LIKE '%%administr%%'
                                 OR lower(COALESCE(v.qualificacao_socio, '')) LIKE '%%diretor%%'
                                 OR lower(COALESCE(v.qualificacao_socio, '')) LIKE '%%presidente%%'
                               THEN 0 ELSE 1
                           END) AS prioridade
                    FROM {}.{} v
                    WHERE v.ano_referencia = e.ano_referencia
                      AND v.origem_monitoramento = e.origem_monitoramento
                      AND v.id_empresa_alvo = e.id_empresa_alvo
                      AND NULLIF(btrim(v.nome_socio), '') IS NOT NULL
                    GROUP BY v.nome_socio
                ) x
            ) v ON true
            LEFT JOIN LATERAL (
                SELECT array_agg(DISTINCT a.alias)
                    FILTER (WHERE a.ativo AND NULLIF(btrim(a.alias), '') IS NOT NULL) AS aliases
                FROM {}.{} a
                WHERE a.ano_referencia = e.ano_referencia
                  AND a.origem_monitoramento = e.origem_monitoramento
                  AND a.id_empresa_alvo = e.id_empresa_alvo
            ) a ON true
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(st.fonte_busca, st.ultima_execucao_sucesso)
                    FILTER (WHERE st.ultima_execucao_sucesso IS NOT NULL) AS estado
                FROM {}.{} st
                WHERE st.ano_referencia = e.ano_referencia
                  AND st.origem_monitoramento = e.origem_monitoramento
                  AND st.id_empresa_alvo = e.id_empresa_alvo
            ) s ON true
            ORDER BY e.id_empresa_alvo
            """
        ).format(
            sql.Identifier(SCHEMA), sql.Identifier(TB_EMPRESAS),
            sql.Identifier(SCHEMA), sql.Identifier(TB_VINCULOS),
            sql.Identifier(SCHEMA), sql.Identifier(TB_ALIASES),
            sql.Identifier(SCHEMA), sql.Identifier(TB_ESTADO),
        ) + limit_sql

        params: list[Any] = [self.settings.year, self.settings.origin]
        if limit:
            params.append(limit)

        with self.connect() as conn:
            cursor_name = f"alvos_v3_{int(time.time())}"
            with conn.cursor(name=cursor_name, withhold=True) as cur:
                cur.execute(query, params)
                conn.commit()
                while True:
                    rows = cur.fetchmany(self.settings.target_batch_size)
                    if not rows:
                        break
                    batch: list[Entity] = []
                    for row in rows:
                        state_raw = row.get("estado") or {}
                        state = {
                            key: parsed
                            for key, value in state_raw.items()
                            if (parsed := parse_datetime(value)) is not None
                        }
                        batch.append(Entity(
                            entity_id=normalize_space(row["id_empresa_alvo"]),
                            cnpj_base=only_digits(row["cnpj_base"]),
                            cnpj=only_digits(row["cnpj"]),
                            legal_name=normalize_space(row["razao_social"]),
                            trade_name=normalize_space(row["nome_fantasia"]),
                            city=normalize_space(row["municipio"]),
                            state=normalize_space(row["uf"]),
                            partners=[normalize_space(x) for x in row["socios"] if normalize_space(x)],
                            aliases=[normalize_space(x) for x in row["aliases"] if normalize_space(x)],
                            state_by_source=state,
                        ))
                    yield batch

    def load_recent_fingerprints(self) -> list[dict[str, Any]]:
        cutoff = utc_now() - timedelta(days=self.settings.dedup_lookback_days)
        query = sql.SQL(
            """
            SELECT id_documento, url_hash, hash_conteudo, simhash_hex, titulo_normalizado
            FROM {}.{}
            WHERE ultima_captura >= %s
            ORDER BY ultima_captura DESC
            LIMIT %s
            """
        ).format(sql.Identifier(SCHEMA), sql.Identifier(TB_DOCUMENTOS))
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(query, (cutoff, self.settings.dedup_recent_limit))
            return list(cur.fetchall())

    def upsert_batch(
        self,
        documents: Sequence[DocumentRecord],
        mentions: Sequence[MentionRecord],
        states: Sequence[StateRecord] = (),
    ) -> None:
        if not documents and not mentions and not states:
            return
        documents_by_id = {item.document_id: item for item in documents}
        mentions_by_key = {(item.document_id, item.entity_id): item for item in mentions}
        with self.connect() as conn, conn.cursor() as cur:
            if documents_by_id:
                self._copy_documents(cur, list(documents_by_id.values()))
            if mentions_by_key:
                self._copy_mentions(cur, list(mentions_by_key.values()))
            if states:
                self._upsert_state_records(cur, states)

    def _copy_documents(self, cur: psycopg.Cursor[Any], rows: Sequence[DocumentRecord]) -> None:
        cur.execute(sql.SQL(
            "CREATE TEMP TABLE tmp_documentos_v3 "
            "(LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(sql.Identifier(SCHEMA), sql.Identifier(TB_DOCUMENTOS)))
        columns = (
            "id_documento", "url_original", "url_agregador", "url_canonica", "url_hash", "fonte_busca",
            "fontes_observadas", "site", "titulo", "titulo_normalizado", "resumo", "corpo_texto",
            "data_publicacao", "data_modificacao", "data_evento", "primeira_captura",
            "ultima_captura", "hash_conteudo", "simhash_hex", "qualidade_extracao",
            "metadados",
        )
        copy_sql = sql.SQL("COPY tmp_documentos_v3 ({}) FROM STDIN").format(
            sql.SQL(", ").join(map(sql.Identifier, columns))
        )
        with cur.copy(copy_sql) as copy:
            for item in rows:
                copy.write_row((
                    item.document_id, item.original_url, item.aggregator_url,
                    item.canonical_url, item.url_hash,
                    item.source, item.observed_sources, item.site, item.title, item.normalized_title, item.summary,
                    item.body, item.published_at, item.modified_at, item.event_date,
                    item.first_seen_at, item.last_seen_at, item.content_hash,
                    item.simhash_hex, item.extraction_quality, Jsonb(item.metadata),
                ))
        target = sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(TB_DOCUMENTOS))
        cur.execute(sql.SQL(
            """
            INSERT INTO {} ({columns})
            SELECT {columns} FROM tmp_documentos_v3
            ON CONFLICT (id_documento) DO UPDATE SET
                ultima_captura = GREATEST({table}.ultima_captura, EXCLUDED.ultima_captura),
                fontes_observadas = ARRAY(
                    SELECT DISTINCT x FROM unnest({table}.fontes_observadas || EXCLUDED.fontes_observadas) x
                ),
                url_canonica = COALESCE(NULLIF(EXCLUDED.url_canonica, ''), {table}.url_canonica),
                url_agregador = COALESCE(NULLIF(EXCLUDED.url_agregador, ''), {table}.url_agregador),
                resumo = CASE WHEN length(EXCLUDED.resumo) > length(COALESCE({table}.resumo, ''))
                              THEN EXCLUDED.resumo ELSE {table}.resumo END,
                corpo_texto = CASE WHEN length(EXCLUDED.corpo_texto) > length(COALESCE({table}.corpo_texto, ''))
                                   THEN EXCLUDED.corpo_texto ELSE {table}.corpo_texto END,
                data_publicacao = COALESCE({table}.data_publicacao, EXCLUDED.data_publicacao),
                data_modificacao = COALESCE(EXCLUDED.data_modificacao, {table}.data_modificacao),
                data_evento = COALESCE({table}.data_evento, EXCLUDED.data_evento),
                qualidade_extracao = GREATEST({table}.qualidade_extracao, EXCLUDED.qualidade_extracao),
                metadados = {table}.metadados || EXCLUDED.metadados
            """
        ).format(
            target,
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            table=sql.Identifier(TB_DOCUMENTOS),
        ))

    def _copy_mentions(self, cur: psycopg.Cursor[Any], rows: Sequence[MentionRecord]) -> None:
        cur.execute(sql.SQL(
            "CREATE TEMP TABLE tmp_mencoes_v3 "
            "(LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(sql.Identifier(SCHEMA), sql.Identifier(TB_MENCOES)))
        columns = (
            "id_documento", "id_empresa_alvo", "cnpj_base", "cnpj", "razao_social",
            "nome_fantasia", "primeira_captura", "ultima_captura", "tipo_query",
            "query_texto", "score_relevancia", "score_risco", "nivel_risco",
            "papel_entidade", "categorias_risco", "gatilhos", "trecho_evidencia",
            "termos_match", "motivos_score",
        )
        copy_sql = sql.SQL("COPY tmp_mencoes_v3 ({}) FROM STDIN").format(
            sql.SQL(", ").join(map(sql.Identifier, columns))
        )
        with cur.copy(copy_sql) as copy:
            for item in rows:
                copy.write_row((
                    item.document_id, item.entity_id, item.cnpj_base, item.cnpj,
                    item.legal_name, item.trade_name, item.first_seen_at, item.last_seen_at,
                    item.query_kind, item.query_text, item.score, item.risk_score,
                    item.risk_level, item.entity_role, item.risk_categories, item.triggers,
                    item.evidence, item.matched_terms, Jsonb(item.score_reasons),
                ))
        target = sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(TB_MENCOES))
        cur.execute(sql.SQL(
            """
            INSERT INTO {} ({columns})
            SELECT {columns} FROM tmp_mencoes_v3
            ON CONFLICT (id_documento, id_empresa_alvo) DO UPDATE SET
                ultima_captura = GREATEST({table}.ultima_captura, EXCLUDED.ultima_captura),
                score_relevancia = GREATEST({table}.score_relevancia, EXCLUDED.score_relevancia),
                score_risco = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.score_risco ELSE {table}.score_risco END,
                nivel_risco = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.nivel_risco ELSE {table}.nivel_risco END,
                papel_entidade = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.papel_entidade ELSE {table}.papel_entidade END,
                categorias_risco = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.categorias_risco ELSE {table}.categorias_risco END,
                gatilhos = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.gatilhos ELSE {table}.gatilhos END,
                trecho_evidencia = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.trecho_evidencia ELSE {table}.trecho_evidencia END,
                termos_match = ARRAY(
                    SELECT DISTINCT x FROM unnest({table}.termos_match || EXCLUDED.termos_match) x
                ),
                motivos_score = CASE
                    WHEN EXCLUDED.ultima_captura >= {table}.ultima_captura
                    THEN EXCLUDED.motivos_score ELSE {table}.motivos_score END
            """
        ).format(
            target,
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            table=sql.Identifier(TB_MENCOES),
        ))

    def _upsert_state_records(
        self, cur: psycopg.Cursor[Any], states: Sequence[StateRecord]
    ) -> None:
        query = sql.SQL(
            """
            INSERT INTO {}.{} (
                ano_referencia, origem_monitoramento, id_empresa_alvo, fonte_busca,
                ultima_execucao_sucesso, ultima_tentativa, status_ultima_tentativa,
                ultimo_erro, atualizado_em
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ano_referencia, origem_monitoramento, id_empresa_alvo, fonte_busca)
            DO UPDATE SET
                ultima_execucao_sucesso = COALESCE(
                    EXCLUDED.ultima_execucao_sucesso, {}.ultima_execucao_sucesso
                ),
                ultima_tentativa = EXCLUDED.ultima_tentativa,
                status_ultima_tentativa = EXCLUDED.status_ultima_tentativa,
                ultimo_erro = EXCLUDED.ultimo_erro,
                atualizado_em = EXCLUDED.atualizado_em
            """
        ).format(
            sql.Identifier(SCHEMA), sql.Identifier(TB_ESTADO), sql.Identifier(TB_ESTADO)
        )
        values = [
            (
                self.settings.year, self.settings.origin, state.entity_id, state.source,
                state.attempted_at if state.success else None, state.attempted_at,
                (
                    "SUCESSO_COM_ALERTA" if state.success and state.degraded
                    else "SUCESSO" if state.success else "ERRO"
                ),
                truncate(state.error, 2000),
                state.attempted_at,
            )
            for state in states
        ]
        if not values:
            return
        cur.executemany(query, values)

    def write_execution(
        self,
        execution_id: str,
        started_at: datetime,
        finished_at: datetime | None,
        status: str,
        metrics: dict[str, Any],
        observation: str = "",
    ) -> None:
        query = sql.SQL(
            """
            INSERT INTO {}.{} (
                id_execucao, ano_referencia, origem_monitoramento, inicio_execucao,
                fim_execucao, status_execucao, empresas_processadas,
                documentos_coletados, mencoes_relevantes, fontes_com_erro,
                metricas, observacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_execucao) DO UPDATE SET
                fim_execucao = EXCLUDED.fim_execucao,
                status_execucao = EXCLUDED.status_execucao,
                empresas_processadas = EXCLUDED.empresas_processadas,
                documentos_coletados = EXCLUDED.documentos_coletados,
                mencoes_relevantes = EXCLUDED.mencoes_relevantes,
                fontes_com_erro = EXCLUDED.fontes_com_erro,
                metricas = EXCLUDED.metricas,
                observacao = EXCLUDED.observacao
            """
        ).format(sql.Identifier(SCHEMA), sql.Identifier(TB_EXECUCOES))
        values = (
            execution_id, self.settings.year, self.settings.origin, started_at,
            finished_at, status, metrics.get("empresas_processadas", 0),
            metrics.get("documentos_coletados", 0), metrics.get("mencoes_relevantes", 0),
            metrics.get("fontes_com_erro", 0), Jsonb(metrics), observation,
        )
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(query, values)


class CsvSink:
    """CSV incremental: não acumula toda a execução na memória."""

    FIELDS = (
        "id_documento", "id_empresa_alvo", "cnpj_base", "cnpj", "razao_social",
        "nome_fantasia", "fonte_busca", "site", "titulo", "url_canonica",
        "url_original", "url_agregador", "qualidade_extracao",
        "data_publicacao", "data_evento", "score_relevancia", "score_risco",
        "nivel_risco", "papel_entidade", "categorias_risco", "gatilhos",
        "trecho_evidencia", "tipo_query", "query_texto",
    )

    def __init__(self, path: Path):
        import csv

        path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = path.open("w", newline="", encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.handle, fieldnames=self.FIELDS, extrasaction="ignore")
        self.writer.writeheader()

    def write(self, documents: Sequence[DocumentRecord], mentions: Sequence[MentionRecord]) -> None:
        by_id = {item.document_id: item for item in documents}
        for mention in mentions:
            document = by_id.get(mention.document_id)
            if not document:
                continue
            self.writer.writerow({
                "id_documento": mention.document_id,
                "id_empresa_alvo": mention.entity_id,
                "cnpj_base": mention.cnpj_base,
                "cnpj": mention.cnpj,
                "razao_social": mention.legal_name,
                "nome_fantasia": mention.trade_name,
                "fonte_busca": document.source,
                "site": document.site,
                "titulo": document.title,
                "url_canonica": document.canonical_url,
                "url_original": document.original_url,
                "url_agregador": document.aggregator_url,
                "qualidade_extracao": document.extraction_quality,
                "data_publicacao": document.published_at.isoformat() if document.published_at else "",
                "data_evento": document.event_date.isoformat() if document.event_date else "",
                "score_relevancia": mention.score,
                "score_risco": mention.risk_score,
                "nivel_risco": mention.risk_level,
                "papel_entidade": mention.entity_role,
                "categorias_risco": " | ".join(mention.risk_categories),
                "gatilhos": " | ".join(mention.triggers),
                "trecho_evidencia": mention.evidence,
                "tipo_query": mention.query_kind,
                "query_texto": mention.query_text,
            })
        self.handle.flush()

    def close(self) -> None:
        self.handle.close()


class HostRateLimiter:
    def __init__(self, per_host_concurrency: int, minimum_interval: float):
        self.per_host_concurrency = per_host_concurrency
        self.minimum_interval = minimum_interval
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._next_allowed: dict[str, float] = defaultdict(float)

    def _semaphore(self, host: str) -> asyncio.Semaphore:
        if host not in self._semaphores:
            self._semaphores[host] = asyncio.Semaphore(self.per_host_concurrency)
        return self._semaphores[host]

    def _lock(self, host: str) -> asyncio.Lock:
        if host not in self._locks:
            self._locks[host] = asyncio.Lock()
        return self._locks[host]

    async def enter(self, host: str) -> asyncio.Semaphore:
        semaphore = self._semaphore(host)
        await semaphore.acquire()
        async with self._lock(host):
            now = asyncio.get_running_loop().time()
            wait_seconds = max(0.0, self._next_allowed[host] - now)
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
            self._next_allowed[host] = (
                asyncio.get_running_loop().time() + self.minimum_interval
            )
        return semaphore


class AsyncHttp:
    RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}

    def __init__(self, settings: Settings):
        self.settings = settings
        self.global_semaphore = asyncio.Semaphore(settings.http_concurrency)
        self.host_limiter = HostRateLimiter(
            settings.per_host_concurrency, settings.per_host_interval_seconds
        )
        limits = httpx.Limits(
            max_connections=settings.http_concurrency,
            max_keepalive_connections=max(4, settings.http_concurrency // 2),
        )
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.request_timeout_seconds),
            follow_redirects=True,
            verify=True,
            limits=limits,
            headers={
                "User-Agent": settings.user_agent,
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def get_bytes(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        accept: str = "*/*",
        max_bytes: int | None = None,
        retries: int | None = None,
    ) -> tuple[httpx.Response, bytes]:
        last_error: Exception | None = None
        max_bytes = max_bytes or self.settings.max_article_bytes
        host = site_from_url(url) or "unknown"
        total_attempts = max(1, retries if retries is not None else self.settings.http_retries)
        for attempt in range(total_attempts):
            host_semaphore: asyncio.Semaphore | None = None
            try:
                async with self.global_semaphore:
                    host_semaphore = await self.host_limiter.enter(host)
                    try:
                        async with self.client.stream(
                            "GET", url, params=params, headers={"Accept": accept}
                        ) as response:
                            if response.status_code in self.RETRY_STATUS:
                                delay = parse_retry_after(response.headers.get("Retry-After"))
                                raise RetryableHttpError(response.status_code, delay)
                            response.raise_for_status()
                            declared = response.headers.get("Content-Length")
                            if declared and declared.isdigit() and int(declared) > max_bytes:
                                raise ValueError(f"resposta excede {max_bytes} bytes")
                            chunks: list[bytes] = []
                            size = 0
                            async for chunk in response.aiter_bytes():
                                size += len(chunk)
                                if size > max_bytes:
                                    raise ValueError(f"resposta excede {max_bytes} bytes")
                                chunks.append(chunk)
                            return response, b"".join(chunks)
                    finally:
                        host_semaphore.release()
            except RetryableHttpError as exc:
                last_error = exc
                if attempt + 1 >= total_attempts:
                    break
                base_delay = 10.0 if exc.status_code == 429 else 1.0
                delay = exc.retry_after or min(120.0, base_delay * (2 ** attempt))
                delay += random.uniform(0.15, 1.25)
                logging.warning(
                    "HTTP %s em %s; nova tentativa em %.1fs (%d/%d)",
                    exc.status_code, host, delay, attempt + 1, total_attempts,
                )
                await asyncio.sleep(delay)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
                if attempt + 1 >= total_attempts:
                    break
                await asyncio.sleep(min(30.0, (2 ** attempt) + random.uniform(0.15, 1.25)))
            except Exception:
                raise
        if isinstance(last_error, RetryableHttpError):
            raise last_error
        raise RuntimeError(f"falha HTTP após retries: {last_error}")

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        retries: int | None = None,
    ) -> tuple[httpx.Response, Any]:
        response, payload = await self.get_bytes(
            url,
            params=params,
            accept="application/json, text/json;q=0.9, */*;q=0.1",
            retries=retries,
        )
        return response, json.loads(payload.decode(response.encoding or "utf-8", errors="replace"))


class RetryableHttpError(Exception):
    def __init__(self, status_code: int, retry_after: float = 0.0):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after


def _iter_json_objects(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_json_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_objects(child)


class ArticleExtractor:
    """Baixa uma URL uma única vez por execução e extrai o conteúdo principal."""

    CONTENT_SELECTORS = (
        "[itemprop='articleBody']", "article", "main", ".article-content",
        ".article-body", ".entry-content", ".post-content", ".materia-conteudo",
        "#article-content", "#content",
    )

    def __init__(self, http: AsyncHttp, settings: Settings):
        self.http = http
        self.settings = settings
        self._tasks: dict[str, asyncio.Task[Article]] = {}
        self._cache_lock = asyncio.Lock()

    async def fetch(self, url: str, publisher_url: str = "") -> Article:
        key = normalize_url(url)
        if not key:
            return Article("", "", "")
        async with self._cache_lock:
            task = self._tasks.get(key)
            if task is None:
                task = asyncio.create_task(self._fetch_uncached(key, publisher_url))
                self._tasks[key] = task
        try:
            return await task
        except Exception as exc:
            logging.debug("Falha ao extrair %s: %s", key, exc)
            return Article(key, key, "", metadata={"extraction_error": truncate(str(exc), 500)})

    async def _fetch_uncached(self, url: str, publisher_url: str = "") -> Article:
        response, payload = await self.http.get_bytes(
            url,
            accept="text/html,text/plain,application/xhtml+xml,application/pdf;q=0.5,*/*;q=0.1",
            max_bytes=self.settings.max_article_bytes,
        )
        final_url = normalize_url(str(response.url))
        content_type = response.headers.get("Content-Type", "").lower()
        if is_google_news_url(url) and is_google_news_url(final_url):
            encoding = response.encoding or "utf-8"
            html_text = payload.decode(encoding, errors="replace")
            resolved_url = self._discover_google_publisher_url(html_text, publisher_url)
            if not resolved_url:
                return Article(
                    final_url=final_url,
                    canonical_url=final_url,
                    body="",
                    metadata={
                        "content_type": content_type,
                        "aggregator_url": url,
                        "publisher_url": normalize_url(publisher_url),
                        "aggregator_unresolved": True,
                    },
                    extraction_quality=0,
                )
            response, payload = await self.http.get_bytes(
                resolved_url,
                accept="text/html,text/plain,application/xhtml+xml,application/pdf;q=0.5,*/*;q=0.1",
                max_bytes=self.settings.max_article_bytes,
            )
            final_url = normalize_url(str(response.url))
            content_type = response.headers.get("Content-Type", "").lower()
            if is_google_news_url(final_url):
                return Article(
                    final_url=normalize_url(url),
                    canonical_url=normalize_url(url),
                    body="",
                    metadata={
                        "aggregator_url": normalize_url(url),
                        "publisher_url": normalize_url(publisher_url),
                        "aggregator_unresolved": True,
                    },
                    extraction_quality=0,
                )
        if "pdf" in content_type or payload[:4] == b"%PDF":
            article = Article(
                final_url, final_url, "", metadata={"content_type": content_type, "pdf": True}
            )
        elif "text/plain" in content_type or urlsplit(final_url).path.lower().endswith(".txt"):
            encoding = response.encoding or "utf-8"
            body = normalize_space(payload.decode(encoding, errors="replace"))
            article = Article(
                final_url=final_url,
                canonical_url=final_url,
                body=truncate(body, 250_000),
                metadata={"content_type": content_type, "plain_text": True},
                extraction_quality=90 if len(body) >= 200 else 35,
            )
        else:
            encoding = response.encoding or "utf-8"
            html_text = payload.decode(encoding, errors="replace")
            article = await asyncio.to_thread(self._extract_html, html_text, final_url, content_type)
        if is_google_news_url(url):
            article.metadata.update({
                "aggregator_url": normalize_url(url),
                "publisher_url": normalize_url(publisher_url),
                "aggregator_resolved": not is_google_news_url(article.final_url),
            })
        return article

    def _discover_google_publisher_url(self, html_text: str, publisher_url: str) -> str:
        """Localiza somente links que pertencem ao publicador informado pelo RSS."""
        expected_host = registrable_host(site_from_url(publisher_url))
        if not expected_host:
            return ""
        soup = BeautifulSoup(html_text, "html.parser")
        raw_candidates: list[str] = []
        canonical = soup.select_one("link[rel~='canonical'][href]")
        if canonical:
            raw_candidates.append(str(canonical.get("href", "")))
        refresh = soup.select_one("meta[http-equiv='refresh'][content]")
        if refresh:
            match = re.search(r"url\s*=\s*['\"]?([^'\";]+)", str(refresh.get("content", "")), re.I)
            if match:
                raw_candidates.append(match.group(1))
        for node in soup.select("a[href], [data-url], [data-href]"):
            raw_candidates.extend(str(node.get(attr, "")) for attr in ("href", "data-url", "data-href"))
        raw_candidates.extend(re.findall(r"https?://[^\s'\"<>\\]+", html_text))

        seen: set[str] = set()
        for raw in raw_candidates:
            candidate = normalize_url(raw, base_url="https://news.google.com")
            if not candidate or candidate in seen or is_google_news_url(candidate):
                continue
            seen.add(candidate)
            host = registrable_host(site_from_url(candidate))
            same_publisher = (
                host == expected_host
                or host.endswith(f".{expected_host}")
                or expected_host.endswith(f".{host}")
            )
            if same_publisher and urlsplit(candidate).path not in {"", "/"}:
                return candidate
        return ""

    def _extract_html(self, html_text: str, final_url: str, content_type: str) -> Article:
        soup = BeautifulSoup(html_text, "html.parser")
        canonical_tag = soup.select_one("link[rel~='canonical'][href]")
        canonical = normalize_url(
            canonical_tag.get("href", "") if canonical_tag else final_url,
            base_url=final_url,
        ) or final_url

        published_at: datetime | None = None
        modified_at: datetime | None = None
        json_body = ""
        jsonld_types: list[str] = []
        for script in soup.select("script[type='application/ld+json']"):
            try:
                value = json.loads(script.string or script.get_text() or "null")
            except (json.JSONDecodeError, TypeError):
                continue
            for obj in _iter_json_objects(value):
                type_value = obj.get("@type")
                if isinstance(type_value, list):
                    jsonld_types.extend(map(str, type_value))
                elif type_value:
                    jsonld_types.append(str(type_value))
                body = normalize_space(obj.get("articleBody"))
                if len(body) > len(json_body):
                    json_body = body
                published_at = published_at or parse_datetime(obj.get("datePublished"))
                modified_at = modified_at or parse_datetime(obj.get("dateModified"))

        for tag in soup.select("script,style,noscript,svg,canvas,form,nav,header,footer,aside"):
            tag.decompose()

        body = json_body
        quality = 95 if body else 0
        if not body:
            candidates: list[tuple[int, str]] = []
            for selector in self.CONTENT_SELECTORS:
                for node in soup.select(selector):
                    text_value = normalize_space(node.get_text(" ", strip=True))
                    if len(text_value) < 200:
                        continue
                    link_text = sum(len(normalize_space(a.get_text(" "))) for a in node.select("a"))
                    link_ratio = link_text / max(len(text_value), 1)
                    score = int(len(text_value) * max(0.2, 1.0 - link_ratio))
                    candidates.append((score, text_value))
            if candidates:
                _, body = max(candidates, key=lambda item: item[0])
                quality = 80
            else:
                paragraphs = [
                    normalize_space(node.get_text(" ", strip=True))
                    for node in soup.select("p")
                ]
                paragraphs = [text for text in paragraphs if len(text) >= 50]
                body = normalize_space(" ".join(paragraphs))
                quality = 45 if len(body) >= 300 else 20

        meta_published = soup.select_one(
            "meta[property='article:published_time'], meta[name='date'], time[datetime]"
        )
        if not published_at and meta_published:
            published_at = parse_datetime(
                meta_published.get("content") or meta_published.get("datetime")
            )
        meta_modified = soup.select_one("meta[property='article:modified_time']")
        if not modified_at and meta_modified:
            modified_at = parse_datetime(meta_modified.get("content"))

        return Article(
            final_url=final_url,
            canonical_url=canonical,
            body=truncate(body, 250_000),
            published_at=published_at,
            modified_at=modified_at,
            metadata={
                "content_type": content_type,
                "jsonld_types": sorted(set(jsonld_types))[:20],
            },
            extraction_quality=quality,
        )


class QueryBuilder:
    def __init__(self, settings: Settings):
        self.settings = settings

    def entity_names(self, entity: Entity) -> list[tuple[str, str]]:
        raw_values = [
            (entity.trade_name, "NOME_FANTASIA"),
            *((alias, "ALIAS_MARCA") for alias in entity.aliases),
            (remove_legal_suffix(entity.legal_name), "RAZAO_SEM_SUFIXO"),
            (entity.legal_name, "RAZAO_SOCIAL"),
        ]
        result: list[tuple[str, str]] = []
        seen: set[str] = set()
        for value, kind in raw_values:
            clean = normalize_space(value)
            key = normalize_for_match(clean)
            if len(key) < 4 or key in seen:
                continue
            seen.add(key)
            result.append((clean, kind))

        for value, _ in list(result):
            tokens = [token for token in normalize_for_match(value).split() if token not in NAME_STOPWORDS]
            if len(tokens) >= 3:
                short = " ".join(tokens[:3])
                short_tokens = significant_tokens(short)
                if len(short_tokens) >= 2 and not short_tokens.issubset(GENERIC_NAME_TOKENS):
                    key = normalize_for_match(short)
                    if key not in seen:
                        seen.add(key)
                        result.append((short.title(), "NOME_CURTO"))
        return result

    def build(self, entity: Entity) -> list[QuerySpec]:
        names = self.entity_names(entity)
        queries: list[QuerySpec] = []
        seen: set[str] = set()

        def add(text_value: str, kind: str, match_text: str, priority: int) -> None:
            text_value = normalize_space(text_value)
            key = normalize_for_match(text_value)
            if not text_value or key in seen:
                return
            seen.add(key)
            queries.append(QuerySpec(text_value, kind, match_text, priority))

        for name, kind in names[:3]:
            tokens = significant_tokens(name)
            query = f'"{name}"' if len(tokens) >= 2 else f'{name} "{entity.city or "Salvador"}"'
            add(query, kind, name, 100 if kind == "NOME_FANTASIA" else 90)

        digits = only_digits(entity.cnpj)
        if len(digits) == 14:
            add(f'"{format_cnpj(digits)}"', "CNPJ_FORMATADO", format_cnpj(digits), 120)
            add(digits, "CNPJ_NUMERICO", digits, 118)

        primary = names[0][0] if names else entity.legal_name
        if primary:
            quoted = f'"{primary}"'
            add(
                f"{quoted} (fraude OR autuação OR sonegação OR operação OR bloqueio OR falência)",
                "RISCO_COMPLIANCE", primary, 85,
            )
            add(
                f"{quoted} (licitação OR contrato OR pregão OR transparência)",
                "VIDA_PUBLICA", primary, 82,
            )
            add(
                f"{quoted} (processo OR tribunal OR dívida ativa OR recuperação judicial)",
                "JUDICIAL_FISCAL", primary, 84,
            )
            add(
                f"{quoted} (CVM OR CADE OR autarquia OR sanção OR inidônea)",
                "REGULATORIO", primary, 80,
            )
            add(
                f"{quoted} (site:in.gov.br OR site:ba.gov.br OR "
                f"site:salvador.ba.gov.br OR site:jus.br OR site:mp.br)",
                "DOMINIOS_OFICIAIS", primary, 78,
            )

        company_context = primary or entity.city or "Salvador"
        partner_candidates = []
        for partner in entity.partners:
            tokens = significant_tokens(partner)
            if len(tokens) >= 2 and normalize_for_match(partner) != normalize_for_match(entity.legal_name):
                partner_candidates.append(partner)
        for partner in partner_candidates[:4]:
            add(
                f'"{partner}" "{company_context}"',
                "SOCIO_ADMINISTRADOR", partner, 70,
            )

        queries.sort(key=lambda item: item.priority, reverse=True)
        return queries[: self.settings.max_queries_per_entity]


def incremental_since(entity: Entity, source: str, settings: Settings, official: bool = False) -> datetime:
    now = utc_now()
    default_days = settings.official_lookback_days if official else settings.initial_lookback_days
    if settings.full_refresh or source not in entity.state_by_source:
        return now - timedelta(days=default_days)
    previous = entity.state_by_source[source]
    floor = now - timedelta(days=default_days)
    return max(floor, previous - timedelta(days=settings.overlap_days))


def feed_candidates(
    payload: bytes,
    source: str,
    since: datetime,
    query: QuerySpec | None = None,
) -> list[Candidate]:
    parsed = feedparser.parse(payload)
    results: list[Candidate] = []
    for entry in getattr(parsed, "entries", []):
        title = normalize_space(getattr(entry, "title", ""))
        summary_html = getattr(entry, "summary", "") or getattr(entry, "description", "")
        summary = normalize_space(BeautifulSoup(str(summary_html), "html.parser").get_text(" "))
        url = normalize_url(getattr(entry, "link", ""))
        published_raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
        published = parse_datetime(published_raw)
        if published and published < since:
            continue
        if not title or not url:
            continue
        source_info = getattr(entry, "source", {}) or {}
        publisher_url = normalize_url(
            source_info.get("href", "") or source_info.get("url", "")
            if isinstance(source_info, dict) else ""
        )
        publisher_name = normalize_space(
            source_info.get("title", "") if isinstance(source_info, dict) else ""
        )
        metadata: dict[str, Any] = {"date_missing": published is None}
        site = site_from_url(url)
        if source == SOURCE_GOOGLE:
            metadata.update({
                "aggregator_url": url,
                "publisher_url": publisher_url,
                "publisher_name": publisher_name,
            })
            site = site_from_url(publisher_url) or site
        results.append(Candidate(
            source=source,
            title=title,
            url=url,
            published_at=published,
            summary=summary,
            site=site,
            query_text=query.text if query else "",
            query_kind=query.kind if query else "RSS",
            metadata=metadata,
        ))
    return results


class RssCatalog:
    def __init__(self, http: AsyncHttp, settings: Settings):
        self.http = http
        self.settings = settings
        extra = tuple(filter(None, (x.strip() for x in os.getenv("RFB_OSINT_EXTRA_RSS", "").split(";"))))
        self.feeds: dict[str, tuple[str, ...]] = {
            SOURCE_RSS_LOCAL: RSS_LOCAL_DEFAULT,
            SOURCE_RSS_NACIONAL: RSS_NATIONAL_DEFAULT + extra,
        }
        self.items: dict[str, list[Candidate]] = defaultdict(list)
        self.errors: dict[str, list[str]] = defaultdict(list)
        self.loaded_feeds: dict[str, int] = defaultdict(int)

    async def load(self) -> None:
        since = utc_now() - timedelta(days=self.settings.initial_lookback_days)

        async def load_one(source: str, url: str) -> None:
            try:
                _, payload = await self.http.get_bytes(
                    url,
                    accept="application/rss+xml,application/atom+xml,application/xml,text/xml,*/*;q=0.1",
                )
                self.items[source].extend(feed_candidates(payload, source, since))
                self.loaded_feeds[source] += 1
            except Exception as exc:
                self.errors[source].append(f"{url}: {truncate(str(exc), 300)}")

        await asyncio.gather(*(
            load_one(source, url)
            for source, urls in self.feeds.items()
            for url in urls
        ))
        for source in self.feeds:
            unique: dict[str, Candidate] = {}
            for item in self.items[source]:
                unique[sha256_text(f"{normalize_url(item.url)}|{normalize_for_match(item.title)}")] = item
            self.items[source] = list(unique.values())
            logging.info(
                "%s: %d itens; %d/%d feeds carregados; %d com erro",
                source, len(self.items[source]), self.loaded_feeds[source],
                len(self.feeds[source]), len(self.errors[source]),
            )

    def search(self, entity: Entity, source: str, queries: Sequence[QuerySpec]) -> SourceOutcome:
        since = incremental_since(entity, source, self.settings)
        results: list[Candidate] = []
        for item in self.items.get(source, []):
            if item.published_at and item.published_at < since:
                continue
            matched = best_matching_query(item.title + " " + item.summary, entity, queries)
            if matched:
                clone = Candidate(
                    source=item.source, title=item.title, url=item.url,
                    published_at=item.published_at, summary=item.summary, site=item.site,
                    query_text=matched.text, query_kind=matched.kind,
                    metadata=dict(item.metadata),
                )
                results.append(clone)
        errors = self.errors.get(source, [])
        successful_feeds = self.loaded_feeds.get(source, 0)
        success = successful_feeds > 0
        return SourceOutcome(
            source,
            results,
            success,
            " | ".join(errors[:3]),
            degraded=bool(errors) and success,
        )


def best_matching_query(text_value: str, entity: Entity, queries: Sequence[QuerySpec]) -> QuerySpec | None:
    normalized = normalize_for_match(text_value)
    title_tokens = significant_tokens(text_value)
    cnpj_digits = only_digits(text_value)
    for query in queries:
        needle = normalize_for_match(query.match_text)
        if query.kind.startswith("CNPJ") and only_digits(query.match_text) in cnpj_digits:
            return query
        if not needle:
            continue
        tokens = significant_tokens(query.match_text)
        if needle in normalized:
            if len(tokens) >= 2:
                return query
            geographic_tokens = significant_tokens(f"{entity.city} {entity.state} salvador bahia")
            if title_tokens & geographic_tokens:
                return query
        if len(tokens) >= 2 and ordered_terms_within(text_value, query.match_text):
            return query
    return None


class QueridoDiarioSource:
    BASE_URL = "https://api.queridodiario.org.br/gazettes"
    FALLBACK_URL = "https://queridodiario.ok.org.br/api/gazettes"

    def __init__(self, http: AsyncHttp, settings: Settings):
        self.http = http
        self.settings = settings
        self._rate_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._blocked_until = 0.0

    async def search(self, entity: Entity, queries: Sequence[QuerySpec]) -> list[SourceOutcome]:
        cnpj_task = asyncio.create_task(self._search_cnpj(entity))
        name_task = asyncio.create_task(self._search_names(entity, queries))
        return list(await asyncio.gather(cnpj_task, name_task))

    async def _request(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        # A documentação pública recomenda no máximo 60 requisições/minuto.
        async with self._rate_lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if self._blocked_until > now:
                raise RuntimeError(
                    "Querido Diário em cooldown por mais "
                    f"{self._blocked_until - now:.0f}s após indisponibilidade"
                )
            if self._next_request_at > now:
                await asyncio.sleep(self._next_request_at - now)
            endpoints = [self.settings.qd_base_url]
            known_hosts = {
                "api.queridodiario.org.br",
                "api.queridodiario.ok.org.br",
                "queridodiario.ok.org.br",
            }
            if (
                site_from_url(self.settings.qd_base_url) in known_hosts
                and normalize_url(self.FALLBACK_URL) not in {normalize_url(url) for url in endpoints}
            ):
                endpoints.append(self.FALLBACK_URL)
            last_error: Exception | None = None
            try:
                for endpoint in endpoints:
                    try:
                        _, payload = await self.http.get_json(
                            endpoint,
                            params=params,
                            retries=1,
                        )
                        if not isinstance(payload, dict):
                            return []
                        if endpoint != endpoints[0]:
                            logging.warning(
                                "Querido Diário acessado pelo endpoint alternativo %s", endpoint
                            )
                        return list(payload.get("gazettes") or [])
                    except httpx.HTTPStatusError:
                        raise
                    except Exception as exc:
                        last_error = exc
                self._blocked_until = loop.time() + self.settings.qd_cooldown_seconds
                raise RuntimeError(
                    f"Querido Diário indisponível nos endpoints oficiais: {last_error}"
                )
            finally:
                self._next_request_at = loop.time() + 1.05

    def _to_candidates(
        self,
        gazettes: Iterable[dict[str, Any]],
        source: str,
        query: str,
        kind: str,
        entity: Entity,
    ) -> list[Candidate]:
        results: list[Candidate] = []
        for gazette in gazettes:
            document_url = normalize_url(gazette.get("url", ""))
            text_url = normalize_url(gazette.get("txt_url", ""))
            fetch_url = text_url or document_url
            if not fetch_url:
                continue
            published = parse_datetime(gazette.get("date"))
            territory = normalize_space(gazette.get("territory_name"))
            state = normalize_space(gazette.get("state_code"))
            excerpts = gazette.get("excerpts") or ["Menção localizada no documento oficial."]
            for excerpt in excerpts[:3]:
                raw_excerpt = str(excerpt)
                summary = (
                    normalize_space(BeautifulSoup(raw_excerpt, "html.parser").get_text(" "))
                    if "<" in raw_excerpt and ">" in raw_excerpt
                    else normalize_space(raw_excerpt)
                )
                if source == SOURCE_QD_CNPJ:
                    digits = only_digits(entity.cnpj)
                    if len(digits) != 14 or digits not in only_digits(summary):
                        continue
                else:
                    normalized_query = normalize_for_match(query)
                    query_tokens = significant_token_list(query)
                    exact = bool(normalized_query and normalized_query in normalize_for_match(summary))
                    near = len(query_tokens) >= 2 and ordered_terms_within(summary, query)
                    single_distinctive = (
                        len(query_tokens) == 1
                        and query_tokens[0] not in GENERIC_NAME_TOKENS
                        and re.search(rf"\b{re.escape(query_tokens[0])}\b", normalize_for_match(summary))
                    )
                    if not (exact or near or single_distinctive):
                        continue
                results.append(Candidate(
                    source=source,
                    title=f"Diário Oficial — {territory}/{state} — {gazette.get('date', '')}",
                    url=fetch_url,
                    published_at=published,
                    summary=summary,
                    site=site_from_url(document_url or fetch_url),
                    query_text=query,
                    query_kind=kind,
                    metadata={
                        "territory": territory,
                        "state": state,
                        "official_document_url": document_url,
                        "text_url": text_url,
                        "canonical_url": document_url or fetch_url,
                    },
                ))
        return results

    async def _search_cnpj(self, entity: Entity) -> SourceOutcome:
        digits = only_digits(entity.cnpj)
        if len(digits) != 14:
            return SourceOutcome(SOURCE_QD_CNPJ, [], True)
        since = incremental_since(entity, SOURCE_QD_CNPJ, self.settings, official=True)
        try:
            for query in (format_cnpj(digits), digits):
                gazettes = await self._request({
                    "querystring": escape_simple_query_phrase(query),
                    "published_since": since.date().isoformat(),
                    "size": 30,
                    "excerpt_size": 1000,
                    "number_of_excerpts": 3,
                })
                candidates = self._to_candidates(
                    gazettes, SOURCE_QD_CNPJ, query, "CNPJ", entity
                )
                if candidates:
                    return SourceOutcome(
                        SOURCE_QD_CNPJ,
                        candidates,
                        True,
                    )
            return SourceOutcome(SOURCE_QD_CNPJ, [], True)
        except Exception as exc:
            return SourceOutcome(SOURCE_QD_CNPJ, [], False, truncate(str(exc), 1000))

    async def _search_names(
        self, entity: Entity, queries: Sequence[QuerySpec]
    ) -> SourceOutcome:
        since = incremental_since(entity, SOURCE_QD_NOME, self.settings, official=True)
        results: list[Candidate] = []
        try:
            name_queries = [q for q in queries if q.kind in {
                "NOME_FANTASIA", "ALIAS_MARCA", "RAZAO_SEM_SUFIXO", "RAZAO_SOCIAL"
            }][:3]
            for spec in name_queries:
                params = {
                    "querystring": escape_simple_query_phrase(spec.match_text),
                    "published_since": since.date().isoformat(),
                    "size": 20,
                    "excerpt_size": 1000,
                    "number_of_excerpts": 3,
                    "territory_ids": self.settings.territory_salvador,
                }
                gazettes = await self._request(params)
                if not gazettes:
                    params.pop("territory_ids")
                    gazettes = await self._request(params)
                results.extend(self._to_candidates(
                    gazettes, SOURCE_QD_NOME, spec.match_text, spec.kind, entity
                ))
            return SourceOutcome(SOURCE_QD_NOME, results, True)
        except Exception as exc:
            return SourceOutcome(SOURCE_QD_NOME, results, False, truncate(str(exc), 1000))


class GoogleNewsSource:
    BASE_URL = "https://news.google.com/rss/search"

    def __init__(self, http: AsyncHttp, settings: Settings):
        self.http = http
        self.settings = settings

    async def search(self, entity: Entity, queries: Sequence[QuerySpec]) -> SourceOutcome:
        since = incremental_since(entity, SOURCE_GOOGLE, self.settings)
        days = max(1, min(self.settings.initial_lookback_days, (utc_now() - since).days + 1))

        async def one(spec: QuerySpec) -> list[Candidate]:
            params = {
                "q": f"{spec.text} when:{days}d",
                "hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419",
            }
            _, payload = await self.http.get_bytes(
                self.BASE_URL,
                params=params,
                accept="application/rss+xml,application/xml,text/xml,*/*;q=0.1",
            )
            return feed_candidates(payload, SOURCE_GOOGLE, since, spec)

        results: list[Candidate] = []
        errors: list[str] = []
        successful_queries = 0
        tasks = [one(spec) for spec in queries]
        for value in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(value, BaseException):
                errors.append(truncate(str(value), 300))
            else:
                successful_queries += 1
                results.extend(value)
        success = successful_queries > 0
        return SourceOutcome(
            SOURCE_GOOGLE,
            results,
            success,
            " | ".join(errors[:3]),
            degraded=bool(errors) and success,
        )


class GdeltCoolingDown(RuntimeError):
    pass


class GdeltSource:
    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self, http: AsyncHttp, settings: Settings):
        self.http = http
        self.settings = settings
        self._rate_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._blocked_until = 0.0
        self._batch_lock = asyncio.Lock()
        self._pending: list[
            tuple[Entity, QuerySpec, asyncio.Future[SourceOutcome]]
        ] = []
        self._flush_task: asyncio.Task[None] | None = None

    async def _request(self, params: dict[str, Any]) -> Any:
        # O endpoint do GDELT é sensível a rajadas: todas as empresas compartilham
        # este lock. Durante o cooldown falhamos rápido, sem bloquear o lote inteiro.
        async with self._rate_lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if self._blocked_until > now:
                raise GdeltCoolingDown(
                    f"GDELT em cooldown por mais {self._blocked_until - now:.0f}s após HTTP 429"
                )
            if self._next_request_at > now:
                await asyncio.sleep(self._next_request_at - now)
            try:
                _, payload = await self.http.get_json(
                    self.BASE_URL,
                    params=params,
                    retries=self.settings.gdelt_retries,
                )
                return payload
            except RetryableHttpError as exc:
                if exc.status_code == 429:
                    self._blocked_until = loop.time() + max(
                        self.settings.gdelt_cooldown_seconds,
                        exc.retry_after,
                    )
                raise
            finally:
                self._next_request_at = (
                    loop.time()
                    + self.settings.gdelt_interval_seconds
                    + random.uniform(0.0, 0.75)
                )

    async def search(self, entity: Entity, queries: Sequence[QuerySpec]) -> SourceOutcome:
        main_query = next((q for q in queries if q.kind in {
            "NOME_FANTASIA", "ALIAS_MARCA", "RAZAO_SEM_SUFIXO", "RAZAO_SOCIAL"
        }), None)
        if not main_query:
            return SourceOutcome(SOURCE_GDELT, [], True)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[SourceOutcome] = loop.create_future()
        async with self._batch_lock:
            self._pending.append((entity, main_query, future))
            if self._flush_task is None or self._flush_task.done():
                self._flush_task = asyncio.create_task(self._flush_pending())
        return await future

    async def _flush_pending(self) -> None:
        # Permite que as empresas iniciadas no mesmo lote compartilhem uma consulta.
        await asyncio.sleep(0.05)
        async with self._batch_lock:
            pending = self._pending
            self._pending = []
            self._flush_task = None
        batch_size = max(1, self.settings.gdelt_batch_size)
        for index in range(0, len(pending), batch_size):
            chunk = pending[index:index + batch_size]
            try:
                outcomes = await self._execute_batch(chunk)
            except Exception as exc:
                outcomes = [
                    SourceOutcome(SOURCE_GDELT, [], False, truncate(str(exc), 1000))
                    for _ in chunk
                ]
            for (_entity, _query, future), outcome in zip(chunk, outcomes):
                if not future.done():
                    future.set_result(outcome)

    async def _execute_batch(
        self,
        requests: Sequence[tuple[Entity, QuerySpec, asyncio.Future[SourceOutcome]]],
    ) -> list[SourceOutcome]:
        since = min(
            incremental_since(entity, SOURCE_GDELT, self.settings)
            for entity, _query, _future in requests
        )
        since = max(since, utc_now() - timedelta(days=89))
        names: list[str] = []
        for _entity, query, _future in requests:
            clean = normalize_space(query.match_text).replace('"', " ")
            if clean and normalize_for_match(clean) not in {
                normalize_for_match(name) for name in names
            }:
                names.append(clean)
        clauses = " OR ".join(f'"{name}"' for name in names)
        query_text = f"({clauses}) sourcelang:portuguese" if len(names) > 1 else f"{clauses} sourcelang:portuguese"
        params = {
            "query": query_text,
            "mode": "ArtList",
            "maxrecords": min(250, max(50, 25 * len(requests))),
            "startdatetime": since.strftime("%Y%m%d%H%M%S"),
            "enddatetime": utc_now().strftime("%Y%m%d%H%M%S"),
            "format": "json",
            "sort": "DateDesc",
        }
        try:
            payload = await self._request(params)
            results_by_request: list[list[Candidate]] = [[] for _ in requests]
            for item in payload.get("articles", []) if isinstance(payload, dict) else []:
                url = normalize_url(item.get("url", ""))
                if not url:
                    continue
                title = normalize_space(item.get("title"))
                normalized_title = normalize_for_match(title)
                for request_index, (_entity, main_query, _future) in enumerate(requests):
                    needle = normalize_for_match(main_query.match_text)
                    tokens = significant_token_list(main_query.match_text)
                    exact = bool(needle and needle in normalized_title)
                    near = len(tokens) >= 2 and ordered_terms_within(title, main_query.match_text, 120)
                    single = (
                        len(tokens) == 1
                        and tokens[0] not in GENERIC_NAME_TOKENS
                        and re.search(rf"\b{re.escape(tokens[0])}\b", normalized_title)
                    )
                    if not (exact or near or single):
                        continue
                    results_by_request[request_index].append(Candidate(
                        source=SOURCE_GDELT,
                        title=title,
                        url=url,
                        published_at=parse_datetime(item.get("seendate")),
                        summary="",
                        site=normalize_space(item.get("domain")) or site_from_url(url),
                        query_text=main_query.text,
                        query_kind=main_query.kind,
                        metadata={
                            "language": item.get("language"),
                            "sourcecountry": item.get("sourcecountry"),
                            "gdelt_batched": True,
                        },
                    ))
            return [
                SourceOutcome(SOURCE_GDELT, results, True)
                for results in results_by_request
            ]
        except (GdeltCoolingDown, RetryableHttpError) as exc:
            if isinstance(exc, GdeltCoolingDown) or exc.status_code == 429:
                return [
                    SourceOutcome(
                        SOURCE_GDELT,
                        [],
                        True,
                        truncate(str(exc), 1000),
                        degraded=True,
                    )
                    for _ in requests
                ]
            return [
                SourceOutcome(SOURCE_GDELT, [], False, truncate(str(exc), 1000))
                for _ in requests
            ]
        except Exception as exc:
            return [
                SourceOutcome(SOURCE_GDELT, [], False, truncate(str(exc), 1000))
                for _ in requests
            ]


class SearxngSource:
    """Pesquisa web opcional. Configure RFB_SEARXNG_URL com um endpoint /search."""

    def __init__(self, http: AsyncHttp, settings: Settings):
        self.http = http
        self.settings = settings

    async def search(self, entity: Entity, queries: Sequence[QuerySpec]) -> SourceOutcome:
        if not self.settings.searxng_url:
            return SourceOutcome(SOURCE_WEB, [], True)
        since = incremental_since(entity, SOURCE_WEB, self.settings)
        selected = [q for q in queries if q.kind in {
            "RISCO_COMPLIANCE", "VIDA_PUBLICA", "JUDICIAL_FISCAL",
            "REGULATORIO", "DOMINIOS_OFICIAIS", "CNPJ_FORMATADO",
        }][:6]

        async def one(spec: QuerySpec) -> list[Candidate]:
            _, payload = await self.http.get_json(self.settings.searxng_url or "", params={
                "q": spec.text, "format": "json", "language": "pt-BR", "safesearch": 0,
            })
            results = []
            for item in payload.get("results", []) if isinstance(payload, dict) else []:
                published = parse_datetime(item.get("publishedDate"))
                if published and published < since:
                    continue
                url = normalize_url(item.get("url", ""))
                if not url:
                    continue
                results.append(Candidate(
                    source=SOURCE_WEB,
                    title=normalize_space(item.get("title")),
                    url=url,
                    published_at=published,
                    summary=normalize_space(item.get("content")),
                    site=site_from_url(url),
                    query_text=spec.text,
                    query_kind=spec.kind,
                    metadata={"engine": item.get("engine")},
                ))
            return results

        results: list[Candidate] = []
        errors: list[str] = []
        successful_queries = 0
        for value in await asyncio.gather(*(one(q) for q in selected), return_exceptions=True):
            if isinstance(value, BaseException):
                errors.append(truncate(str(value), 300))
            else:
                successful_queries += 1
                results.extend(value)
        success = successful_queries > 0 or not selected
        return SourceOutcome(
            SOURCE_WEB,
            results,
            success,
            " | ".join(errors[:3]),
            degraded=bool(errors) and success,
        )


class RelevanceScorer:
    SOURCE_POINTS = {
        SOURCE_QD_CNPJ: 28,
        SOURCE_QD_NOME: 20,
        SOURCE_RSS_LOCAL: 10,
        SOURCE_RSS_NACIONAL: 8,
        SOURCE_GOOGLE: 6,
        SOURCE_GDELT: 5,
        SOURCE_WEB: 8,
    }

    CRITICAL_CATEGORIES = {
        "FRAUDE_COMPLIANCE", "OPERACAO_POLICIAL", "BLOQUEIO_BENS", "INSOLVENCIA",
    }
    HIGH_CATEGORIES = {"DIVIDA_ATIVA", "AUTUACAO_FISCAL", "JUDICIAL", "REGULATORIO"}

    def __init__(self, settings: Settings, query_builder: QueryBuilder):
        self.settings = settings
        self.query_builder = query_builder

    def quick_identity_score(self, candidate: Candidate, entity: Entity) -> int:
        text_value = f"{candidate.title} {candidate.summary}"
        normalized = normalize_for_match(text_value)
        title_normalized = normalize_for_match(candidate.title)
        score = 0
        digits = only_digits(entity.cnpj)
        if len(digits) == 14 and digits in only_digits(text_value):
            score += 70
        for name, _ in self.query_builder.entity_names(entity):
            needle = normalize_for_match(name)
            tokens = significant_tokens(name)
            if needle and needle in title_normalized:
                distinctive = len(tokens) == 1 and not tokens.issubset(GENERIC_NAME_TOKENS)
                score = max(score, 50 if len(tokens) >= 2 else 40 if distinctive else 12)
            elif needle and needle in normalized:
                distinctive = len(tokens) == 1 and not tokens.issubset(GENERIC_NAME_TOKENS)
                score = max(score, 38 if len(tokens) >= 2 else 22 if distinctive else 8)
            elif len(tokens) >= 2 and ordered_terms_within(candidate.title, name, 120):
                score = max(score, 42)
            elif len(tokens) >= 2 and ordered_terms_within(text_value, name):
                score = max(score, 30)
        if candidate.source.startswith("QUERIDO_DIARIO"):
            score += 15
        return score

    def assess(self, candidate: Candidate, article: Article, entity: Entity) -> Assessment:
        title = normalize_space(candidate.title)
        summary = normalize_space(candidate.summary)
        body = normalize_space(article.body)
        full_text = normalize_space(f"{title} {summary} {body}")
        normalized_title = normalize_for_match(title)
        normalized_text = normalize_for_match(full_text)
        full_tokens = significant_tokens(full_text)
        score = 0
        reasons: dict[str, Any] = {}
        matched_terms: list[str] = []
        strong_identity = False
        identity_verified = False

        digits = only_digits(entity.cnpj)
        if len(digits) == 14 and digits in only_digits(full_text):
            score += 70
            reasons["cnpj_exato"] = 70
            matched_terms.append(format_cnpj(digits))
            strong_identity = True
            identity_verified = True

        best_name_score = 0
        best_coverage = 0.0
        for name, kind in self.query_builder.entity_names(entity):
            normalized_name = normalize_for_match(name)
            tokens = significant_tokens(name)
            if not normalized_name or not tokens:
                continue
            local_score = 0
            match_mode = ""
            if normalized_name in normalized_title:
                distinctive = len(tokens) == 1 and not tokens.issubset(GENERIC_NAME_TOKENS)
                local_score = 42 if len(tokens) >= 2 else 34 if distinctive else 10
                match_mode = "frase_titulo"
            elif normalized_name in normalized_text:
                distinctive = len(tokens) == 1 and not tokens.issubset(GENERIC_NAME_TOKENS)
                local_score = 32 if len(tokens) >= 2 else 22 if distinctive else 6
                match_mode = "frase_texto"
            elif len(tokens) >= 2 and ordered_terms_within(title, name, 120):
                local_score = 36
                match_mode = "proximidade_titulo"
            elif len(tokens) >= 2 and ordered_terms_within(full_text, name):
                local_score = 30
                match_mode = "proximidade_texto"
            if match_mode.startswith("frase"):
                best_coverage = max(best_coverage, 1.0)
            elif match_mode.startswith("proximidade"):
                best_coverage = max(best_coverage, 0.9)
            if local_score > best_name_score:
                best_name_score = local_score
                reasons["melhor_nome"] = {
                    "valor": name,
                    "tipo": kind,
                    "pontos": local_score,
                    "modo": match_mode,
                }
            if local_score >= 30:
                matched_terms.append(name)
                strong_identity = True
                identity_verified = True
        score += best_name_score

        company_context = strong_identity
        partner_score = 0
        for partner in entity.partners[:10]:
            normalized_partner = normalize_for_match(partner)
            if normalized_partner and normalized_partner in normalized_text:
                matched_terms.append(partner)
                partner_score = max(partner_score, 18 if company_context else 6)
        if partner_score:
            score += partner_score
            reasons["socio_administrador"] = partner_score

        geographic_terms = {
            token for token in significant_tokens(f"{entity.city} {entity.state} salvador bahia")
            if token
        }
        geographic_match = sorted(geographic_terms & full_tokens)
        if geographic_match:
            score += 6
            reasons["contexto_geografico"] = geographic_match

        primary_tokens = significant_tokens(
            entity.trade_name or remove_legal_suffix(entity.legal_name)
        )
        ambiguous = len(primary_tokens) <= 1 or primary_tokens.issubset(GENERIC_NAME_TOKENS)
        if ambiguous and not strong_identity:
            penalty = 28
            score -= penalty
            reasons["penalidade_nome_ambiguo"] = -penalty

        source_points = self.SOURCE_POINTS.get(candidate.source, 0)
        score += source_points
        reasons["fonte"] = source_points

        published = article.published_at or candidate.published_at
        if published:
            age_days = max(0, (utc_now() - published).days)
            recency_points = 14 if age_days <= 1 else 10 if age_days <= 7 else 6 if age_days <= 30 else 2
            score += recency_points
            reasons["recencia"] = recency_points

        identity_floor = 18 if candidate.source == SOURCE_QD_CNPJ else 28
        identity_score = int(reasons.get("cnpj_exato", 0)) + best_name_score + partner_score
        relevant = (
            identity_verified
            and score >= self.settings.minimum_relevance_score
            and identity_score >= identity_floor
        )

        categories, triggers, evidence, entity_role = self._contextual_risk_matches(
            full_text,
            entity,
            cnpj_exact=bool(reasons.get("cnpj_exato")),
        )
        risk_score = self._risk_score(categories, entity_role)
        if categories:
            reasons["risco_contextual"] = {
                "score_risco": risk_score,
                "categorias": categories,
                "papel_entidade": entity_role,
            }
        evidence = evidence or self._evidence(full_text, None, matched_terms)
        event_date = self._extract_event_date(evidence or full_text, published)
        risk_level = self._risk_level(risk_score)
        reasons["score_final"] = score
        reasons["score_risco"] = risk_score
        reasons["papel_entidade"] = entity_role
        reasons["versao_classificacao"] = "v3-contextual-2"
        reasons["identidade"] = identity_score
        reasons["identidade_verificada"] = identity_verified
        reasons["cobertura_tokens"] = round(best_coverage, 3)
        reasons["termos_match"] = sorted(set(matched_terms))
        return Assessment(
            relevant=relevant,
            score=score,
            risk_score=risk_score,
            risk_level=risk_level,
            entity_role=entity_role,
            categories=categories,
            triggers=triggers,
            evidence=evidence,
            reasons=reasons,
            event_date=event_date,
        )

    def _is_public_authority(self, entity: Entity) -> bool:
        identity = normalize_for_match(f"{entity.legal_name} {entity.trade_name}")
        return any(marker in identity for marker in PUBLIC_AUTHORITY_MARKERS)

    def _entity_terms(self, entity: Entity) -> list[str]:
        terms: list[str] = []
        for value, _ in self.query_builder.entity_names(entity):
            normalized = normalize_for_match(value)
            if len(normalized) >= 4 and normalized not in terms:
                terms.append(normalized)
        return sorted(terms, key=len, reverse=True)

    def _role_for_context(
        self,
        normalized: str,
        terms: Sequence[str],
        *,
        public_authority: bool,
    ) -> str:
        for term in terms:
            entity_pattern = rf"\b{re.escape(term)}\b"
            passive_target = (
                rf"{entity_pattern}.{{0,90}}\b(?:foi|e|esta|sera|teve|tem|entrou|sofreu|"
                rf"enfrenta|responde)\b.{{0,55}}\b(?:alvo|investigad[ao]|autuad[ao]|"
                rf"condenad[ao]|denunciad[ao]|multad[ao]|acusad[ao]|bloquead[ao]|"
                rf"falencia|recuperacao judicial|execucao fiscal|processo sancionador)\b"
            )
            action_target = (
                rf"\b(?:{'|'.join(ADVERSE_TARGET_VERBS)})\b.{{0,120}}{entity_pattern}"
            )
            noun_target = (
                rf"\b(?:fraude|corrupcao|sonegacao|falencia|recuperacao judicial|"
                rf"divida ativa|operacao policial|processo|acao)\b.{{0,40}}"
                rf"\b(?:contra|na|no|nas|nos|da|do|das|dos|envolvendo)\b.{{0,80}}"
                rf"{entity_pattern}"
            )
            if re.search(passive_target, normalized) or re.search(action_target, normalized):
                return "ALVO"

            contracted = (
                rf"(?:{entity_pattern}.{{0,80}}\b(?:venceu|assina|assinou|firmou|"
                rf"contratada|adjudicataria)\b|\b(?:contratada|adjudicataria)\b.{{0,80}}"
                rf"{entity_pattern})"
            )
            if re.search(contracted, normalized):
                return "CONTRATADA"

            agent = (
                rf"{entity_pattern}.{{0,65}}\b(?:{'|'.join(AUTHORITY_ACTION_VERBS)}|"
                rf"lanca|lancar|oferece|oferecer|cria|criar|implementa|implementar|"
                rf"previne|prevenir|evita|evitar)\b"
            )
            if re.search(agent, normalized):
                return "AUTORIDADE" if public_authority else "AGENTE"
            if re.search(noun_target, normalized):
                return "ALVO"
        return "CITADA"

    def _contextual_risk_matches(
        self,
        text_value: str,
        entity: Entity,
        *,
        cnpj_exact: bool,
    ) -> tuple[list[str], list[str], str, str]:
        terms = self._entity_terms(entity)
        digits = only_digits(entity.cnpj)
        public_authority = self._is_public_authority(entity)
        accepted: dict[str, list[str]] = defaultdict(list)
        evidence = ""
        evidence_weight = -1
        best_role = "CITADA"
        role_priority = {
            "CITADA": 0,
            "AGENTE": 1,
            "AUTORIDADE": 1,
            "CONTRATADA": 2,
            "ALVO_PROVAVEL": 3,
            "ALVO": 4,
        }
        interest_categories = {"LICITACAO_CONTRATO", "ALTERACAO_SOCIETARIA"}

        sentences = [
            normalize_space(match.group(0))
            for match in re.finditer(r"[^.!?\n]+(?:[.!?]+|$)", text_value)
            if normalize_space(match.group(0))
        ] or [normalize_space(text_value)]
        for sentence in sentences:
            normalized_sentence = normalize_for_match(sentence)
            sentence_digits = only_digits(sentence)
            mentioned_terms = [term for term in terms if term in normalized_sentence]
            cnpj_in_sentence = len(digits) == 14 and digits in sentence_digits
            if not mentioned_terms and not cnpj_in_sentence:
                continue

            # Sentenças anormalmente longas (menus/texto agregado) são reduzidas à
            # vizinhança da entidade para impedir gatilhos sem relação contextual.
            context = normalized_sentence
            if len(context) > 900:
                positions = [context.find(term) for term in mentioned_terms]
                positions = [position for position in positions if position >= 0]
                center = positions[0] if positions else 0
                context = context[max(0, center - 350): center + 550]

            role = self._role_for_context(
                context,
                mentioned_terms or terms,
                public_authority=public_authority,
            )
            if role_priority[role] > role_priority[best_role]:
                best_role = role

            sentence_hits: dict[str, list[str]] = defaultdict(list)
            for category, patterns in COMPILED_RISK_PATTERNS.items():
                for pattern in patterns:
                    hits = [match.group(0) for match in pattern.finditer(context)]
                    if hits:
                        sentence_hits[category].extend(hits)
            if not sentence_hits:
                continue

            effective_role = role
            for category, hits in sentence_hits.items():
                accept = category in interest_categories or role == "ALVO"
                if (
                    not accept
                    and role == "CITADA"
                    and not public_authority
                    and (cnpj_in_sentence or cnpj_exact or any(len(term.split()) >= 2 for term in mentioned_terms))
                ):
                    accept = True
                    effective_role = "ALVO_PROVAVEL"
                if not accept:
                    continue
                accepted[category].extend(hits[:3])
                weight = RISK_WEIGHTS[category]
                if weight > evidence_weight:
                    evidence = truncate(sentence, 700)
                    evidence_weight = weight
            if role_priority[effective_role] > role_priority[best_role]:
                best_role = effective_role

        categories = sorted(accepted, key=lambda value: RISK_WEIGHTS[value], reverse=True)
        triggers = sorted({hit for hits in accepted.values() for hit in hits})[:20]
        return categories, triggers, evidence, best_role

    def _risk_score(self, categories: Sequence[str], entity_role: str) -> int:
        if not categories:
            return 0
        score = max(RISK_WEIGHTS[category] for category in categories) * 2
        score += {
            "ALVO": 20,
            "ALVO_PROVAVEL": 10,
            "CONTRATADA": 4,
        }.get(entity_role, 0)
        if len(categories) > 1:
            score += min(10, (len(categories) - 1) * 5)
        return min(100, score)

    def _evidence(
        self, full_text: str, trigger_position: int | None, matched_terms: Sequence[str]
    ) -> str:
        if not full_text:
            return ""
        normalized = normalize_for_match(full_text)
        position = trigger_position
        if position is None:
            for term in matched_terms:
                idx = normalized.find(normalize_for_match(term))
                if idx >= 0:
                    position = idx
                    break
        if position is None:
            return truncate(full_text, 700)
        # A posição normalizada não é idêntica à original, mas é uma boa aproximação.
        start = max(0, position - 260)
        end = min(len(full_text), position + 440)
        return truncate(full_text[start:end], 700)

    def _extract_event_date(
        self, text_value: str, published_at: datetime | None
    ) -> date | None:
        today = utc_now().date()
        candidates: list[date] = []
        for day_text, month_text, year_text in re.findall(
            r"(?<!\d)([0-3]?\d)[/-]([01]?\d)[/-]((?:19|20)\d{2})(?!\d)", text_value
        ):
            try:
                value = date(int(year_text), int(month_text), int(day_text))
            except ValueError:
                continue
            if date(today.year - 10, 1, 1) <= value <= today + timedelta(days=1):
                candidates.append(value)
        if not candidates:
            return None
        if published_at:
            publication_date = published_at.date()
            plausible = [value for value in candidates if value <= publication_date + timedelta(days=1)]
            if plausible:
                return min(plausible, key=lambda value: abs((publication_date - value).days))
        return max(candidates)

    def _risk_level(self, risk_score: int) -> str:
        if risk_score >= 85:
            return "CRITICO"
        if risk_score >= 60:
            return "ALTO"
        if risk_score >= 35:
            return "MEDIO"
        if risk_score > 0:
            return "ATENCAO"
        return "INFORMATIVO"


class Deduplicator:
    """Índice LSH simples para hashes exatos, URL e SimHash em memória."""

    def __init__(self, settings: Settings, existing: Sequence[dict[str, Any]]):
        self.settings = settings
        self.by_content: dict[str, str] = {}
        self.by_url: dict[str, str] = {}
        self.records: dict[str, tuple[int, str]] = {}
        self.buckets: dict[tuple[int, int], set[str]] = defaultdict(set)
        for row in existing:
            doc_id = normalize_space(row.get("id_documento"))
            content_hash = normalize_space(row.get("hash_conteudo"))
            url_hash = normalize_space(row.get("url_hash"))
            simhash_value = normalize_space(row.get("simhash_hex"))
            title = normalize_space(row.get("titulo_normalizado"))
            if content_hash:
                self.by_content[content_hash] = doc_id
            if url_hash:
                self.by_url[url_hash] = doc_id
            if simhash_value:
                try:
                    self._add_simhash(doc_id, int(simhash_value, 16), title)
                except ValueError:
                    pass

    def _add_simhash(self, doc_id: str, value: int, title: str) -> None:
        self.records[doc_id] = (value, title)
        for index in range(4):
            self.buckets[(index, (value >> (index * 16)) & 0xFFFF)].add(doc_id)

    def resolve(self, document: DocumentRecord) -> str:
        existing_id = self.by_content.get(document.content_hash) or self.by_url.get(document.url_hash)
        current_simhash = int(document.simhash_hex, 16)
        if not existing_id:
            candidate_ids: set[str] = set()
            for index in range(4):
                candidate_ids.update(
                    self.buckets.get((index, (current_simhash >> (index * 16)) & 0xFFFF), set())
                )
            for candidate_id in candidate_ids:
                previous_simhash, previous_title = self.records[candidate_id]
                if (
                    hamming_distance(current_simhash, previous_simhash) <= self.settings.simhash_max_distance
                    and token_jaccard(document.normalized_title, previous_title) >= 0.55
                ):
                    existing_id = candidate_id
                    document.metadata["near_duplicate_of"] = candidate_id
                    break

        if existing_id:
            document.document_id = existing_id
            return existing_id

        self.by_content[document.content_hash] = document.document_id
        self.by_url[document.url_hash] = document.document_id
        self._add_simhash(document.document_id, current_simhash, document.normalized_title)
        return document.document_id


class EntityProcessor:
    def __init__(
        self,
        settings: Settings,
        http: AsyncHttp,
        rss: RssCatalog,
        extractor: ArticleExtractor,
    ):
        self.settings = settings
        self.query_builder = QueryBuilder(settings)
        self.scorer = RelevanceScorer(settings, self.query_builder)
        self.rss = rss
        self.extractor = extractor
        self.qd = QueridoDiarioSource(http, settings)
        self.google = GoogleNewsSource(http, settings)
        self.gdelt = GdeltSource(http, settings)
        self.searx = SearxngSource(http, settings)

    async def process(self, entity: Entity) -> EntityOutcome:
        queries = self.query_builder.build(entity)
        if not queries:
            logging.warning("%s sem termos úteis para busca", entity.entity_id)
            return EntityOutcome([], [], [])

        rss_outcomes = [
            self.rss.search(entity, SOURCE_RSS_LOCAL, queries),
            self.rss.search(entity, SOURCE_RSS_NACIONAL, queries),
        ]
        tasks: list[Any] = [
            self.qd.search(entity, queries),
            self.google.search(entity, queries),
            self.gdelt.search(entity, queries),
        ]
        if self.settings.searxng_url:
            tasks.append(self.searx.search(entity, queries))
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        outcomes = list(rss_outcomes)
        for value in gathered:
            if isinstance(value, BaseException):
                logging.error(
                    "Falha inesperada em fonte para %s: %s", entity.entity_id, value
                )
                continue
            if isinstance(value, list):
                outcomes.extend(value)
            else:
                outcomes.append(value)

        candidates = self._unique_candidates(outcomes)
        candidates.sort(
            key=lambda item: (
                self.scorer.quick_identity_score(item, entity),
                item.published_at or datetime.min.replace(tzinfo=UTC),
            ),
            reverse=True,
        )

        fetch_candidates = self._select_fetch_candidates(candidates, entity)
        article_by_key: dict[str, Article] = {}
        if self.settings.fetch_articles:
            articles = await asyncio.gather(*(
                self.extractor.fetch(
                    item.url,
                    normalize_space(item.metadata.get("publisher_url")),
                )
                for item in fetch_candidates
            ))
            article_by_key = {
                normalize_url(item.url): article
                for item, article in zip(fetch_candidates, articles)
            }

        captured_at = utc_now()
        pairs: list[tuple[DocumentRecord, MentionRecord]] = []
        for candidate in candidates:
            article = article_by_key.get(
                normalize_url(candidate.url),
                Article(
                    candidate.url,
                    normalize_url(candidate.url),
                    "",
                    metadata={"snippet_only": True},
                ),
            )
            assessment = self.scorer.assess(candidate, article, entity)
            if not assessment.relevant:
                continue
            document = self._document(candidate, article, assessment, captured_at)
            mention = MentionRecord(
                document_id=document.document_id,
                entity_id=entity.entity_id,
                cnpj_base=entity.cnpj_base,
                cnpj=entity.cnpj,
                legal_name=entity.legal_name,
                trade_name=entity.trade_name,
                first_seen_at=captured_at,
                last_seen_at=captured_at,
                query_kind=candidate.query_kind,
                query_text=candidate.query_text,
                score=assessment.score,
                risk_score=assessment.risk_score,
                risk_level=assessment.risk_level,
                entity_role=assessment.entity_role,
                risk_categories=assessment.categories,
                triggers=assessment.triggers,
                evidence=assessment.evidence,
                matched_terms=assessment.reasons.get("termos_match", []),
                score_reasons=assessment.reasons,
            )
            pairs.append((document, mention))

        pairs.sort(
            key=lambda pair: (pair[1].risk_score, pair[1].score),
            reverse=True,
        )
        pairs = self._limit_pairs_by_source(pairs)
        return EntityOutcome(
            documents=[pair[0] for pair in pairs],
            mentions=[pair[1] for pair in pairs],
            source_outcomes=outcomes,
        )

    def _select_fetch_candidates(
        self,
        candidates: Sequence[Candidate],
        entity: Entity,
    ) -> list[Candidate]:
        eligible = [
            item for item in candidates
            if self.scorer.quick_identity_score(item, entity) >= 15
        ]
        eligible.sort(
            key=lambda item: (
                bool(item.metadata.get("text_url")),
                not is_google_news_url(item.url),
                self.scorer.quick_identity_score(item, entity),
                item.published_at or datetime.min.replace(tzinfo=UTC),
            ),
            reverse=True,
        )
        return eligible[: self.settings.max_article_fetch_per_entity]

    def _limit_pairs_by_source(
        self,
        pairs: Sequence[tuple[DocumentRecord, MentionRecord]],
    ) -> list[tuple[DocumentRecord, MentionRecord]]:
        selected: list[tuple[DocumentRecord, MentionRecord]] = []
        source_counts: dict[str, int] = defaultdict(int)
        for pair in pairs:
            source = pair[0].source
            if source_counts[source] >= self.settings.max_results_per_source:
                continue
            selected.append(pair)
            source_counts[source] += 1
            if len(selected) >= self.settings.max_results_per_entity:
                break
        return selected

    def _unique_candidates(self, outcomes: Sequence[SourceOutcome]) -> list[Candidate]:
        unique: dict[str, Candidate] = {}
        for outcome in outcomes:
            for item in outcome.candidates:
                url_key = normalize_url(item.url)
                title_key = normalize_for_match(item.title)
                key = sha256_text(url_key or title_key)
                previous = unique.get(key)
                if previous is None or len(item.summary) > len(previous.summary):
                    unique[key] = item
        return list(unique.values())

    def _document(
        self,
        candidate: Candidate,
        article: Article,
        assessment: Assessment,
        captured_at: datetime,
    ) -> DocumentRecord:
        preferred_canonical = normalize_url(candidate.metadata.get("canonical_url", ""))
        canonical = preferred_canonical or normalize_url(
            article.canonical_url or article.final_url or candidate.url
        )
        original_url = normalize_url(
            candidate.metadata.get("official_document_url", "") or candidate.url
        )
        body = normalize_space(article.body)
        summary = truncate(candidate.summary, 10_000)
        content_basis = body if len(body) >= 200 else f"{candidate.title} {summary}"
        normalized_content = normalize_for_match(content_basis)
        content_hash = sha256_text(normalized_content)
        document_id = content_hash
        simhash_value = simhash64(f"{candidate.title} {content_basis[:20_000]}")
        metadata = dict(candidate.metadata)
        metadata.update(article.metadata)
        if article.final_url and normalize_url(candidate.url) != normalize_url(article.final_url):
            metadata["redirected_from"] = normalize_url(candidate.url)
        canonical_site = site_from_url(canonical)
        if is_google_news_url(canonical) and candidate.site:
            canonical_site = candidate.site
            metadata["canonical_pending_resolution"] = True
        return DocumentRecord(
            document_id=document_id,
            original_url=original_url,
            canonical_url=canonical,
            url_hash=sha256_text(canonical) if canonical else "",
            source=candidate.source,
            observed_sources=[candidate.source],
            site=canonical_site or candidate.site,
            title=truncate(candidate.title, 4000),
            normalized_title=normalize_for_match(candidate.title),
            summary=summary,
            body=body,
            published_at=article.published_at or candidate.published_at,
            modified_at=article.modified_at,
            event_date=assessment.event_date,
            first_seen_at=captured_at,
            last_seen_at=captured_at,
            content_hash=content_hash,
            simhash_hex=simhash_hex(simhash_value),
            extraction_quality=article.extraction_quality,
            metadata=metadata,
            aggregator_url=(
                normalize_url(candidate.metadata.get("aggregator_url", ""))
                if candidate.source == SOURCE_GOOGLE else ""
            ),
        )


def build_settings(args: argparse.Namespace) -> Settings:
    year = args.year or env_int("RFB_ANO_REFERENCIA", datetime.now().year)
    origin = args.origin or os.getenv("RFB_ORIGEM_MONITORAMENTO") or f"CONTRIBUINTE_SUPORTE_{year}"
    return Settings(
        year=year,
        origin=origin,
        output_dir=BASE_DIR / "saida_noticias",
        pg_dbname=os.getenv("RFB_PG_DB", "db_rfb"),
        pg_user=os.getenv("RFB_PG_USER", "cif_user"),
        pg_password=os.getenv("RFB_PG_PASS") or None,
        pg_host=os.getenv("RFB_PG_HOST", "valedosrios"),
        pg_port=env_int("RFB_PG_PORT", 5432),
        target_batch_size=max(1, args.batch_size or env_int("RFB_OSINT_BATCH_SIZE", 200)),
        entity_concurrency=max(1, args.concurrency or env_int("RFB_OSINT_ENTITY_CONCURRENCY", 8)),
        http_concurrency=max(1, env_int("RFB_OSINT_HTTP_CONCURRENCY", 20)),
        per_host_concurrency=max(1, env_int("RFB_OSINT_HOST_CONCURRENCY", 2)),
        per_host_interval_seconds=max(0.0, env_float("RFB_OSINT_HOST_INTERVAL", 0.35)),
        gdelt_interval_seconds=max(1.0, env_float("RFB_GDELT_INTERVAL_SECONDS", 6.0)),
        gdelt_cooldown_seconds=max(1.0, env_float("RFB_GDELT_COOLDOWN_SECONDS", 900.0)),
        gdelt_retries=max(1, env_int("RFB_GDELT_RETRIES", 2)),
        gdelt_batch_size=max(1, env_int("RFB_GDELT_BATCH_SIZE", 15)),
        request_timeout_seconds=max(1.0, env_float("RFB_OSINT_HTTP_TIMEOUT", 25.0)),
        http_retries=max(1, env_int("RFB_OSINT_HTTP_RETRIES", 4)),
        initial_lookback_days=max(1, env_int("RFB_OSINT_LOOKBACK_DAYS", 30)),
        official_lookback_days=max(1, env_int("RFB_OSINT_OFFICIAL_LOOKBACK_DAYS", 90)),
        overlap_days=max(0, env_int("RFB_OSINT_OVERLAP_DAYS", 3)),
        max_results_per_entity=max(1, env_int("RFB_OSINT_MAX_RESULTS", 30)),
        max_results_per_source=max(1, env_int("RFB_OSINT_MAX_RESULTS_PER_SOURCE", 12)),
        max_queries_per_entity=max(1, env_int("RFB_OSINT_MAX_QUERIES", 12)),
        max_article_fetch_per_entity=max(0, env_int("RFB_OSINT_MAX_ARTICLES", 25)),
        max_article_bytes=max(100_000, env_int("RFB_OSINT_MAX_ARTICLE_BYTES", 2_500_000)),
        minimum_relevance_score=env_int("RFB_OSINT_MIN_SCORE", 38),
        dedup_lookback_days=max(1, env_int("RFB_OSINT_DEDUP_DAYS", 180)),
        dedup_recent_limit=max(1, env_int("RFB_OSINT_DEDUP_LIMIT", 100_000)),
        simhash_max_distance=max(0, env_int("RFB_OSINT_SIMHASH_DISTANCE", 3)),
        fetch_articles=not args.no_article_fetch,
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
        searxng_url=os.getenv("RFB_SEARXNG_URL") or None,
        qd_base_url=os.getenv(
            "RFB_QD_BASE_URL", "https://api.queridodiario.org.br/gazettes"
        ),
        qd_cooldown_seconds=max(1.0, env_float("RFB_QD_COOLDOWN_SECONDS", 300.0)),
        territory_salvador=os.getenv("RFB_QD_TERRITORY", "2927408"),
        user_agent=os.getenv(
            "RFB_OSINT_USER_AGENT",
            "SEFAZ-Salvador-OSINT/3.0 (+monitoramento institucional)",
        ),
    )


def setup_logging(settings: Settings, verbose: bool) -> Path:
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = settings.output_dir / f"log_v3_{datetime.now():%Y-%m-%d}.txt"
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
    logging.getLogger("httpx").setLevel(logging.DEBUG if verbose else logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.DEBUG if verbose else logging.WARNING)
    return log_path


async def run_pipeline(
    settings: Settings,
    database: Database,
    execution_id: str,
    limit: int | None,
) -> dict[str, Any]:
    started_at = utc_now()
    metrics: dict[str, Any] = {
        "versao": "v3",
        "empresas_processadas": 0,
        "documentos_coletados": 0,
        "mencoes_relevantes": 0,
        "fontes_com_erro": 0,
        "fontes_degradadas": 0,
        "resultados_por_fonte": defaultdict(int),
        "candidatos_por_fonte": defaultdict(int),
        "erros_por_fonte": defaultdict(int),
        "alertas_por_fonte": defaultdict(int),
        "modo_incremental": not settings.full_refresh,
        "dry_run": settings.dry_run,
    }
    if not settings.dry_run:
        database.write_execution(execution_id, started_at, None, "EM_EXECUCAO", metrics)

    recent = await asyncio.to_thread(database.load_recent_fingerprints)
    deduplicator = Deduplicator(settings, recent)
    http = AsyncHttp(settings)
    rss = RssCatalog(http, settings)
    extractor = ArticleExtractor(http, settings)
    processor = EntityProcessor(settings, http, rss, extractor)
    entity_semaphore = asyncio.Semaphore(settings.entity_concurrency)
    csv_path = settings.output_dir / f"resultados_v3_{datetime.now():%Y%m%d_%H%M%S}.csv"
    csv_sink = CsvSink(csv_path)

    async def process_safe(entity: Entity) -> tuple[str, EntityOutcome]:
        async with entity_semaphore:
            try:
                return entity.entity_id, await processor.process(entity)
            except Exception as exc:
                logging.error("Falha ao processar %s: %s", entity.entity_id, exc, exc_info=True)
                return entity.entity_id, EntityOutcome([], [], [
                    SourceOutcome("PROCESSAMENTO_EMPRESA", [], False, truncate(str(exc), 1000))
                ])

    try:
        logging.info("Carregando catálogo RSS compartilhado...")
        await rss.load()
        for batch_number, batch in enumerate(database.iter_entity_batches(limit), start=1):
            logging.info("Lote %d: processando %d empresas", batch_number, len(batch))
            wrapped = await asyncio.gather(*(process_safe(entity) for entity in batch))
            documents: dict[str, DocumentRecord] = {}
            mentions: dict[tuple[str, str], MentionRecord] = {}
            states: list[StateRecord] = []
            reported_source_messages: set[tuple[str, str, bool]] = set()
            for (entity_id, outcome) in wrapped:
                for document, mention in zip(outcome.documents, outcome.mentions):
                    resolved_id = deduplicator.resolve(document)
                    document.document_id = resolved_id
                    mention.document_id = resolved_id
                    previous = documents.get(resolved_id)
                    if previous is not None:
                        merged_sources = sorted(set(previous.observed_sources + document.observed_sources))
                        alternate_urls = sorted({
                            url for url in (
                                previous.canonical_url,
                                document.canonical_url,
                                *(previous.metadata.get("alternate_urls") or []),
                                *(document.metadata.get("alternate_urls") or []),
                            ) if url
                        })
                        if document.extraction_quality > previous.extraction_quality:
                            document.observed_sources = merged_sources
                            document.metadata["alternate_urls"] = alternate_urls
                            documents[resolved_id] = document
                        else:
                            previous.observed_sources = merged_sources
                            previous.metadata["alternate_urls"] = alternate_urls
                    else:
                        documents[resolved_id] = document
                    mention_key = (resolved_id, mention.entity_id)
                    previous_mention = mentions.get(mention_key)
                    if previous_mention is None or mention.score > previous_mention.score:
                        mentions[mention_key] = mention
                attempted_at = utc_now()
                for source_outcome in outcome.source_outcomes:
                    states.append(StateRecord(
                        entity_id=entity_id,
                        source=source_outcome.source,
                        attempted_at=attempted_at,
                        success=source_outcome.success,
                        error=source_outcome.error,
                        degraded=source_outcome.degraded,
                    ))
                    metrics["candidatos_por_fonte"][source_outcome.source] += len(
                        source_outcome.candidates
                    )
                    if not source_outcome.success:
                        metrics["fontes_com_erro"] += 1
                        metrics["erros_por_fonte"][source_outcome.source] += 1
                        message_key = (
                            source_outcome.source,
                            source_outcome.error or "erro sem detalhe",
                            False,
                        )
                        if message_key not in reported_source_messages:
                            reported_source_messages.add(message_key)
                            logging.warning(
                                "%s falhou no lote (primeira empresa %s): %s",
                                source_outcome.source,
                                entity_id,
                                source_outcome.error or "erro sem detalhe",
                            )
                    elif source_outcome.degraded:
                        metrics["fontes_degradadas"] += 1
                        metrics["alertas_por_fonte"][source_outcome.source] += 1
                        message_key = (
                            source_outcome.source,
                            source_outcome.error or "parte das consultas falhou",
                            True,
                        )
                        if message_key not in reported_source_messages:
                            reported_source_messages.add(message_key)
                            logging.warning(
                                "%s operou parcialmente no lote (primeira empresa %s): %s",
                                source_outcome.source,
                                entity_id,
                                source_outcome.error or "parte das consultas falhou",
                            )

            document_rows = list(documents.values())
            mention_rows = list(mentions.values())
            if not settings.dry_run:
                await asyncio.to_thread(database.upsert_batch, document_rows, mention_rows, states)
            csv_sink.write(document_rows, mention_rows)

            metrics["empresas_processadas"] += len(batch)
            metrics["documentos_coletados"] += len(document_rows)
            metrics["mencoes_relevantes"] += len(mention_rows)
            for document in document_rows:
                metrics["resultados_por_fonte"][document.source] += 1
            logging.info(
                "Lote %d concluído: %d documentos, %d menções relevantes",
                batch_number, len(document_rows), len(mention_rows),
            )
            source_summary = ", ".join(
                f"{source}={count}"
                for source, count in sorted(
                    metrics["resultados_por_fonte"].items(),
                    key=lambda item: (-item[1], item[0]),
                )
            ) or "sem resultados"
            logging.info("Resultados acumulados por fonte: %s", source_summary)
    finally:
        csv_sink.close()
        await http.close()

    metrics["resultados_por_fonte"] = dict(metrics["resultados_por_fonte"])
    metrics["candidatos_por_fonte"] = dict(metrics["candidatos_por_fonte"])
    metrics["erros_por_fonte"] = dict(metrics["erros_por_fonte"])
    metrics["alertas_por_fonte"] = dict(metrics["alertas_por_fonte"])
    metrics["arquivo_csv"] = str(csv_path)
    metrics["duracao_segundos"] = round((utc_now() - started_at).total_seconds(), 3)
    return metrics


def run_self_test() -> None:
    assert normalize_url("https://EXAMPLE.com/a/?utm_source=x&b=2&a=1#fim") == "https://example.com/a?a=1&b=2"
    assert remove_legal_suffix("EMPRESA TESTE LTDA") == "EMPRESA TESTE"
    assert hamming_distance(simhash64("texto igual"), simhash64("texto igual")) == 0
    settings = Settings(year=2026, origin="TESTE", output_dir=BASE_DIR / "saida_noticias")
    entity = Entity(
        entity_id="1", cnpj_base="12345678", cnpj="12345678000190",
        legal_name="EMPRESA TESTE LTDA", trade_name="MARCA TESTE",
        city="Salvador", state="BA", partners=["FULANO DE TAL"], aliases=[],
    )
    builder = QueryBuilder(settings)
    queries = builder.build(entity)
    assert any(query.kind == "CNPJ_FORMATADO" for query in queries)
    scorer = RelevanceScorer(settings, builder)
    candidate = Candidate(
        source=SOURCE_RSS_LOCAL,
        title="Operação investiga fraude da Marca Teste em Salvador",
        url="https://example.com/noticia",
        published_at=utc_now(),
        summary="A empresa Marca Teste foi alvo de operação policial.",
    )
    assessment = scorer.assess(candidate, Article(candidate.url, candidate.url, ""), entity)
    assert assessment.relevant
    assert "FRAUDE_COMPLIANCE" in assessment.categories
    assert "OPERACAO_POLICIAL" in assessment.categories


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monitoramento OSINT incremental V3 de contribuintes em PostgreSQL."
    )
    parser.add_argument("--ano", dest="year", type=int, help="Ano de referência da carteira.")
    parser.add_argument("--origem", dest="origin", help="Origem de monitoramento.")
    parser.add_argument("--limite", dest="limit", type=int, help="Limita empresas para homologação.")
    parser.add_argument("--batch-size", type=int, help="Empresas lidas/processadas por lote.")
    parser.add_argument("--concorrencia", dest="concurrency", type=int, help="Empresas concorrentes.")
    parser.add_argument("--full-refresh", action="store_true", help="Ignora estado incremental.")
    parser.add_argument("--dry-run", action="store_true", help="Não grava documentos, menções ou estado.")
    parser.add_argument("--no-article-fetch", action="store_true", help="Não baixa corpo dos artigos.")
    parser.add_argument("--create-schema-only", action="store_true", help="Cria tabelas V3 e encerra.")
    parser.add_argument("--self-test", action="store_true", help="Executa testes locais sem banco/rede.")
    parser.add_argument("--verbose", action="store_true", help="Ativa logs de depuração.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.self_test:
        run_self_test()
        print("Self-test V3 concluído com sucesso.")
        return 0

    settings = build_settings(args)
    log_path = setup_logging(settings, args.verbose)
    database = Database(settings)
    execution_id = str(uuid4())
    started_at = utc_now()
    metrics: dict[str, Any] = {}
    try:
        logging.info(
            "V3 iniciada | ano=%s origem=%s host=%s banco=%s incremental=%s",
            settings.year, settings.origin, settings.pg_host, settings.pg_dbname,
            not settings.full_refresh,
        )
        database.ensure_schema()
        if args.create_schema_only:
            logging.info("Estrutura V3 criada/verificada.")
            return 0
        metrics = asyncio.run(run_pipeline(settings, database, execution_id, args.limit))
        status = (
            "SUCESSO_PARCIAL"
            if metrics.get("fontes_com_erro")
            else "SUCESSO_COM_ALERTAS"
            if metrics.get("fontes_degradadas")
            else "SUCESSO"
        )
        if not settings.dry_run:
            database.write_execution(
                execution_id, started_at, utc_now(), status, metrics,
                "Execução incremental V3 com extração e desduplicação de conteúdo.",
            )
        logging.info(
            "Execução %s: %d empresas, %d documentos, %d menções. CSV: %s",
            status,
            metrics.get("empresas_processadas", 0),
            metrics.get("documentos_coletados", 0),
            metrics.get("mencoes_relevantes", 0),
            metrics.get("arquivo_csv", ""),
        )
        logging.info("Log: %s", log_path)
        return 0 if status in {"SUCESSO", "SUCESSO_COM_ALERTAS"} else 2
    except KeyboardInterrupt:
        logging.warning("Execução interrompida pelo usuário.")
        return 130
    except Exception as exc:
        logging.exception("Falha fatal na V3: %s", exc)
        if not settings.dry_run:
            try:
                database.write_execution(
                    execution_id, started_at, utc_now(), "FALHA", metrics, truncate(str(exc), 2000)
                )
            except Exception:
                logging.exception("Não foi possível registrar a falha no banco.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
