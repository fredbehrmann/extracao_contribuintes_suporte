# -*- coding: utf-8 -*-
"""
FASE 2 V2 - Motor de busca multi-fonte para contribuintes suporte
=================================================================

Por que esta versão é diferente da V1:

PROBLEMA V1:
  - Buscava apenas em RSS nacionais (G1, Folha) onde empresas de Salvador
    raramente aparecem.
  - Usava o nome exato entre aspas ("DISTRIBUIDORA XYZ LTDA"), mas jornais
    não escrevem assim — omitem o LTDA, abreviam, usam apelido.
  - Janela de 3 dias era curta demais.
  - Nunca encontrava nada para empresas médias/locais.

SOLUÇÃO V2 — quatro mudanças fundamentais:

  1. PROCESSADOR DE NOMES: antes de qualquer busca, gera variantes inteligentes
     do nome da empresa (remove LTDA/S.A., extrai nome curto, usa nome fantasia).
     Ex: "SUPERMERCADOS NORDESTE LTDA" → ["Supermercados Nordeste", "Nordeste"]

  2. QUERIDO DIÁRIO (fonte nova): API gratuita que indexa Diários Oficiais
     (Municipal de Salvador, Estadual da Bahia, Federal). Busca por CNPJ (exata,
     sem ruído) e por nome. Retorna autuações, contratos, licitações, embargos —
     exatamente o que inteligência fiscal precisa.
     API: https://queridodiario.ok.org.br/api/gazettes

  3. RSS LOCAIS DA BAHIA (fontes novas): Correio da Bahia, A Tarde, Bahia
     Notícias, Tribuna da Bahia. Empresas de Salvador aparecem muito mais
     nesses portais do que no G1 ou Folha.

  4. GDELT (fonte nova): Base global de notícias gratuita. Cobre portais
     brasileiros em português. Útil para empresas com cobertura nacional.

  5. FILTRO DE RELEVÂNCIA: depois de coletar, verifica se o texto encontrado
     realmente menciona a empresa (reduz falsos positivos).

Schema do banco: idêntico à V1 — compatibilidade total com relatórios existentes.
"""

import os
import re
import html
import json
import time
import hashlib
import unicodedata
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from uuid import uuid4
import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as date_parser


# =========================================================
# CONFIGURAÇÕES
# =========================================================

pg_config_saida = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": os.getenv("RFB_PG_HOST", "valedosrios"),
    "port": int(os.getenv("RFB_PG_PORT", "5432"))
}

SCHEMA_DESTINO = "suporte_arrecadacao"
TB_EMPRESAS_ALVO = "noticias_empresas_alvo"
TB_RESULTADOS = "noticias_resultados"
TB_EXECUCOES = "noticias_execucoes_fase2"

ANO_REFERENCIA = 2026
ORIGEM_MONITORAMENTO = f"CONTRIBUINTE_SUPORTE_{ANO_REFERENCIA}"

PASTA_SAIDA = Path("saida_noticias")
PASTA_SAIDA.mkdir(exist_ok=True)

DATA_EXECUCAO = datetime.now()
DATA_EXECUCAO_STR = DATA_EXECUCAO.strftime("%Y-%m-%d")

ARQUIVO_LOG = PASTA_SAIDA / f"log_v2_{DATA_EXECUCAO_STR}.txt"
ARQUIVO_SAIDA = PASTA_SAIDA / f"resultados_v2_{DATA_EXECUCAO_STR}.csv"

# --- Janela de busca ---
# V1 usava 3 dias. V2 usa 30 para notícias e 90 para Diários Oficiais.
DIAS_JANELA_NOTICIAS = 30
DIAS_JANELA_DIARIO_CNPJ = 90   # CNPJ: busca mais ampla, sem risco de ruído
DIAS_JANELA_DIARIO_NOME = 60

# --- Limites ---
MAX_RESULTADOS_POR_EMPRESA = 20
MAX_WORKERS_RSS = 8
MAX_WORKERS_BUSCA = 3
TIMEOUT_HTTP = 20

# --- Querido Diário ---
# Código IBGE do município de Salvador (BA)
TERRITORIO_SALVADOR = "2927408"
# Deixar None para buscar em todos os territórios indexados (DOU, DOE-BA, DOM-Salvador)
TERRITORIO_ABRANGENTE = None

# --- GDELT ---
USAR_GDELT = True  # API gratuita, sem chave

# --- Cabeçalho HTTP ---
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# --- Feeds RSS locais da Bahia (adicionados na V2) ---
RSS_FEEDS_BAHIA = [
    "https://www.correio24horas.com.br/rss.xml",
    "https://www.correio24horas.com.br/rss/bahia.xml",
    "https://atarde.com.br/feed",
    "https://www.bahianoticias.com.br/rss.xml",
    "https://tribunadabahia.com.br/feed",
    "https://www.acordacidade.com.br/feed",
    "https://bahia.ba.gov.br/rss.xml",
]

# --- Feeds RSS nacionais (mantidos da V1) ---
RSS_FEEDS_NACIONAIS = [
    "https://g1.globo.com/rss/g1/",
    "https://g1.globo.com/economia/rss/g1/",
    "https://valor.globo.com/rss/",
    "https://www.cnnbrasil.com.br/rss/",
    "https://www.estadao.com.br/rss/ultimas.xml",
    "https://www1.folha.uol.com.br/rss/ultimas_noticias.xml",
    "https://exame.com/feed/",
    "https://www.infomoney.com.br/feed/",
]

RSS_FEEDS_TODOS = RSS_FEEDS_BAHIA + RSS_FEEDS_NACIONAIS

# --- Palavras sensíveis para scoring ---
PALAVRAS_SENSIVEIS = [
    "fraude", "fraudes", "operação", "operações",
    "investigação", "investigado", "investigada",
    "lavagem", "lavagem de dinheiro",
    "denúncia", "denunciado", "denunciada",
    "autuação", "autuações", "embargado", "embargo",
    "licitação", "contrato", "bloqueio",
    "prisão", "preso", "presa",
    "fusão", "incorporação", "aquisição", "cisão",
    "falência", "recuperação judicial", "dissolução",
    "sonegação", "sonegou", "multa fiscal",
    "irregularidade", "infração tributária",
    "benefício fiscal", "planejamento tributário",
    "substituição tributária", "inadimplência",
]


# =========================================================
# LOG
# =========================================================

def log(msg: str) -> None:
    stamp = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(stamp)
    with open(ARQUIVO_LOG, "a", encoding="utf-8") as f:
        f.write(stamp + "\n")


# =========================================================
# UTILITÁRIOS GERAIS
# =========================================================

def strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(ch)
    )


def normalize_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    value = html.unescape(str(value))
    return re.sub(r"\s+", " ", value).strip()


def normalize_lower(value) -> str:
    return normalize_text(value).lower()


def strip_html_tags(text) -> str:
    if not text:
        return ""
    s = str(text)
    if "<" not in s:
        return normalize_text(s)
    try:
        return normalize_text(BeautifulSoup(s, "html.parser").get_text(" "))
    except Exception:
        return normalize_text(s)


def extrair_site(url: str) -> str:
    m = re.search(r"https?://([^/]+)", url or "")
    return m.group(1).lower() if m else ""


def normalizar_url(url) -> str:
    if not url:
        return ""
    return re.sub(r"#.*$", "", str(url).strip())


def garantir_tz(dt) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = date_parser.parse(dt)
        except Exception:
            return None
    if isinstance(dt, datetime):
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return None


def parse_data(value) -> Optional[datetime]:
    return garantir_tz(value)


def eh_recente(dt, dias: int = 30) -> bool:
    dt = garantir_tz(dt)
    if not dt:
        return False
    return dt >= datetime.now(timezone.utc) - timedelta(days=dias)


def eh_hoje(dt) -> bool:
    dt = garantir_tz(dt)
    if not dt:
        return False
    return dt.astimezone().date() == datetime.now().astimezone().date()


def hash_noticia(url: str, titulo: str) -> str:
    base = f"{normalizar_url(url)}|{normalize_lower(titulo)}"
    return hashlib.sha256(base.encode()).hexdigest()


def encontrar_palavras_sensiveis(texto: str) -> List[str]:
    t = normalize_lower(texto)
    return sorted({p for p in PALAVRAS_SENSIVEIS if p.lower() in t})


def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ de 14 dígitos para XX.XXX.XXX/XXXX-XX."""
    d = re.sub(r"\D", "", cnpj or "")
    if len(d) != 14:
        return cnpj
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"


# =========================================================
# PROCESSADOR DE NOMES
# Peça central da nova abordagem.
# =========================================================

# Sufixos jurídicos a remover
SUFIXOS_LEGAIS = {
    "LTDA", "LTDA.", "S/A", "S.A.", "S.A", "SA", "S/A.", "EIRELI", "ME",
    "MEI", "EPP", "SS", "S/S", "S/C", "CIA", "CIA.", "COMPANHIA",
    "SOCIEDADE ANONIMA", "LIMITADA", "INC", "LLC", "EMPRESARIO INDIVIDUAL",
}

# Preposições e artigos que não ajudam na busca
STOPWORDS_NOME = {
    "DE", "DA", "DO", "DAS", "DOS", "E", "EM", "COM", "PARA", "POR",
    "A", "O", "AS", "OS", "NO", "NA", "NOS", "NAS", "PELO", "PELA",
    "AO", "AOS", "NUM", "NUMA", "DEL", "DI",
}


class ProcessadorNomes:
    """
    Gera variantes de busca a partir de nomes de empresas.

    O problema central da V1: o nome "POSTO DE COMBUSTIVEIS NORDESTE LTDA"
    nunca aparece assim em jornais. A solução é gerar variantes progressivamente
    mais simples, cada uma com estratégia diferente:
      1. Nome fantasia (quando disponível) — geralmente o mais reconhecível
      2. Razão social sem sufixo legal — "Posto de Combustiveis Nordeste"
      3. Nome curto (2-3 palavras mais significativas) — "Posto Nordeste"
    """

    def remover_sufixo_legal(self, nome: str) -> str:
        """Remove sufixos do tipo LTDA, S/A, EIRELI do final do nome."""
        if not nome:
            return nome
        partes = nome.strip().split()
        while partes and partes[-1].upper().rstrip(".") in SUFIXOS_LEGAIS:
            partes.pop()
        resultado = " ".join(partes)
        # Remove padrões residuais como "S / A" ou "S.A"
        resultado = re.sub(r"\s+[Ss]\s*/\s*[Aa]\.?\s*$", "", resultado)
        resultado = re.sub(r"\s+[Ss]\.[Aa]\.?\s*$", "", resultado)
        return re.sub(r"\s+", " ", resultado).strip()

    def titulo_case(self, nome: str) -> str:
        """Converte para Title Case preservando siglas conhecidas."""
        palavras = nome.lower().split()
        resultado = []
        for i, p in enumerate(palavras):
            if i == 0 or p.upper() not in STOPWORDS_NOME:
                resultado.append(p.capitalize())
            else:
                resultado.append(p)
        return " ".join(resultado)

    def nome_curto(self, nome: str, max_palavras: int = 3) -> Optional[str]:
        """
        Extrai as palavras mais significativas do nome.
        Ex: "Posto de Combustiveis Nordeste" → "Posto Combustiveis Nordeste"
        Ex: "Distribuidora Bahia de Alimentos" → "Distribuidora Bahia Alimentos"
        """
        sem_acento = strip_accents(nome.upper())
        tokens = re.split(r"\s+", sem_acento)
        significativos = [
            t for t in tokens
            if t not in STOPWORDS_NOME and len(t) > 2 and re.match(r"^[A-Z]", t)
        ]
        if not significativos:
            return None
        curto_upper = " ".join(significativos[:max_palavras])
        curto = self.titulo_case(curto_upper.lower())
        # Só retorna se for diferente do nome completo
        return curto if normalize_lower(curto) != normalize_lower(nome) else None

    def e_util_para_busca(self, nome: str) -> bool:
        """Verifica se o nome tem conteúdo mínimo para uma busca."""
        if not nome or len(nome.strip()) < 4:
            return False
        sem_sufixo = self.remover_sufixo_legal(nome)
        tokens = [t for t in re.split(r"\s+", sem_sufixo) if len(t) > 2]
        return len(tokens) >= 1

    def gerar_variantes(
        self,
        razao_social: str,
        nome_fantasia: Optional[str] = None,
    ) -> List[Dict]:
        """
        Retorna lista ordenada de variantes de busca. Cada variante é um dict:
          - nome: string para busca
          - tipo: identificador da variante
          - com_aspas: True = busca exata com aspas (para nomes longos/únicos)
          - adicionar_ba: True = adiciona "Salvador" na query (para evitar ruído
                          em nomes curtos/ambíguos)
        """
        variantes: List[Dict] = []
        vistos: Set[str] = set()

        def adicionar(nome: str, tipo: str, com_aspas: bool, adicionar_ba: bool = False):
            chave = normalize_lower(nome)
            if chave in vistos or len(chave) < 4:
                return
            vistos.add(chave)
            variantes.append({
                "nome": nome,
                "tipo": tipo,
                "com_aspas": com_aspas,
                "adicionar_ba": adicionar_ba,
            })

        # --- Variante 1: Nome fantasia (mais reconhecível pelo público) ---
        if nome_fantasia:
            nf_limpo = self.remover_sufixo_legal(normalize_text(nome_fantasia))
            if nf_limpo and self.e_util_para_busca(nf_limpo):
                nf_tc = self.titulo_case(nf_limpo.lower())
                # Aspas se o nome fantasia for único e específico (> 2 palavras)
                com_aspas = len(nf_tc.split()) > 2
                adicionar(nf_tc, "fantasia", com_aspas=com_aspas)
                # Versão curta do fantasia
                curto_nf = self.nome_curto(nf_limpo)
                if curto_nf:
                    adicionar(curto_nf, "fantasia_curto", com_aspas=False, adicionar_ba=True)

        # --- Variante 2: Razão social sem sufixo legal ---
        if razao_social:
            rs_limpa = self.remover_sufixo_legal(normalize_text(razao_social))
            if rs_limpa and self.e_util_para_busca(rs_limpa):
                rs_tc = self.titulo_case(rs_limpa.lower())
                com_aspas = len(rs_tc.split()) > 2
                adicionar(rs_tc, "razao_limpa", com_aspas=com_aspas)
                # Versão curta da razão social
                curto_rs = self.nome_curto(rs_limpa)
                if curto_rs:
                    adicionar(curto_rs, "razao_curto", com_aspas=False, adicionar_ba=True)

        return variantes


# =========================================================
# FONTE 1: QUERIDO DIÁRIO
# =========================================================

class BuscadorQueridoDiario:
    """
    Busca nos Diários Oficiais via API gratuita do Querido Diário.

    Cobertura:
      - Diário Oficial Municipal de Salvador (DOM)
      - Diário Oficial do Estado da Bahia (DOE-BA)
      - Diário Oficial da União (DOU)
      e centenas de outros municípios brasileiros.

    Por que é a fonte mais valiosa para inteligência fiscal:
      - Autuações, embargos, processos administrativos
      - Contratos e licitações públicas
      - Notificações e intimações fiscais
      - A busca por CNPJ é exata — sem risco de falso positivo.

    Documentação: https://queridodiario.ok.org.br/api/docs
    """

    BASE_URL = "https://queridodiario.ok.org.br/api/gazettes"

    def _request(self, params: dict) -> Optional[dict]:
        try:
            resp = requests.get(
                self.BASE_URL,
                params=params,
                headers=HEADERS,
                timeout=TIMEOUT_HTTP,
            )
            if resp.status_code == 200:
                return resp.json()
            log(f"  [QueridoDiario] HTTP {resp.status_code} para params={params}")
        except Exception as e:
            log(f"  [QueridoDiario] Erro de conexão: {e}")
        return None

    def _gazette_para_resultado(
        self, gazette: dict, query: str, subtipo_fonte: str
    ) -> List[Dict]:
        """Converte um gazette da API para lista de dicts de resultado."""
        resultados = []
        excerpts = gazette.get("excerpts") or []
        if not excerpts:
            # Sem excerto mas encontrou — registra como menção sem contexto
            excerpts = ["(Menção encontrada — acesse o documento para detalhes)"]

        url = normalizar_url(gazette.get("url", ""))
        data_str = gazette.get("date", "")
        territory = gazette.get("territory_name", "")
        state = gazette.get("state_code", "")
        data_pub = parse_data(data_str)

        for excerto in excerpts[:3]:  # máx 3 excertos por edição
            titulo = f"Diário Oficial — {territory}/{state} — {data_str}"
            trecho = strip_html_tags(excerto)
            resultados.append({
                "fonte_busca": subtipo_fonte,
                "site": "queridodiario.ok.org.br",
                "titulo": titulo,
                "url": url,
                "data_publicacao": data_str,
                "data_publicacao_dt": data_pub,
                "trecho": trecho,
                "query_usada": query,
            })
        return resultados

    def buscar_por_cnpj(self, cnpj: str) -> List[Dict]:
        """
        Busca por CNPJ nos Diários Oficiais.
        Alta precisão: CNPJ é único, não gera falso positivo.
        Busca sem restrição de território para máxima cobertura.

        Formatos tentados (diários publicam de formas variadas):
          XX.XXX.XXX/XXXX-XX  (formatado padrão RFB)
          XXXXXXXXXXXXXX      (só dígitos)
        """
        digitos = re.sub(r"\D", "", cnpj or "")
        if len(digitos) != 14:
            return []

        cnpj_fmt = formatar_cnpj(digitos)
        data_inicio = (datetime.now() - timedelta(days=DIAS_JANELA_DIARIO_CNPJ)).strftime("%Y-%m-%d")
        resultados = []

        for query in [cnpj_fmt, digitos]:
            payload = self._request({
                "querystring": query,
                "published_since": data_inicio,   # parâmetro correto da API
                "page_size": 20,
                "excerpt_size": 500,              # excertos maiores = mais contexto
            })
            if payload and payload.get("gazettes"):
                for gazette in payload["gazettes"]:
                    resultados.extend(
                        self._gazette_para_resultado(gazette, query, "DIARIO_OFICIAL_CNPJ")
                    )
                break  # encontrou com formatado, não repete sem formato
            time.sleep(0.5)

        return resultados

    def buscar_por_nome(
        self, nome: str, territory_ids: Optional[str] = TERRITORIO_SALVADOR
    ) -> List[Dict]:
        """
        Busca por nome da empresa nos Diários Oficiais.
        Usa 'territory_ids' (plural) conforme parâmetro real da API.
        Por padrão restringe a Salvador; passa None para busca nacional.
        """
        if not nome or len(nome) < 5:
            return []

        data_inicio = (datetime.now() - timedelta(days=DIAS_JANELA_DIARIO_NOME)).strftime("%Y-%m-%d")
        params: Dict = {
            "querystring": nome,
            "published_since": data_inicio,       # parâmetro correto da API
            "page_size": 10,
            "excerpt_size": 500,
        }
        if territory_ids:
            params["territory_ids"] = territory_ids  # plural conforme spec da API

        payload = self._request(params)
        if not payload or not payload.get("gazettes"):
            return []

        resultados = []
        for gazette in payload["gazettes"]:
            resultados.extend(
                self._gazette_para_resultado(gazette, nome, "DIARIO_OFICIAL_NOME")
            )
        return resultados

    def buscar_por_nome_variantes(self, variantes: List[Dict]) -> List[Dict]:
        """
        Busca no Querido Diário para múltiplas variantes do nome.
        Tenta Salvador primeiro; se não achar, tenta sem filtro de território.
        Retorna no máximo 15 resultados únicos por empresa.
        """
        resultados: List[Dict] = []
        hashes: Set[str] = set()
        # Usa até 3 variantes para não sobrecarregar a API
        for variante in variantes[:3]:
            nome = variante["nome"]
            # Busca restrita a Salvador
            res = self.buscar_por_nome(nome, territory_ids=TERRITORIO_SALVADOR)
            if not res:
                # Sem resultado local — tenta nacional (sem territory_ids)
                time.sleep(0.3)
                res = self.buscar_por_nome(nome, territory_ids=None)
            for r in res:
                h = hashlib.md5((r.get("url", "") + r.get("trecho", "")).encode()).hexdigest()
                if h not in hashes:
                    hashes.add(h)
                    resultados.append(r)
            if len(resultados) >= 15:
                break
            time.sleep(0.5)
        return resultados


# =========================================================
# FONTE 2: GOOGLE NEWS RSS (REESCRITO)
# =========================================================

class BuscadorGoogleNews:
    """
    Google News RSS com estratégia de query completamente reformulada.

    Diferenças da V1:
      - Gera múltiplas queries por empresa (não uma única por query de banco)
      - Usa aspas apenas quando necessário (nomes longos/específicos)
      - Adiciona contexto "Salvador" para nomes curtos (reduz ruído)
      - Janela de 30 dias em vez de 3
      - Não tenta resolver URLs do Google News (processo frágil da V1) —
        usa título e trecho diretamente para análise imediata
    """

    def _url_gnews(self, query: str, dias: int) -> str:
        q = quote_plus(f"{query} when:{dias}d")
        return f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-419"

    def _parsear(self, feed_url: str, dias: int) -> List[Dict]:
        resultados = []
        try:
            parsed = feedparser.parse(feed_url)
            for entry in getattr(parsed, "entries", []):
                titulo = normalize_text(getattr(entry, "title", ""))
                trecho = strip_html_tags(
                    getattr(entry, "summary", "") or getattr(entry, "description", "")
                )
                link = normalizar_url(getattr(entry, "link", ""))
                pub_raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
                data_pub = parse_data(pub_raw)

                if not eh_recente(data_pub, dias):
                    continue

                resultados.append({
                    "fonte_busca": "GOOGLE_NEWS",
                    "site": extrair_site(link),
                    "titulo": titulo,
                    "url": link,
                    "data_publicacao": pub_raw or "",
                    "data_publicacao_dt": data_pub,
                    "trecho": trecho,
                })
        except Exception as e:
            log(f"  [GoogleNews] Erro ao parsear: {e}")
        return resultados

    def buscar_variantes(self, variantes: List[Dict], dias: int = DIAS_JANELA_NOTICIAS) -> List[Dict]:
        """Busca todas as variantes de nome e consolida resultados únicos."""
        todos: List[Dict] = []
        hashes_vistos: Set[str] = set()

        for variante in variantes:
            nome = variante["nome"]
            # Monta query: com aspas se indicado, com "Salvador" se nome ambíguo
            if variante["com_aspas"]:
                q = f'"{nome}"'
            else:
                q = nome
            if variante.get("adicionar_ba"):
                q = f"{q} Salvador"

            url = self._url_gnews(q, dias)
            resultados = self._parsear(url, dias)

            for r in resultados:
                h = hash_noticia(r["url"], r["titulo"])
                if h not in hashes_vistos:
                    hashes_vistos.add(h)
                    r["query_usada"] = nome
                    todos.append(r)

            time.sleep(0.4)  # Evita bloqueio por rate limit

        return todos


# =========================================================
# FONTE 3: RSS LOCAL (BAHIA + NACIONAIS)
# =========================================================

class BuscadorRSSLocal:
    """
    Carrega feeds RSS uma vez por execução e faz busca local.
    Inclui fontes locais da Bahia/Salvador ausentes na V1.
    """

    def _carregar_feed(self, feed_url: str, dias: int) -> List[Dict]:
        resultados = []
        # Identifica se é fonte local (bahia)
        is_local = any(x in feed_url for x in [
            "correio", "atarde", "bahia", "tribuna", "acordacidade"
        ])
        fonte = "RSS_LOCAL_BA" if is_local else "RSS_NACIONAL"

        try:
            parsed = feedparser.parse(feed_url)
            for entry in getattr(parsed, "entries", []):
                titulo = normalize_text(getattr(entry, "title", ""))
                trecho = strip_html_tags(
                    getattr(entry, "summary", "") or getattr(entry, "description", "")
                )
                link = normalizar_url(getattr(entry, "link", ""))
                pub_raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
                data_pub = parse_data(pub_raw)

                if not eh_recente(data_pub, dias):
                    continue

                resultados.append({
                    "fonte_busca": fonte,
                    "site": extrair_site(link),
                    "titulo": titulo,
                    "titulo_norm": normalize_lower(titulo),
                    "url": link,
                    "data_publicacao": pub_raw or "",
                    "data_publicacao_dt": data_pub,
                    "trecho": trecho,
                    "trecho_norm": normalize_lower(trecho),
                })
        except Exception as e:
            log(f"  [RSS] Falha em {feed_url}: {e}")
        return resultados

    def carregar_todos(self, dias: int = DIAS_JANELA_NOTICIAS) -> pd.DataFrame:
        """Carrega todos os feeds em paralelo. Chamado uma vez por execução."""
        todos: List[Dict] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_RSS) as executor:
            futures = {
                executor.submit(self._carregar_feed, url, dias): url
                for url in RSS_FEEDS_TODOS
            }
            for future in as_completed(futures):
                try:
                    todos.extend(future.result())
                except Exception as e:
                    log(f"  [RSS] Erro paralelo: {e}")

        if not todos:
            return pd.DataFrame()

        df = pd.DataFrame(todos)
        df["_hash"] = df.apply(
            lambda r: hash_noticia(r.get("url", ""), r.get("titulo", "")), axis=1
        )
        df = df.drop_duplicates(subset=["_hash"]).drop(columns=["_hash"]).reset_index(drop=True)

        locais = (df["fonte_busca"] == "RSS_LOCAL_BA").sum()
        nacionais = (df["fonte_busca"] == "RSS_NACIONAL").sum()
        log(f"  [RSS] Carregados: {locais} locais (BA) + {nacionais} nacionais = {len(df)} artigos")
        return df

    def buscar(
        self, df_rss: pd.DataFrame, variantes: List[Dict], max_por_variante: int = 5
    ) -> List[Dict]:
        """Busca no cache RSS por todas as variantes de nome."""
        if df_rss.empty:
            return []

        resultados: List[Dict] = []
        hashes_vistos: Set[str] = set()

        for variante in variantes:
            termo = normalize_lower(variante["nome"])
            if not termo:
                continue

            # Busca no título e no trecho
            mask = (
                df_rss["titulo_norm"].str.contains(re.escape(termo), regex=True, na=False) |
                df_rss["trecho_norm"].str.contains(re.escape(termo), regex=True, na=False)
            )
            matches = df_rss[mask].head(max_por_variante)

            for _, row in matches.iterrows():
                h = hash_noticia(row.get("url", ""), row.get("titulo", ""))
                if h not in hashes_vistos:
                    hashes_vistos.add(h)
                    r = {k: v for k, v in row.items() if k not in ("titulo_norm", "trecho_norm")}
                    r["query_usada"] = variante["nome"]
                    resultados.append(r)

        return resultados


# =========================================================
# FONTE 4: GDELT
# =========================================================

class BuscadorGDELT:
    """
    GDELT Project — base global de notícias, gratuita e sem autenticação.
    Cobre portais brasileiros em português.
    Mais eficaz para empresas com cobertura nacional/regional.

    Documentação: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
    """

    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def buscar(self, query: str, dias: int = DIAS_JANELA_NOTICIAS) -> List[Dict]:
        if not USAR_GDELT or not query:
            return []

        agora = datetime.now()
        inicio = agora - timedelta(days=dias)
        params = {
            "query": f"{query} sourcelang:Portuguese",
            "mode": "ArtList",
            "maxrecords": 10,
            "startdatetime": inicio.strftime("%Y%m%d%H%M%S"),
            "enddatetime": agora.strftime("%Y%m%d%H%M%S"),
            "format": "json",
            "sort": "DateDesc",
        }

        try:
            resp = requests.get(
                self.BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT_HTTP
            )
            if resp.status_code != 200:
                return []

            artigos = resp.json().get("articles") or []
            resultados = []
            for art in artigos:
                url = normalizar_url(art.get("url", ""))
                titulo = normalize_text(art.get("title", ""))
                data_str = art.get("seendate", "")
                # Formato GDELT: YYYYMMDDTHHMMSSZ
                try:
                    data_pub = datetime.strptime(data_str[:15], "%Y%m%dT%H%M%S").replace(
                        tzinfo=timezone.utc
                    )
                except Exception:
                    data_pub = None

                resultados.append({
                    "fonte_busca": "GDELT",
                    "site": extrair_site(url),
                    "titulo": titulo,
                    "url": url,
                    "data_publicacao": data_str,
                    "data_publicacao_dt": data_pub,
                    "trecho": normalize_text(art.get("url", "")),  # GDELT não retorna trecho
                    "query_usada": query,
                })
            return resultados

        except Exception as e:
            log(f"  [GDELT] Erro para '{query}': {e}")
            return []


# =========================================================
# FILTRO DE RELEVÂNCIA
# =========================================================

class FiltroRelevancia:
    """
    Verifica se um resultado realmente se refere à empresa buscada.

    Por que isso é necessário:
      - Nomes curtos como "Nordeste" ou "Bahia" podem aparecer em
        qualquer notícia sem relação com a empresa.
      - O filtro calcula a sobreposição de tokens entre o nome da empresa
        e o texto da notícia, eliminando resultados espúrios.
    """

    def _tokenizar(self, texto: str) -> Set[str]:
        return set(re.findall(r"\w{3,}", strip_accents(normalize_lower(texto))))

    def _tokens_significativos(self, nome: str) -> Set[str]:
        """Tokens do nome sem stopwords."""
        todos = self._tokenizar(nome)
        stops = {s.lower() for s in STOPWORDS_NOME}
        return {t for t in todos if t not in stops}

    def calcular_overlap(self, nome: str, texto: str) -> float:
        """Fração dos tokens significativos do nome que aparecem no texto."""
        tokens_nome = self._tokens_significativos(nome)
        if not tokens_nome:
            return 0.0
        tokens_texto = self._tokenizar(texto)
        return len(tokens_nome & tokens_texto) / len(tokens_nome)

    def e_relevante(self, item: Dict, variantes: List[Dict]) -> bool:
        """
        Retorna True se o resultado provavelmente é sobre a empresa.
        Diários Oficiais sempre passam (busca já foi precisa).

        Estratégia por comprimento do nome da empresa:
          - 1 palavra  : exige presença no título E contexto geográfico (Salvador/Bahia)
          - 2 palavras : presença exata no título
          - 3+ palavras: sobreposição de tokens >= 50% no texto completo
        """
        if "DIARIO_OFICIAL" in item.get("fonte_busca", ""):
            return True  # Diários Oficiais: busca já foi precisa por CNPJ/nome

        titulo = item.get("titulo", "")
        trecho = item.get("trecho", "")
        texto_completo = titulo + " " + trecho
        texto_lower = normalize_lower(texto_completo)
        titulo_lower = normalize_lower(titulo)

        CONTEXTO_GEO = ("salvador", "bahia", " ba ", "sefaz")

        for variante in variantes:
            nome = variante["nome"]
            nome_lower = normalize_lower(nome)
            palavras = nome.split()
            qtd = len(palavras)

            # ── caso: nome de 1 palavra (ex: "Nordeste") ──────────────────────
            # Muito ambíguo: exige que o nome esteja no título E que haja
            # algum indicador geográfico no texto para reduzir ruído.
            if qtd == 1:
                geo = any(g in texto_lower for g in CONTEXTO_GEO)
                if geo and (nome_lower in titulo_lower):
                    return True
                continue  # não cai nos casos abaixo

            # ── caso: nome de 2 palavras (ex: "Posto Silva") ──────────────────
            # Presença exata no título já é indicativo suficiente.
            if qtd == 2:
                if nome_lower in titulo_lower:
                    return True
                continue  # não cai no caso abaixo

            # ── caso: nome de 3+ palavras (ex: "Supermercados Nordeste") ──────
            # Sobreposição de tokens: >= 50% das palavras significativas presentes.
            if self.calcular_overlap(nome, texto_completo) >= 0.5:
                return True

        return False

    def calcular_score(self, item: Dict, variantes: List[Dict]) -> int:
        """Score de prioridade: maior = mais relevante."""
        score = 0
        fonte = item.get("fonte_busca", "")
        data_pub = item.get("data_publicacao_dt")
        titulo = item.get("titulo", "")
        trecho = item.get("trecho", "")

        # Recência
        if eh_hoje(data_pub):
            score += 50
        elif eh_recente(data_pub, 3):
            score += 30
        elif eh_recente(data_pub, 7):
            score += 20
        elif eh_recente(data_pub, 30):
            score += 10

        # Valor da fonte
        if "DIARIO_OFICIAL" in fonte:
            score += 40  # Fonte mais valiosa para inteligência fiscal
        elif fonte == "RSS_LOCAL_BA":
            score += 20  # Fonte local relevante
        elif fonte == "RSS_NACIONAL":
            score += 15
        elif fonte == "GOOGLE_NEWS":
            score += 10
        elif fonte == "GDELT":
            score += 8

        # Qualidade do match no título
        for variante in variantes[:2]:
            if normalize_lower(variante["nome"]) in normalize_lower(titulo):
                score += 20
                break

        # Palavras sensíveis para inteligência fiscal
        sensiveis = encontrar_palavras_sensiveis(f"{titulo} {trecho}")
        if sensiveis:
            score += min(len(sensiveis) * 5, 25)

        return score


# =========================================================
# MOTOR PRINCIPAL
# =========================================================

class MotorBusca:
    """
    Processa cada empresa em todas as fontes e consolida resultados.

    Mudança arquitetural central da V2:
    - V1 processava cada "query de banco" independentemente.
    - V2 processa cada EMPRESA, gera variantes inteligentes de nome,
      busca em todas as fontes e filtra por relevância.
    Isso elimina redundâncias e permite estratégias por empresa.
    """

    def __init__(self):
        self.proc_nomes = ProcessadorNomes()
        self.buscador_diario = BuscadorQueridoDiario()
        self.buscador_google = BuscadorGoogleNews()
        self.buscador_gdelt = BuscadorGDELT()
        self.filtro = FiltroRelevancia()
        self.buscador_rss = BuscadorRSSLocal()
        self.df_rss: Optional[pd.DataFrame] = None

    def carregar_rss(self):
        """Carrega todos os feeds RSS uma vez antes do processamento."""
        log("Carregando feeds RSS (locais BA + nacionais)...")
        self.df_rss = self.buscador_rss.carregar_todos(dias=DIAS_JANELA_NOTICIAS)

    def _adicionar_unico(
        self, lista: List[Dict], novos: List[Dict], hashes: Set[str]
    ) -> int:
        """Adiciona resultados novos (sem duplicatas) à lista. Retorna quantos foram adicionados."""
        adicionados = 0
        for r in novos:
            h = hash_noticia(r.get("url", ""), r.get("titulo", ""))
            if h not in hashes:
                hashes.add(h)
                lista.append(r)
                adicionados += 1
        return adicionados

    def processar_empresa(self, empresa: Dict) -> List[Dict]:
        """
        Pipeline completo para uma empresa:
        1. Gera variantes de nome
        2. Busca em cada fonte
        3. Filtra por relevância
        4. Calcula score e enriquece com metadados da empresa
        """
        razao_social = normalize_text(empresa.get("razao_social", ""))
        nome_fantasia = normalize_text(empresa.get("nome_fantasia", ""))
        cnpj = normalize_text(empresa.get("cnpj", ""))
        cnpj_base = normalize_text(empresa.get("cnpj_base", ""))
        id_empresa = normalize_text(empresa.get("id_empresa_alvo", ""))

        log(f"\n  [{id_empresa}] {razao_social}")
        if nome_fantasia:
            log(f"  Fantasia: {nome_fantasia}")

        # Gera variantes de nome
        variantes = self.proc_nomes.gerar_variantes(razao_social, nome_fantasia or None)
        if not variantes:
            log(f"  → Nenhuma variante útil gerada. Empresa ignorada.")
            return []

        log(f"  Variantes: {[v['nome'] for v in variantes]}")

        todos: List[Dict] = []
        hashes_vistos: Set[str] = set()

        # ---- FONTE 1: Querido Diário por CNPJ -------------------------
        if cnpj:
            log(f"  → [1/4] Querido Diário (CNPJ: {formatar_cnpj(cnpj)})...")
            resultados_cnpj = self.buscador_diario.buscar_por_cnpj(cnpj)
            n = self._adicionar_unico(todos, resultados_cnpj, hashes_vistos)
            log(f"        {n} novo(s)")

        # ---- FONTE 1b: Querido Diário por nome (múltiplas variantes) ---
        if variantes:
            nomes_log = [v["nome"] for v in variantes[:3]]
            log(f"  → [1b/4] Querido Diário (nomes: {nomes_log})...")
            resultados_qd_nome = self.buscador_diario.buscar_por_nome_variantes(variantes)
            n = self._adicionar_unico(todos, resultados_qd_nome, hashes_vistos)
            log(f"          {n} novo(s)")

        # ---- FONTE 2: RSS local (Bahia) + nacional --------------------
        if self.df_rss is not None and not self.df_rss.empty:
            log(f"  → [2/4] RSS (local + nacional)...")
            resultados_rss = self.buscador_rss.buscar(self.df_rss, variantes)
            n = self._adicionar_unico(todos, resultados_rss, hashes_vistos)
            log(f"          {n} novo(s)")

        # ---- FONTE 3: Google News (múltiplas variantes) ---------------
        log(f"  → [3/4] Google News ({len(variantes)} variante(s))...")
        resultados_gn = self.buscador_google.buscar_variantes(variantes)
        n = self._adicionar_unico(todos, resultados_gn, hashes_vistos)
        log(f"          {n} novo(s)")

        # ---- FONTE 4: GDELT (variante principal) ----------------------
        if variantes:
            nome_gdelt = variantes[0]["nome"]
            log(f"  → [4/4] GDELT ('{nome_gdelt}')...")
            resultados_gdelt = self.buscador_gdelt.buscar(nome_gdelt)
            n = self._adicionar_unico(todos, resultados_gdelt, hashes_vistos)
            log(f"          {n} novo(s)")

        log(f"  Total bruto: {len(todos)} | Aplicando filtro de relevância...")

        # ---- Filtro de relevância -------------------------------------
        relevantes = [
            r for r in todos
            if self.filtro.e_relevante(r, variantes)
        ]
        log(f"  Após filtro: {len(relevantes)} resultado(s)")

        # ---- Monta linhas finais com metadados da empresa -------------
        variante_principal = variantes[0]["nome"] if variantes else razao_social
        linhas = []

        for item in relevantes:
            titulo = normalize_text(item.get("titulo", ""))
            trecho = normalize_text(item.get("trecho", ""))
            url = normalizar_url(item.get("url", ""))
            site = normalize_text(item.get("site", "")) or extrair_site(url)
            fonte = normalize_text(item.get("fonte_busca", ""))
            data_pub_dt = garantir_tz(item.get("data_publicacao_dt"))
            data_pub_raw = normalize_text(item.get("data_publicacao", ""))
            query_usada = normalize_text(item.get("query_usada", variante_principal))
            score = self.filtro.calcular_score(item, variantes)
            sensiveis = encontrar_palavras_sensiveis(f"{titulo} {trecho}")

            linhas.append({
                "data_execucao": DATA_EXECUCAO.strftime("%Y-%m-%d %H:%M:%S"),
                "id_consulta": f"{id_empresa}_v2",
                "id_empresa_alvo": id_empresa,
                "cnpj_base": cnpj_base,
                "cnpj": cnpj,
                "razao_social": razao_social,
                "nome_fantasia": nome_fantasia,
                "tipo_origem": "EMPRESA",
                "subtipo_consulta": "MULTI_FONTE_V2",
                "consulta_texto": query_usada,
                "consulta_texto_limpa": query_usada,
                "entidade_referencia": razao_social,
                "fonte_busca": fonte,
                "site": site,
                "titulo": titulo,
                "url": url,
                "data_publicacao": data_pub_raw,
                "data_publicacao_normalizada": (
                    data_pub_dt.strftime("%Y-%m-%d %H:%M:%S") if data_pub_dt else ""
                ),
                "data_publicacao_dia": (
                    data_pub_dt.date().isoformat() if data_pub_dt else ""
                ),
                "publicado_hoje": "S" if eh_hoje(data_pub_dt) else "N",
                "recente_ate_7_dias": "S" if eh_recente(data_pub_dt, 7) else "N",
                "trecho": trecho,
                "tipo_match": "MULTI_FONTE_V2",
                "termo_encontrado": query_usada,
                "palavra_sensivel_encontrada": " | ".join(sensiveis),
                "score_prioridade": score,
                "duplicado": "N",
                "hash_noticia": hash_noticia(url, titulo),
            })

        # Ordena por score e aplica limite por empresa
        linhas.sort(key=lambda x: x["score_prioridade"], reverse=True)
        return linhas[:MAX_RESULTADOS_POR_EMPRESA]


# =========================================================
# BANCO DE DADOS
# =========================================================

def build_pg_url(cfg: dict) -> str:
    return (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )


def get_engine():
    return create_engine(
        build_pg_url(pg_config_saida),
        pool_pre_ping=True,
        connect_args={"connect_timeout": 15},
    )


DDL_FASE2 = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_DESTINO};

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_EXECUCOES} (
    id_execucao text PRIMARY KEY,
    ano_referencia integer,
    origem_monitoramento text,
    data_execucao timestamp,
    data_execucao_dia date,
    status_execucao text,
    total_consultas integer,
    total_consultas_validas integer,
    total_resultados integer,
    consultas_publicas_restantes integer,
    metricas_json text,
    observacao text
);

CREATE TABLE IF NOT EXISTS {SCHEMA_DESTINO}.{TB_RESULTADOS} (
    id_resultado text PRIMARY KEY,
    id_execucao text,
    ano_referencia integer,
    origem_monitoramento text,
    data_execucao timestamp,
    id_consulta text,
    id_empresa_alvo text,
    cnpj_base text,
    cnpj text,
    razao_social text,
    nome_fantasia text,
    tipo_origem text,
    subtipo_consulta text,
    consulta_texto text,
    consulta_texto_limpa text,
    entidade_referencia text,
    fonte_busca text,
    site text,
    titulo text,
    url text,
    data_publicacao text,
    data_publicacao_normalizada timestamp,
    data_publicacao_dia date,
    publicado_hoje text,
    recente_ate_7_dias text,
    trecho text,
    tipo_match text,
    termo_encontrado text,
    palavra_sensivel_encontrada text,
    score_prioridade double precision,
    duplicado text,
    hash_noticia text
);

CREATE INDEX IF NOT EXISTS ix_noticias_resultados_exec
    ON {SCHEMA_DESTINO}.{TB_RESULTADOS}(id_execucao);
CREATE INDEX IF NOT EXISTS ix_noticias_resultados_hash
    ON {SCHEMA_DESTINO}.{TB_RESULTADOS}(hash_noticia);
CREATE INDEX IF NOT EXISTS ix_noticias_resultados_empresa
    ON {SCHEMA_DESTINO}.{TB_RESULTADOS}(id_empresa_alvo);
"""


def ensure_tables(engine) -> None:
    with engine.begin() as conn:
        for stmt in [s.strip() for s in DDL_FASE2.split(";") if s.strip()]:
            conn.execute(text(stmt))


def carregar_empresas(engine) -> pd.DataFrame:
    """Carrega empresas-alvo diretamente da tabela montada pela Fase 1."""
    sql = text(f"""
        SELECT *
        FROM {SCHEMA_DESTINO}.{TB_EMPRESAS_ALVO}
        WHERE ano_referencia = :ano
          AND origem_monitoramento = :origem
          AND COALESCE(monitorar, 'S') = 'S'
        ORDER BY id_empresa_alvo
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={
            "ano": ANO_REFERENCIA,
            "origem": ORIGEM_MONITORAMENTO,
        })


def persistir_resultados(engine, df: pd.DataFrame, id_execucao: str) -> None:
    if df.empty:
        return
    df = df.copy()
    df.insert(0, "id_resultado", [str(uuid4()) for _ in range(len(df))])
    df.insert(1, "id_execucao", id_execucao)
    df.insert(2, "ano_referencia", ANO_REFERENCIA)
    df.insert(3, "origem_monitoramento", ORIGEM_MONITORAMENTO)
    df["data_execucao"] = pd.to_datetime(df["data_execucao"], errors="coerce")
    df["data_publicacao_normalizada"] = pd.to_datetime(
        df["data_publicacao_normalizada"], errors="coerce"
    )
    df["data_publicacao_dia"] = pd.to_datetime(
        df["data_publicacao_dia"], errors="coerce"
    ).dt.date
    df.to_sql(
        TB_RESULTADOS, engine, schema=SCHEMA_DESTINO,
        if_exists="append", index=False, method="multi", chunksize=500,
    )


def persistir_execucao(engine, row: dict) -> None:
    pd.DataFrame([row]).to_sql(
        TB_EXECUCOES, engine, schema=SCHEMA_DESTINO,
        if_exists="append", index=False,
    )


# =========================================================
# MAIN
# =========================================================

def main():
    inicio = time.time()
    id_execucao = str(uuid4())
    engine = get_engine()
    ensure_tables(engine)

    log("=" * 65)
    log("FASE 2 V2 — Motor de busca multi-fonte | Contribuintes Suporte")
    log(f"Janela notícias: {DIAS_JANELA_NOTICIAS}d | Diários Oficiais: {DIAS_JANELA_DIARIO_CNPJ}d")
    log(f"Fontes: Querido Diário · RSS BA/Nacional · Google News · GDELT")
    log("=" * 65)

    # Carrega empresas-alvo
    df_empresas = carregar_empresas(engine)
    log(f"\nEmpresas monitoradas: {len(df_empresas):,}")

    if df_empresas.empty:
        log("AVISO: Nenhuma empresa encontrada. Execute a Fase 1 primeiro.")
        persistir_execucao(engine, {
            "id_execucao": id_execucao,
            "ano_referencia": ANO_REFERENCIA,
            "origem_monitoramento": ORIGEM_MONITORAMENTO,
            "data_execucao": DATA_EXECUCAO,
            "data_execucao_dia": DATA_EXECUCAO.date(),
            "status_execucao": "SEM_EMPRESAS",
            "total_consultas": 0,
            "total_consultas_validas": 0,
            "total_resultados": 0,
            "consultas_publicas_restantes": 0,
            "metricas_json": "{}",
            "observacao": "Nenhuma empresa encontrada na tabela de entrada",
        })
        return

    # Dedup por cnpj_base: processa cada grupo econômico uma vez
    df_proc = df_empresas.drop_duplicates(subset=["cnpj_base"], keep="first").reset_index(drop=True)
    log(f"Grupos únicos por cnpj_base: {len(df_proc):,}\n")

    # Inicializa motor e carrega RSS uma única vez
    motor = MotorBusca()
    motor.carregar_rss()

    todos_resultados: List[Dict] = []
    total = len(df_proc)
    empresas_com_resultado = 0

    for i, (_, row) in enumerate(df_proc.iterrows(), start=1):
        log(f"\n[{i}/{total}] Processando:")
        try:
            resultados = motor.processar_empresa(row.to_dict())
            if resultados:
                todos_resultados.extend(resultados)
                empresas_com_resultado += 1
        except Exception as e:
            log(f"  ERRO inesperado: {e}")
            import traceback
            log(traceback.format_exc())

    # Monta DataFrame final
    df_final = pd.DataFrame(todos_resultados) if todos_resultados else pd.DataFrame()

    if df_final.empty:
        log("\nNenhum resultado encontrado.")
        pd.DataFrame().to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8-sig")
    else:
        # Deduplicação global por hash
        df_final = (
            df_final
            .sort_values("score_prioridade", ascending=False)
            .drop_duplicates(subset=["hash_noticia"])
            .reset_index(drop=True)
        )

        log(f"\n{'=' * 50}")
        log(f"RESULTADO FINAL: {len(df_final):,} notícias únicas")
        log(f"Empresas com ao menos 1 resultado: {empresas_com_resultado}/{total}")
        log(f"\nDistribuição por fonte:")
        for fonte, cnt in df_final["fonte_busca"].value_counts().items():
            log(f"  {fonte:30s}: {cnt:4d}")

        df_final.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8-sig")
        log(f"\nCSV exportado: {ARQUIVO_SAIDA}")

        persistir_resultados(engine, df_final, id_execucao)
        log(f"Resultados gravados no banco: {len(df_final):,} registros")

    metricas = {
        "versao": "v2",
        "fontes": ["QUERIDO_DIARIO_CNPJ", "QUERIDO_DIARIO_NOME", "RSS_LOCAL_BA",
                   "RSS_NACIONAL", "GOOGLE_NEWS", "GDELT"],
        "dias_noticias": DIAS_JANELA_NOTICIAS,
        "dias_diario_oficial": DIAS_JANELA_DIARIO_CNPJ,
        "empresas_processadas": total,
        "empresas_com_resultado": empresas_com_resultado,
        "total_resultados": len(df_final) if not df_final.empty else 0,
    }

    persistir_execucao(engine, {
        "id_execucao": id_execucao,
        "ano_referencia": ANO_REFERENCIA,
        "origem_monitoramento": ORIGEM_MONITORAMENTO,
        "data_execucao": DATA_EXECUCAO,
        "data_execucao_dia": DATA_EXECUCAO.date(),
        "status_execucao": "SUCESSO",
        "total_consultas": total,
        "total_consultas_validas": total,
        "total_resultados": len(df_final) if not df_final.empty else 0,
        "consultas_publicas_restantes": 0,
        "metricas_json": json.dumps(metricas, ensure_ascii=False),
        "observacao": f"V2 multi-fonte | {DIAS_JANELA_NOTICIAS}d noticias | {DIAS_JANELA_DIARIO_CNPJ}d DO",
    })

    log(f"\nTempo total: {time.time() - inicio:.1f}s")
    log("Execução concluída.")


if __name__ == "__main__":
    main()
