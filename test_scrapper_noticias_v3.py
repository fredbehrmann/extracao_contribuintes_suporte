import unittest
import asyncio
from datetime import timezone
from types import SimpleNamespace

import scrapper_noticias_v3 as v3


class V3CoreTests(unittest.TestCase):
    def setUp(self):
        self.settings = v3.Settings(
            year=2026,
            origin="TESTE",
            output_dir=v3.BASE_DIR / "saida_noticias",
        )
        self.entity = v3.Entity(
            entity_id="empresa-1",
            cnpj_base="12345678",
            cnpj="12345678000190",
            legal_name="EMPRESA TESTE LTDA",
            trade_name="MARCA TESTE",
            city="Salvador",
            state="BA",
            partners=["FULANO DE TAL"],
            aliases=["TESTE SERVICOS"],
        )

    def test_url_normalization_removes_tracking_and_orders_query(self):
        result = v3.normalize_url(
            "HTTPS://Example.COM/a/?utm_source=x&b=2&fbclid=y&a=1#fim"
        )
        self.assertEqual(result, "https://example.com/a?a=1&b=2")

    def test_query_plan_includes_cnpj_brand_risk_and_partner(self):
        queries = v3.QueryBuilder(self.settings).build(self.entity)
        kinds = {query.kind for query in queries}
        self.assertIn("CNPJ_FORMATADO", kinds)
        self.assertIn("NOME_FANTASIA", kinds)
        self.assertIn("RISCO_COMPLIANCE", kinds)
        self.assertIn("SOCIO_ADMINISTRADOR", kinds)

    def test_querido_diario_uses_current_endpoint_and_exact_query(self):
        self.assertEqual(
            self.settings.qd_base_url,
            "https://api.queridodiario.org.br/gazettes",
        )
        self.assertEqual(
            v3.escape_simple_query_phrase("12.345.678/0001-90"),
            '"12.345.678/0001\\-90"',
        )

    def test_relevant_risk_article_scores_high(self):
        builder = v3.QueryBuilder(self.settings)
        scorer = v3.RelevanceScorer(self.settings, builder)
        candidate = v3.Candidate(
            source=v3.SOURCE_RSS_LOCAL,
            title="Operação investiga fraude da Marca Teste em Salvador",
            url="https://example.com/noticia",
            published_at=v3.utc_now(),
            summary="A empresa Marca Teste foi alvo de operação policial.",
        )
        result = scorer.assess(
            candidate,
            v3.Article(candidate.url, candidate.url, ""),
            self.entity,
        )
        self.assertTrue(result.relevant)
        self.assertIn("FRAUDE_COMPLIANCE", result.categories)
        self.assertIn("OPERACAO_POLICIAL", result.categories)
        self.assertEqual(result.entity_role, "ALVO")
        self.assertGreaterEqual(result.risk_score, 85)

    def test_regulator_as_agent_is_not_classified_as_risk_target(self):
        regulator = v3.Entity(
            entity_id="bacen",
            cnpj_base="00038166",
            cnpj="00038166000105",
            legal_name="BANCO CENTRAL DO BRASIL",
            trade_name="Banco Central",
            city="Brasilia",
            state="DF",
        )
        scorer = v3.RelevanceScorer(self.settings, v3.QueryBuilder(self.settings))
        candidate = v3.Candidate(
            source=v3.SOURCE_GOOGLE,
            title="Banco Central endurece regras para combater fraudes no Pix",
            url="https://news.google.com/rss/articles/teste",
            published_at=v3.utc_now(),
            summary="A autoridade divulgou novas medidas de prevenção.",
        )
        result = scorer.assess(candidate, v3.Article(candidate.url, candidate.url, ""), regulator)
        self.assertTrue(result.relevant)
        self.assertEqual(result.categories, [])
        self.assertEqual(result.risk_score, 0)
        self.assertEqual(result.risk_level, "INFORMATIVO")
        self.assertEqual(result.entity_role, "AUTORIDADE")

        candidate.title = "Fraude no Pix leva Banco Central a endurecer controles"
        result = scorer.assess(candidate, v3.Article(candidate.url, candidate.url, ""), regulator)
        self.assertEqual(result.categories, [])
        self.assertEqual(result.entity_role, "AUTORIDADE")

    def test_regulator_explicitly_named_as_target_keeps_risk(self):
        regulator = v3.Entity(
            entity_id="bacen",
            cnpj_base="00038166",
            cnpj="00038166000105",
            legal_name="BANCO CENTRAL DO BRASIL",
            trade_name="Banco Central",
            city="Brasilia",
            state="DF",
        )
        scorer = v3.RelevanceScorer(self.settings, v3.QueryBuilder(self.settings))
        candidate = v3.Candidate(
            source=v3.SOURCE_RSS_NACIONAL,
            title="Banco Central é alvo de investigação por fraude",
            url="https://example.com/noticia",
            published_at=v3.utc_now(),
        )
        result = scorer.assess(candidate, v3.Article(candidate.url, candidate.url, ""), regulator)
        self.assertIn("FRAUDE_COMPLIANCE", result.categories)
        self.assertEqual(result.entity_role, "ALVO")
        self.assertGreater(result.risk_score, 0)

    def test_trigger_in_another_sentence_is_not_attributed_to_company(self):
        scorer = v3.RelevanceScorer(self.settings, v3.QueryBuilder(self.settings))
        candidate = v3.Candidate(
            source=v3.SOURCE_RSS_LOCAL,
            title="Marca Teste anuncia novo serviço.",
            url="https://example.com/produto",
            published_at=v3.utc_now(),
            summary="Polícia investiga fraude cometida por outra empresa.",
        )
        result = scorer.assess(candidate, v3.Article(candidate.url, candidate.url, ""), self.entity)
        self.assertTrue(result.relevant)
        self.assertEqual(result.categories, [])
        self.assertEqual(result.risk_score, 0)

    def test_generic_homonym_is_rejected(self):
        generic = v3.Entity(
            entity_id="empresa-2",
            cnpj_base="99999999",
            cnpj="99999999000199",
            legal_name="BAHIA LTDA",
            trade_name="Bahia",
            city="Salvador",
            state="BA",
        )
        scorer = v3.RelevanceScorer(self.settings, v3.QueryBuilder(self.settings))
        candidate = v3.Candidate(
            source=v3.SOURCE_RSS_NACIONAL,
            title="Bahia vence partida pelo campeonato",
            url="https://example.com/esporte",
            published_at=v3.utc_now(),
            summary="Time baiano marcou dois gols.",
        )
        result = scorer.assess(candidate, v3.Article(candidate.url, candidate.url, ""), generic)
        self.assertFalse(result.relevant)

    def test_scattered_company_tokens_do_not_prove_identity(self):
        regulator = v3.Entity(
            entity_id="bacen",
            cnpj_base="00038166",
            cnpj="00038166000105",
            legal_name="BANCO CENTRAL DO BRASIL",
            trade_name="Banco Central",
            city="Brasilia",
            state="DF",
        )
        scorer = v3.RelevanceScorer(self.settings, v3.QueryBuilder(self.settings))
        candidate = v3.Candidate(
            source=v3.SOURCE_RSS_NACIONAL,
            title="Agenda cultural reúne atrações em Feira de Santana",
            url="https://example.com/cultura",
            published_at=v3.utc_now(),
            summary=(
                "O banco da praça receberá atividades. "
                + "x " * 120
                + "Na região central haverá uma mostra afro-brasileira."
            ),
        )
        result = scorer.assess(
            candidate,
            v3.Article(candidate.url, candidate.url, ""),
            regulator,
        )
        self.assertFalse(result.relevant)
        self.assertFalse(result.reasons["identidade_verificada"])

    def test_rss_matching_rejects_tokens_spread_across_document(self):
        regulator = v3.Entity(
            entity_id="bacen",
            cnpj_base="00038166",
            cnpj="00038166000105",
            legal_name="BANCO CENTRAL DO BRASIL",
            trade_name="Banco Central",
            city="Brasilia",
            state="DF",
        )
        queries = v3.QueryBuilder(self.settings).build(regulator)
        text = "O banco abriu cedo. " + "x " * 120 + "A área central recebe cultura brasileira."
        self.assertIsNone(v3.best_matching_query(text, regulator, queries))

    def test_querido_diario_prefers_txt_and_validates_excerpt(self):
        source = v3.QueridoDiarioSource(object(), self.settings)
        gazette = {
            "url": "https://files.example.gov/diario.pdf",
            "txt_url": "https://files.example.gov/diario.txt",
            "date": "2026-10-01",
            "territory_name": "Salvador",
            "state_code": "BA",
            "excerpts": ["A empresa 12.345.678/0001-90 foi intimada."],
        }
        candidates = source._to_candidates(
            [gazette], v3.SOURCE_QD_CNPJ, "12.345.678/0001-90", "CNPJ", self.entity
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].url, "https://files.example.gov/diario.txt")
        self.assertEqual(
            candidates[0].metadata["canonical_url"],
            "https://files.example.gov/diario.pdf",
        )

        gazette["excerpts"] = ["Documento sem identificação da empresa."]
        self.assertEqual(
            source._to_candidates(
                [gazette], v3.SOURCE_QD_CNPJ, "12.345.678/0001-90", "CNPJ", self.entity
            ),
            [],
        )

    def test_feed_parser_applies_incremental_cutoff(self):
        payload = b"""<?xml version='1.0' encoding='UTF-8'?>
        <rss version='2.0'><channel><title>Teste</title><item>
          <title>Marca Teste firma contrato</title>
          <link>https://example.com/item?utm_source=rss</link>
          <description>Contrato administrativo em Salvador.</description>
          <pubDate>Thu, 01 Oct 2026 10:00:00 GMT</pubDate>
        </item></channel></rss>"""
        since = v3.datetime(2026, 9, 30, tzinfo=timezone.utc)
        items = v3.feed_candidates(payload, v3.SOURCE_RSS_LOCAL, since)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].url, "https://example.com/item")

    def test_google_feed_preserves_publisher_metadata(self):
        payload = b"""<?xml version='1.0' encoding='UTF-8'?>
        <rss version='2.0'><channel><title>Google News</title><item>
          <title>Marca Teste firma contrato - Jornal Exemplo</title>
          <link>https://news.google.com/rss/articles/abc?oc=5</link>
          <source url='https://jornal.example.com'>Jornal Exemplo</source>
          <pubDate>Thu, 01 Oct 2026 10:00:00 GMT</pubDate>
        </item></channel></rss>"""
        since = v3.datetime(2026, 9, 30, tzinfo=timezone.utc)
        items = v3.feed_candidates(payload, v3.SOURCE_GOOGLE, since)
        self.assertEqual(items[0].site, "jornal.example.com")
        self.assertEqual(items[0].metadata["publisher_url"], "https://jornal.example.com/")
        self.assertEqual(items[0].metadata["publisher_name"], "Jornal Exemplo")

    def test_google_resolver_only_accepts_expected_publisher(self):
        extractor = v3.ArticleExtractor(object(), self.settings)
        html = """
        <html><body>
          <a href='https://policies.google.com/privacy'>Privacidade</a>
          <a data-url='https://jornal.example.com/noticias/marca-teste'>Matéria</a>
        </body></html>
        """
        resolved = extractor._discover_google_publisher_url(
            html, "https://jornal.example.com/"
        )
        self.assertEqual(resolved, "https://jornal.example.com/noticias/marca-teste")

    def test_rss_partial_feed_failure_is_degraded_but_successful(self):
        catalog = v3.RssCatalog(object(), self.settings)
        catalog.loaded_feeds[v3.SOURCE_RSS_LOCAL] = 2
        catalog.errors[v3.SOURCE_RSS_LOCAL] = ["feed indisponível"]
        outcome = catalog.search(self.entity, v3.SOURCE_RSS_LOCAL, [])
        self.assertTrue(outcome.success)
        self.assertTrue(outcome.degraded)

    def test_per_source_quota_prevents_one_source_from_filling_output(self):
        self.settings.max_results_per_entity = 10
        self.settings.max_results_per_source = 3
        processor = object.__new__(v3.EntityProcessor)
        processor.settings = self.settings
        pairs = [
            (SimpleNamespace(source=v3.SOURCE_GOOGLE), object())
            for _ in range(15)
        ] + [
            (SimpleNamespace(source=v3.SOURCE_RSS_LOCAL), object())
            for _ in range(5)
        ]
        selected = processor._limit_pairs_by_source(pairs)
        source_counts = {
            source: sum(pair[0].source == source for pair in selected)
            for source in {v3.SOURCE_GOOGLE, v3.SOURCE_RSS_LOCAL}
        }
        self.assertEqual(source_counts[v3.SOURCE_GOOGLE], 3)
        self.assertEqual(source_counts[v3.SOURCE_RSS_LOCAL], 3)

    def test_direct_article_is_fetched_before_google_wrapper(self):
        self.settings.max_article_fetch_per_entity = 1
        processor = object.__new__(v3.EntityProcessor)
        processor.settings = self.settings
        processor.query_builder = v3.QueryBuilder(self.settings)
        processor.scorer = v3.RelevanceScorer(self.settings, processor.query_builder)
        google = v3.Candidate(
            source=v3.SOURCE_GOOGLE,
            title="Marca Teste firma contrato",
            url="https://news.google.com/rss/articles/abc",
            published_at=v3.utc_now(),
        )
        direct = v3.Candidate(
            source=v3.SOURCE_RSS_LOCAL,
            title="Marca Teste firma contrato",
            url="https://example.com/noticia",
            published_at=v3.utc_now(),
        )
        selected = processor._select_fetch_candidates([google, direct], self.entity)
        self.assertEqual(selected, [direct])

    def test_schema_migration_contains_separate_risk_fields(self):
        self.assertIn("score_risco integer NOT NULL DEFAULT 0", v3.DDL_V3)
        self.assertIn("papel_entidade text NOT NULL DEFAULT 'CITADA'", v3.DDL_V3)
        self.assertIn("'classificacao_legada', true", v3.DDL_V3)
        self.assertIn("status_triagem = 'REPROCESSAR'", v3.DDL_V3)

    def test_new_assessment_can_downgrade_stale_risk_on_upsert(self):
        import inspect

        source = inspect.getsource(v3.Database._copy_mentions)
        self.assertIn("THEN EXCLUDED.score_risco", source)
        self.assertIn("THEN EXCLUDED.categorias_risco", source)
        self.assertNotIn(
            "unnest({table}.categorias_risco || EXCLUDED.categorias_risco)",
            source,
        )

    def test_exact_content_dedup_preserves_existing_id(self):
        existing = [{
            "id_documento": "existente",
            "url_hash": "url-antiga",
            "hash_conteudo": "conteudo-igual",
            "simhash_hex": "0000000000000000",
            "titulo_normalizado": "titulo",
        }]
        dedup = v3.Deduplicator(self.settings, existing)
        document = v3.DocumentRecord(
            document_id="novo",
            original_url="https://example.com/nova",
            canonical_url="https://example.com/nova",
            url_hash="url-nova",
            source=v3.SOURCE_WEB,
            observed_sources=[v3.SOURCE_WEB],
            site="example.com",
            title="Título",
            normalized_title="titulo",
            summary="",
            body="",
            published_at=None,
            modified_at=None,
            event_date=None,
            first_seen_at=v3.utc_now(),
            last_seen_at=v3.utc_now(),
            content_hash="conteudo-igual",
            simhash_hex="0000000000000000",
            extraction_quality=0,
            metadata={},
        )
        self.assertEqual(dedup.resolve(document), "existente")

    def test_target_query_is_composable_and_parameterized(self):
        captured = {}

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def execute(self, query, params):
                captured["sql"] = query.as_string(None)
                captured["params"] = params

            def fetchmany(self, _size):
                return []

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def cursor(self, **_kwargs):
                return FakeCursor()

            def commit(self):
                return None

        database = v3.Database(self.settings)
        database.connect = lambda: FakeConnection()
        self.assertEqual(list(database.iter_entity_batches(limit=7)), [])
        self.assertEqual(captured["params"], [2026, "TESTE", 7])
        self.assertIn("COALESCE(s.estado, '{}'::jsonb)", captured["sql"])
        self.assertIn('FROM "suporte_arrecadacao"."noticias_empresas_alvo"', captured["sql"])


class V3AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_querido_diario_opens_circuit_after_official_endpoints_fail(self):
        settings = v3.Settings(
            year=2026,
            origin="TESTE",
            output_dir=v3.BASE_DIR / "saida_noticias",
            qd_cooldown_seconds=60.0,
        )

        class FakeHttp:
            def __init__(self):
                self.calls = 0

            async def get_json(self, _url, *, params=None, retries=None):
                self.calls += 1
                raise RuntimeError("timeout simulado")

        fake_http = FakeHttp()
        source = v3.QueridoDiarioSource(fake_http, settings)
        results = await asyncio.gather(
            source._request({"querystring": '"teste"'}),
            source._request({"querystring": '"teste"'}),
            return_exceptions=True,
        )
        self.assertEqual(fake_http.calls, 2)
        self.assertTrue(all(isinstance(result, RuntimeError) for result in results))
        self.assertIn("cooldown", str(results[1]).lower())

    async def test_plain_text_document_is_extracted(self):
        settings = v3.Settings(
            year=2026,
            origin="TESTE",
            output_dir=v3.BASE_DIR / "saida_noticias",
        )

        class FakeHttp:
            async def get_bytes(self, url, **_kwargs):
                response = v3.httpx.Response(
                    200,
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                    request=v3.httpx.Request("GET", url),
                )
                return response, ("Marca Teste " * 40).encode("utf-8")

        article = await v3.ArticleExtractor(FakeHttp(), settings).fetch(
            "https://files.example.gov/diario.txt"
        )
        self.assertGreaterEqual(article.extraction_quality, 90)
        self.assertTrue(article.metadata["plain_text"])
        self.assertIn("Marca Teste", article.body)

    async def test_gdelt_coalesces_concurrent_entities_in_one_request(self):
        settings = v3.Settings(
            year=2026,
            origin="TESTE",
            output_dir=v3.BASE_DIR / "saida_noticias",
            gdelt_interval_seconds=0.0,
        )

        class FakeHttp:
            def __init__(self):
                self.calls = 0
                self.params = None

            async def get_json(self, _url, *, params=None, retries=None):
                self.calls += 1
                self.params = params
                return None, {"articles": [
                    {
                        "title": "Marca Teste vence licitação",
                        "url": "https://example.com/teste",
                        "seendate": "20261001T100000Z",
                        "domain": "example.com",
                    },
                    {
                        "title": "Companhia Alfa anuncia contrato",
                        "url": "https://example.com/alfa",
                        "seendate": "20261001T100000Z",
                        "domain": "example.com",
                    },
                ]}

        first = v3.Entity(
            "1", "12345678", "12345678000190", "EMPRESA TESTE LTDA",
            "Marca Teste", "Salvador", "BA",
        )
        second = v3.Entity(
            "2", "99999999", "99999999000199", "COMPANHIA ALFA LTDA",
            "Companhia Alfa", "Salvador", "BA",
        )
        fake_http = FakeHttp()
        source = v3.GdeltSource(fake_http, settings)
        builder = v3.QueryBuilder(settings)
        outcomes = await asyncio.gather(
            source.search(first, builder.build(first)),
            source.search(second, builder.build(second)),
        )
        self.assertEqual(fake_http.calls, 1)
        self.assertIn(" OR ", fake_http.params["query"])
        self.assertEqual([len(outcome.candidates) for outcome in outcomes], [1, 1])


if __name__ == "__main__":
    unittest.main()
