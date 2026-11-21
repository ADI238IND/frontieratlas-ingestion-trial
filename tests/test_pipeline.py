from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from openpyxl import load_workbook

from frontieratlas.bulk import paper_identity, repo_path
from frontieratlas.core import Resolver, Store, canonical_url, iso, parse_date, record
from frontieratlas.export import publish
from frontieratlas.http import FetchError, Response, retry_delay
from frontieratlas.llm import Orchestrator, Provider, chunks, fit, grounding
from frontieratlas.signals import article, classify, parse_page

NOW = datetime(2026, 11, 21, 4, tzinfo=UTC)


class QualityTests(unittest.TestCase):
    def test_timestamp_at_cutoff(self):
        cutoff = parse_date("2026-11-20T09:30:00+05:30", NOW, "meta")
        self.assertTrue(cutoff.eligible(NOW))
        self.assertFalse(cutoff.eligible(NOW + timedelta(microseconds=1)))

    def test_relative_bounds(self):
        parsed = parse_date("2 hours ago", NOW, "label", relative=True)
        self.assertEqual(parsed.earliest, NOW - timedelta(hours=3))
        self.assertEqual(parsed.latest, NOW - timedelta(hours=2))
        self.assertFalse(parse_date("24 hours ago", NOW, "label", relative=True).eligible(NOW))

    def test_unverified_publication(self):
        self.assertIsNone(parse_date("2026-11-20", NOW, "meta"))
        self.assertIsNone(parse_date("2026-11-20T17:00:00", NOW, "meta"))
        self.assertFalse(parse_date("2026-11-21T04:00:01Z", NOW, "meta").eligible(NOW))

    def test_alias_and_ambiguous_domain(self):
        resolver = Resolver(Path(__file__).resolve().parents[1] / "config" / "aliases.json")
        for name in ("OpenAI", "Open AI", "OpenAI, Inc."):
            self.assertEqual(resolver.resolve(name, "company")[0], "OpenAI")
        resolver.observe_company("Example A", "https://www.example.com/")
        resolver.observe_company("Example B", "https://example.com/")
        self.assertEqual(resolver.vendor("https://example.com/tool"), (None, "ambiguous_domain"))
        self.assertEqual(resolver.resolve("Chat GPT", "product")[0], "ChatGPT")

    def test_stable_source_keys(self):
        self.assertEqual(canonical_url("https://EXAMPLE.com/a?utm_source=x&job=4#candidate"),
                         "https://example.com/a?job=4")
        self.assertEqual(paper_identity({"arxiv_id": "2611.19320v2"}), "2611.19320")
        self.assertEqual(repo_path("https://github.com/owner/project.git"), "owner/project")
        self.assertIsNone(repo_path("https://github.com/owner/project/issues"))

    def test_article_ignores_update_field_and_honors_canonical(self):
        tree = parse_page(b'''<html><head>
        <link rel="canonical" href="https://bad.example/other">
        <meta property="article:modified_time" content="2026-11-21T01:30:00Z">
        <meta property="article:published_time" content="2026-11-20T19:20:00Z">
        </head><body><article><aside>template boilerplate</aside>
        <h2>Training a large model</h2><p>The article provides source evidence for our dataset.</p>
        </article></body></html>''')
        url, dates, text = article(tree, "https://example.com/item", {"example.com"})
        self.assertEqual(url, "https://example.com/item")
        self.assertEqual(dates, [("article:published_time", "2026-11-20T19:20:00Z")])
        self.assertIn("source evidence", text)
        self.assertNotIn("boilerplate", text)
        self.assertEqual(classify("Research Engineer")[0], "Research")

    def test_sheet_dates_idempotence_and_literal_text(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            store = Store(directory / "run.sqlite")
            for identity, date in [("a", NOW - timedelta(hours=1)), ("b", NOW - timedelta(hours=25))]:
                item = record("NEWS", "News source", f"https://example.com/{identity}",
                              {"date": iso(date), "title": "=1+1", "full_text": "body"},
                              {"earliest": iso(date), "latest": iso(date)})
                store.put(identity, item)
                store.put(identity, item)
            self.assertEqual(publish(store, directory, NOW)["News"], 1)
            sheet = load_workbook(directory / "FrontierAtlas_Data.xlsx")["News"]
            self.assertEqual(sheet["G2"].data_type, "s")
            self.assertEqual(sheet["G2"].value, "=1+1")
            self.assertEqual(json.loads((directory / "news.jsonl").read_text())["bodyCharacters"], 4)
            store.db.close()

    def test_retry_after_honored(self):
        self.assertGreaterEqual(retry_delay({"Retry-After": "15"}, 0), 15)

    def test_multibyte_payload_budget(self):
        provider = Provider("groq", "fake", "example", "https://example.com/l", 12, 90000)
        text = ("Model training evidence: " + "🧪" * 120 + "\n") * 70
        parts = chunks(text, "Which Model training note?")
        _, payload, evidence = fit(provider, "Model training note", parts, 3200)
        self.assertTrue(evidence)
        self.assertLessEqual(len(payload) + provider.max_output + 256, 3200)

    def test_quote_must_support_literal_value(self):
        self.assertEqual(grounding({"value": "DeepMind", "quote": "from DeepMind"}, "A note from DeepMind")["value"],
                         "DeepMind")
        with self.assertRaises(ValueError):
            grounding({"value": "Google", "quote": "from DeepMind"}, "A note from DeepMind")


class ScriptedClient:
    def __init__(self):
        self.calls = []

    async def request(self, url, **kw):
        self.calls.append((url, len(kw["data"])))
        seq = len(self.calls)
        if seq == 1:
            raise FetchError(Response(413, url, {}, b"", NOW))
        if seq == 2:
            raise FetchError(Response(429, url, {"Retry-After": "90"}, b"", NOW))
        if seq == 3:
            return Response(200, url, {}, b'{"choices":[{"message":{"content":"bad"},"finish_reason":"stop"}]}', NOW)
        data = {"choices": [{"message": {"content": json.dumps(
            {"value": "Acme Labs", "quote": "Acme Labs builds models"})}, "finish_reason": "stop"}]}
        return Response(200, url, {}, json.dumps(data).encode(), NOW)


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_overflow_shrink_retry_after_and_fallback(self):
        client = ScriptedClient()
        providers = [Provider(name, "fake", "demo", f"https://example.com/{name}", 15, 100000)
                     for name in ("gemini", "groq", "deepseek")]
        stack = Orchestrator(client, providers)
        text = "Acme Labs builds models. " + "\n".join(f"evidence models token {i} " + "x" * 600 for i in range(50))
        extracted = await stack.extract(text, {"entity": "models evidence"})
        self.assertEqual([x[0].rsplit("/", 1)[-1] for x in client.calls], ["gemini", "gemini", "groq", "deepseek"])
        self.assertLess(client.calls[1][1], client.calls[0][1])
        self.assertEqual(extracted["provider"], "deepseek")
        self.assertEqual(extracted["value"], "Acme Labs")
