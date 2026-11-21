from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime
import json
import logging
from pathlib import Path

import aiohttp
from lxml import etree

from .bulk import directories, papers
from .core import Resolver, Store, iso, utcnow
from .export import publish
from .http import Client
from .llm import Orchestrator
from .signals import job_source, news_source

LOG = logging.getLogger(__name__)


async def run(args):
    path = Path(args.db)
    path.parent.mkdir(parents=True, exist_ok=True)
    store = Store(path)
    config = json.loads((Path(args.config) / "sources.json").read_text())
    resolver = Resolver(Path(args.config) / "aliases.json")
    report = {"startedAt": iso(utcnow())}
    async with aiohttp.ClientSession(
        trust_env=True, timeout=aiohttp.ClientTimeout(total=28),
        headers={"User-Agent": "FrontierAtlasTrial/0.1 (dataset ingestion; configured public sources)"}
    ) as session:
        client = Client(session, total=args.concurrency, seconds={"paperswithcode.co": 0.22,
                                                                  "api.github.com": 0.25})
        orchestrator = Orchestrator.configured(client)
        report["llmProvidersConfigured"] = [p.name for p, _ in orchestrator.providers]
        if args.mode in {"all", "directories"}:
            report["directories"] = await directories(client, store, resolver, config, args.target)
        if args.mode in {"all", "signals"}:
            observed = utcnow()
            report["signalSnapshotAt"] = iso(observed)
            async def collect(kind, source):
                try:
                    if kind == "news":
                        output = await news_source(client, store, source, observed, orchestrator)
                    else:
                        output = await job_source(client, store, resolver, source, observed, orchestrator)
                    return {"source": source["name"], "kind": kind, "status": "ok", **output}
                except (aiohttp.ClientError, OSError, RuntimeError, ValueError, KeyError, etree.Error) as exc:
                    LOG.warning("source_failed source=%s error=%s", source["name"], exc)
                    return {"source": source["name"], "kind": kind, "status": "failed", "error": str(exc)}
            report["sources"] = await asyncio.gather(*(
                collect(kind, source) for kind in ("news", "jobs") for source in config[kind]
            ))
        if args.mode in {"all", "papers"}:
            report["paperRun"] = await papers(client, store, config, args.target, args.direct_github, args.page_budget)
    as_of = datetime.fromisoformat(args.as_of) if args.as_of else utcnow()
    if not as_of.tzinfo:
        raise ValueError("--as-of must include a UTC offset")
    report["exportedAsOf"] = iso(as_of)
    report["sheetRows"] = publish(store, args.output, as_of)
    report["productVendorDecisions"] = dict(Counter(
        (row["evidence"]["vendorMatch"] for _, row in store.rows("PRODUCT"))))
    report["paperMetricOrigins"] = dict(Counter(
        (row["evidence"]["githubMetricSource"] for _, row in store.rows("RESEARCH_PAPER"))))
    destination = Path(args.output)
    with (destination / "run_manifest.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(report, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    store.db.close()
    if args.require_targets and any(report["sheetRows"][name] < args.target
                                    for name in ("Startups", "Products", "Research Papers")):
        return 2
    return 0


def main():
    parser = argparse.ArgumentParser(description="Run or export the FrontierAtlas ingestion trial.")
    parser.add_argument("mode", choices=["all", "directories", "signals", "papers", "export"])
    parser.add_argument("--db", default="state/frontieratlas.sqlite")
    parser.add_argument("--config", default="config")
    parser.add_argument("--output", default="output")
    parser.add_argument("--target", type=int, default=1000)
    parser.add_argument("--page-budget", type=int, default=45)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--direct-github", type=int, default=20)
    parser.add_argument("--as-of", help="ISO timestamp for reproducing a prior export's freshness window")
    parser.add_argument("--require-targets", action="store_true")
    args = parser.parse_args()
    if min(args.target, args.page_budget, args.concurrency) <= 0 or args.direct_github < 0:
        parser.error("Targets and concurrency must be positive, with a nonnegative GitHub sample size")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
