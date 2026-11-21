from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, date, datetime, timedelta
import json
import logging
import os
import re
from urllib.parse import quote, urlencode, urlsplit

import aiohttp

from .core import canonical_url, clean, company_name_key, domain, iso, name_key, parse_date, record, utcnow
from .http import FetchError

LOG = logging.getLogger(__name__)
AI_TAGS = {"ai", "machine learning", "artificial intelligence", "generative ai", "ai assistant",
           "computer vision", "deep learning", "natural language processing", "large language models",
           "ai agents", "ai infrastructure", "aiops"}
PRICES = {"free": "FREE", "open-source": None, "freemium": "FREEMIUM", "paid": "PAID",
          "contact-sales": "ENTERPRISE", "enterprise": "ENTERPRISE"}


async def directories(client, store, resolver, config, target):
    counts = Counter()
    yc_url = config["startups"]
    try:
        response = await client.request(yc_url)
    except (FetchError, aiohttp.ClientError, TimeoutError, OSError):
        response = await client.request("https://raw.githubusercontent.com/yc-oss/api/main/companies/all.json")
    companies = response.json()
    yc_hash = store.raw(response.url, response.fetched_at, response.body)
    for row in companies:
        if clean(row.get("name")):
            resolver.observe_company(row["name"], row.get("website"), row.get("former_names", []))
    # These domains are maintained independently from the trial's YC startup filter.
    for vendor, website in config.get("vendor_domains", {}).items():
        resolver.observe_company(vendor, website)
    seen_names = set()
    for row in sorted(companies, key=lambda value: (value.get("status") != "Active", value["id"])):
        tags = {clean(tag).casefold() for tag in row.get("tags", [])}
        if not tags & AI_TAGS or row.get("nonprofit") or row.get("status") != "Active":
            continue
        name = clean(row.get("name"))
        source = canonical_url(row.get("url"))
        if not name or not source:
            continue
        resolved, decision = resolver.resolve(name, "company")
        key = company_name_key(resolved)
        if key in seen_names and decision != "ambiguous":
            continue
        seen_names.add(key)
        team = row.get("team_size")
        evidence = {"snapshotUrl": response.url, "rawSha256": yc_hash, "ycId": row["id"]}
        payload = record("STARTUP", "Y Combinator via YC OSS", source, {
            "entityName": resolved, "data": {"employeeCount": team if type(team) is int and team > 0 else None},
            "website": canonical_url(row.get("website")), "batch": row.get("batch"),
            "tags": row.get("tags", []),
        }, evidence)
        store.put(str(row["id"]), payload)
        store.mapping("company", name, payload["source"]["name"], resolved, decision, source)
        counts["startups"] += 1
        if counts["startups"] >= target:
            break

    response = await client.request(config["products"])
    product_hash = store.raw(response.url, response.fetched_at, response.body)
    catalog = response.json()
    seen_products = set()
    for row in sorted(catalog["tools"], key=lambda value: value["slug"]):
        name = clean(row.get("name"))
        source = canonical_url(row.get("url"))
        website = canonical_url(row.get("website"))
        price = PRICES.get(row.get("pricingModel"))
        if row.get("pricingModel") == "open-source" and row.get("freePlan") is True:
            price = "FREE"
        if not name or not source or not website or not price or row.get("retired"):
            continue
        resolved, decision = resolver.resolve(name, "product")
        key = (name_key(resolved), domain(website))
        if key in seen_products:
            continue
        seen_products.add(key)
        vendor, vendor_decision = resolver.vendor(website)
        content = {
            "entityName": resolved, "startupName": vendor, "pricingModel": price,
            "website": website, "category": row.get("category"), "pricesCheckedAt": row.get("pricesCheckedAt"),
        }
        payload = record("PRODUCT", "AGIPicks", source, content, {
            "snapshotUrl": response.url, "rawSha256": product_hash, "sourcePricingModel": row.get("pricingModel"),
            "vendorMatch": vendor_decision, "license": "CC BY 4.0",
            "attribution": "Data: AGIPicks (https://agipicks.com/data/), CC BY 4.0",
        })
        store.put(row["slug"], payload)
        store.mapping("product", name, "AGIPicks", resolved, decision, source)
        store.mapping("product_vendor", domain(website), "AGIPicks", vendor, vendor_decision, source)
        counts["products"] += 1
        counts["vendors_resolved"] += vendor is not None
        if counts["products"] >= target:
            break
    return dict(counts)


def repo_path(url):
    if domain(url) != "github.com":
        return None
    repo = urlsplit(url).path.strip("/").removesuffix(".git")
    return repo if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo) else None


def paper_identity(row):
    paper_id = str(row.get("arxiv_id") or "")
    match = re.fullmatch(r"(\d{4}\.\d{4,5}|[a-z.-]+/\d{7})(?:v\d+)?", paper_id, flags=re.I)
    return match[1] if match else None


async def papers(client, store, config, target, direct_limit, page_budget):
    base = config["papers"]
    seen_papers = set()
    progress = Counter()
    as_of = utcnow()
    direct_token = os.environ.get("GITHUB_TOKEN")
    if direct_token:
        direct_limit = target
    gh_headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if direct_token:
        gh_headers["Authorization"] = f"Bearer {direct_token}"
    direct_checks = 0
    gh_cache = {}
    repo_window = timedelta(hours=8)
    scope = base + "v2:has-code:with-resources:published-desc"

    async def read_page(page):
        cache = store.db.execute("SELECT observed,payload FROM paper_pages_v2 WHERE scope=? AND page=?",
                                 (scope, page)).fetchone()
        base_url = base + "papers/?" + urlencode({
            "page": page, "page_size": 100, "include_resources": "true", "has_code": "true",
            "order_by": "date_published", "order_dir": "desc",
        })
        if cache and as_of - datetime.fromisoformat(cache[0]) <= repo_window:
            at, data = datetime.fromisoformat(cache[0]), cache[1]
        else:
            response = await client.request(base_url)
            at, data = response.fetched_at, response.body
            with store.db:
                store.db.execute("INSERT OR REPLACE INTO paper_pages_v2 VALUES (?, ?, ?, ?)",
                                 (scope, page, iso(at), data))
        return base_url, at, json.loads(data), store.raw(base_url, at, data)

    async def github(repository):
        path = repo_path(repository["url"])
        key = path.casefold()
        if key in gh_cache:
            return gh_cache[key]
        result = await client.request("https://api.github.com/repos/" + path, headers=gh_headers)
        output = result.json()
        stars = output.get("stargazers_count")
        if type(stars) is not int or stars < 0 or not repo_path(output.get("html_url", "")):
            raise ValueError("Invalid GitHub repository response")
        answer = (stars, output["html_url"], iso(result.fetched_at), result.url,
                  store.raw(result.url, result.fetched_at, result.body))
        gh_cache[key] = answer
        return answer

    async def save(body, endpoint, observed, raw_hash):
        nonlocal direct_checks
        for row in body["results"]:
            paper_id = paper_identity(row)
            if not paper_id or paper_id in seen_papers:
                continue
            links = row.get("repositories") or []
            repos = [repo for repo in links if repo_path(repo.get("url", ""))]
            repos.sort(key=lambda repo: (
                not bool(repo.get("is_official")), not bool(repo.get("num_stars")),
                str(repo["url"]).casefold(),
            ))
            if not repos:
                continue
            repo = repos[0]
            stamp = str(row.get("published") or "")
            try:
                published_date = date.fromisoformat(stamp)
            except ValueError:
                continue
            if published_date > as_of.date():
                continue
            stars = repo.get("num_stars")
            if type(stars) is not int or stars < 0:
                continue
            repository_url = repo["url"]
            metric_url = endpoint
            metric_source = "Papers with Code GitHub metric snapshot"
            metric_observed = iso(observed)
            metric_hash = raw_hash
            upstream_updated = repo.get("updated_at")
            previous = store.db.execute(
                "SELECT payload FROM records WHERE kind='RESEARCH_PAPER' AND identity=?", (paper_id,)
            ).fetchone()
            if previous:
                old = json.loads(previous[0])
                ev = old["evidence"]
                at = datetime.fromisoformat(ev["githubMetricObservedAt"])
                if (ev["githubMetricSource"] == "GitHub REST API" and as_of - at <= repo_window
                        and repo_path(old["content"]["github_url"]).casefold() == repo_path(repository_url).casefold()):
                    gh_cache[repo_path(repository_url).casefold()] = (
                        old["content"]["github_stars"], old["content"]["github_url"],
                        ev["githubMetricObservedAt"], ev["githubMetricUrl"], ev["githubMetricSha256"],
                    )
            should_check = direct_checks < direct_limit or repo_path(repository_url).casefold() in gh_cache
            if should_check:
                direct_checks += repo_path(repository_url).casefold() not in gh_cache
                try:
                    stars, repository_url, metric_observed, metric_url, metric_hash = await github(repo)
                    metric_source = "GitHub REST API"
                except (FetchError, aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
                    progress["github_lookup_errors"] += 1
                    if isinstance(exc, FetchError) and exc.response.status in {403, 429}:
                        direct_checks = direct_limit
            if metric_source != "GitHub REST API" and stars == 0:
                progress["unverified_zero_excluded"] += 1
                continue
            authors = [clean(author) for author in row.get("authors", []) if isinstance(author, str) and clean(author)]
            if not authors or not clean(row.get("title")):
                continue
            url = "https://paperswithcode.co/paper/" + quote(str(row["id"]), safe="")
            content = {
                "title": clean(row["title"]), "authors": authors,
                "paper_url": "https://arxiv.org/abs/" + paper_id,
                "github_url": repository_url, "github_stars": stars,
                "published_date": iso(datetime.combine(published_date, datetime.min.time(), tzinfo=UTC)),
            }
            evidence = {
                "catalogRecordUrl": base + f"papers/{row['id']}",
                "repositoryLinkUrl": base + f"papers/{row['id']}/repositories", "repositoryLinksSha256": raw_hash,
                "catalogPageUrl": endpoint,
                "githubMetricSource": metric_source, "githubMetricUrl": metric_url,
                "githubMetricObservedAt": metric_observed, "githubMetricSha256": metric_hash,
                "upstreamRepositoryUpdatedDate": upstream_updated, "publicationPrecision": "date",
                "repositoryIsOfficial": bool(repo.get("is_official")),
            }
            seen_papers.add(paper_id)
            store.put(paper_id, record("RESEARCH_PAPER", "Papers with Code + arXiv", url, content, evidence))
            progress["papers"] += 1
            progress["github_direct" if metric_source == "GitHub REST API" else "github_snapshot"] += 1
            if progress["papers"] % 50 == 0:
                LOG.info("paper_progress %s", dict(progress))
            if progress["papers"] >= target:
                break

    for start in range(1, page_budget + 1, 3):
        pages = await asyncio.gather(*(read_page(page) for page in range(start, min(start + 3, page_budget + 1))))
        for endpoint, observed, body, raw_hash in pages:
            await save(body, endpoint, observed, raw_hash)
            if progress["papers"] >= target or not body.get("next_page"):
                return dict(progress)
    return dict(progress)
