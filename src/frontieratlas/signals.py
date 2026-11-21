from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from html import unescape
import json
import logging
import re
from urllib.parse import urljoin

import aiohttp
from lxml import etree, html

from .core import canonical_url, clean, digest, domain, iso, parse_date, record, utcnow
from .http import FetchError

LOG = logging.getLogger(__name__)
META_DATES = {"article:published_time", "datepublished", "parsely-pub-date", "publish-date", "pubdate"}
REMOVE = "script|style|noscript|svg|nav|footer|aside|form|button|iframe"
ROLE = {
    "Research": r"\b(research|scientist|postdoctoral)\b",
    "Engineering": r"\b(engineer\w*|developer|architect|technical lead|sre|security|infrastructure)\b",
    "Design": r"\b(design\w*|creative director)\b",
    "Product": r"\b(product|program manager)\b",
    "Sales": r"\b(sales|account executive|business development|solutions consultant)\b",
    "Operations": r"\b(operations|finance|legal|recruit\w*|people|procurement)\b",
    "Marketing": r"\b(marketing|communications|brand|content strategist)\b",
    "Customer Success": r"\b(customer success|customer support)\b",
}


def classify(title, department=None):
    for family, pattern in ROLE.items():
        match = re.search(pattern, title or "", flags=re.I) or re.search(pattern, department or "", flags=re.I)
        if match:
            return family, match.group()
    return None, None


def strip_html(value):
    if not isinstance(value, str) or not clean(value):
        return ""
    try:
        tree = html.fromstring(value)
    except (ValueError, etree.ParserError):
        return clean(value)
    for el in tree.xpath(".//" + "|.//".join(REMOVE.split("|"))):
        el.drop_tree()
    return "\n".join(clean(part) for part in tree.itertext() if clean(part))


def objects(node):
    if isinstance(node, list):
        for entry in node:
            yield from objects(entry)
    elif isinstance(node, dict):
        yield node
        yield from objects(node.get("@graph"))


def structured(tree, types):
    for script in tree.xpath('//script[@type="application/ld+json"]/text()'):
        try:
            entries = json.loads(script)
        except (json.JSONDecodeError, ValueError):
            continue
        for entry in objects(entries):
            labels = entry.get("@type", [])
            labels = [labels] if isinstance(labels, str) else labels
            if isinstance(labels, list) and types.intersection(labels):
                yield entry


def parse_page(body):
    return html.fromstring(body, parser=html.HTMLParser(no_network=True, recover=True))


def article(tree, page_url, permitted):
    date_values = []
    body = ""
    for obj in structured(tree, {"Article", "NewsArticle", "BlogPosting", "TechArticle"}):
        if obj.get("isAccessibleForFree") in {False, "false", "False"}:
            raise ValueError("Article body requires subscription access")
        if obj.get("datePublished"):
            date_values.append(("json-ld.datePublished", obj["datePublished"]))
        body = body or strip_html(obj.get("articleBody"))
    canonical = page_url
    for href in tree.xpath('//link[@rel="canonical"]/@href'):
        candidate = canonical_url(urljoin(page_url, href))
        # Restrict canonical claims to the same publisher and a specific article path.
        if candidate and candidate.rstrip("/") != "https://" + domain(candidate) and domain(candidate) in permitted:
            canonical = candidate
            break
    for tag in tree.xpath("//meta[@content]"):
        key = (tag.get("property") or tag.get("name") or tag.get("itemprop") or "").casefold()
        if key in META_DATES:
            date_values.append((key, tag.get("content")))
    for tag in tree.xpath('//time[@itemprop="datePublished" or @pubdate]'):
        date_values.append(("published-time-element", tag.get("datetime") or tag.text_content()))
    if len(body) < 240:
        candidates = tree.xpath('//*[@itemprop="articleBody"]|//article|//main')
        for candidate in candidates:
            for el in candidate.xpath(".//" + "|.//".join(REMOVE.split("|"))):
                el.drop_tree()
            paragraphs = [clean(" ".join(p.itertext()))
                          for p in candidate.xpath(".//p|.//h2|.//h3|.//li|.//blockquote|.//pre")]
            # Preserve the source sequence while suppressing repeated template blocks.
            text = "\n\n".join(dict.fromkeys(line for line in paragraphs if len(line) >= 12))
            if len(text) > len(body):
                body = text
    return canonical, date_values, body


def agree(evidence):
    if not evidence:
        return None
    if max(x.earliest for x in evidence) - min(x.latest for x in evidence) > timedelta(hours=2):
        return None
    # Check the conservative envelope, then prefer the publisher's first-ranked value for display.
    return min(item.earliest for item in evidence), max(item.latest for item in evidence)


async def map_batches(items, function, size=12):
    for offset in range(0, len(items), size):
        await asyncio.gather(*(function(item) for item in items[offset:offset + size]))


async def news_source(client, store, source, as_of, llm):
    counts = Counter()
    result = await client.request(source["url"], redirects=source["hosts"])
    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=False)
    tree = etree.fromstring(result.body, parser)
    feed_hash = store.raw(result.url, result.fetched_at, result.body)
    items = tree.xpath('//*[local-name()="item" or local-name()="entry"]')
    if not items:
        raise ValueError("News feed contains no entries")
    seen = set()

    async def ingest(item):
        title = clean(item.xpath('string(./*[local-name()="title"])'))
        link_value = item.xpath('string(./*[local-name()="link"][not(@rel) or @rel="alternate"]/@href)')
        link_value = link_value or item.xpath('string(./*[local-name()="link"])')
        url = canonical_url(urljoin(result.url, link_value)) if link_value else None
        allowed = {host.removeprefix("www.") for host in source["hosts"]}
        if not url or domain(url) not in allowed or not title or url in seen:
            return
        seen.add(url)
        pub_value = item.xpath('string(./*[local-name()="published" or local-name()="pubDate"])')
        feed_date = parse_date(pub_value, result.fetched_at, "feed.publication", relative=True)
        if feed_date and not feed_date.eligible(as_of):
            counts["outside_window"] += 1
            return
        if store.has("NEWS", url):
            counts["duplicate"] += 1
            return
        previous = store.observe("NEWS", url, as_of, "candidate", url)
        try:
            page = await client.request(url, redirects=source["hosts"], max_bytes=8_000_000)
            canonical, page_dates, full_text = article(parse_page(page.body), page.url, allowed)
            canonical = canonical_url(canonical)
        except (FetchError, aiohttp.ClientError, TimeoutError, OSError, ValueError, etree.Error) as exc:
            LOG.info("news_page_unavailable publisher=%s type=%s", source["name"], type(exc).__name__)
            counts["article_unavailable"] += 1
            store.observe("NEWS", url, as_of, "article_unavailable", url)
            return
        evidence = [d for method, text in page_dates
                    if (d := parse_date(text, page.fetched_at, method, relative=True))]
        if feed_date:
            evidence.append(feed_date)
        bounds = agree(evidence)
        if not bounds or not all(d.eligible(as_of) for d in evidence):
            reason = "newly_discovered_unverified" if not evidence and not previous else "publication_unverified"
            store.observe("NEWS", url, as_of, reason, url)
            counts[reason] += 1
            return
        if len(full_text) < 240:
            counts["body_unavailable"] += 1
            return
        if canonical != url and store.has("NEWS", canonical):
            counts["duplicate"] += 1
            return
        timestamp = evidence[0]
        raw_hash = store.raw(page.url, page.fetched_at, page.body)
        content = {"title": title, "date": iso(timestamp.latest), "full_text": full_text}
        if llm.enabled:
            result_llm = await llm.extract(full_text, {"entity": "Core company or lab explicitly named in the article."})
            if result_llm:
                content["mentionedEntity"] = result_llm
        store.put(canonical, record("NEWS", source["name"], canonical, content, {
            "bodySha256": digest(full_text.encode()), "rawSha256": raw_hash, "feedSha256": feed_hash,
            "dateMethod": timestamp.method, "rawDate": timestamp.value,
            "earliest": iso(bounds[0]), "latest": iso(bounds[1]),
            "discoveryUrl": result.url, "pageUrl": page.url,
        }))
        counts["accepted"] += 1

    await map_batches(items, ingest)
    return dict(counts)


async def job_source(client, store, resolver, source, as_of, llm):
    counts = Counter()
    response = await client.request(source["url"])
    jobs = response.json()["jobs"]
    listing_hash = store.raw(response.url, response.fetched_at, response.body)
    ashby = source["adapter"] == "ashby"
    company, decision = resolver.resolve(source["name"], "company")
    allowed = {host.removeprefix("www.") for host in source["hosts"]}
    seen = set()

    async def ingest(job):
        title = clean(job.get("title"))
        url = canonical_url(job.get("jobUrl" if ashby else "absolute_url"))
        identifier = str(job.get("id") or "")
        if not title or not identifier or not url or domain(url) not in allowed or (ashby and not job.get("isListed")):
            return
        identity = f"{source['adapter']}:{source['name']}:{identifier}"
        if identity in seen or store.has("JOB", identity):
            counts["duplicate"] += 1
            return
        seen.add(identity)
        raw_date = job.get("publishedAt" if ashby else "first_published")
        publication = parse_date(raw_date, response.fetched_at, "Greenhouse.first_published" if not ashby else
                                 "Ashby.publishedAt")
        if publication and not publication.eligible(as_of):
            counts["outside_window"] += 1
            return
        page_hash = None
        if ashby or not publication:
            # Ashby documents publishedAt as the last publish event. Prefer datePosted from the posting itself.
            try:
                page = await client.request(url, redirects=source["hosts"], max_bytes=8_000_000)
                postings = list(structured(parse_page(page.body), {"JobPosting"}))
                dates = [d for posting in postings if (
                    d := parse_date(posting.get("datePosted"), page.fetched_at, "JobPosting.datePosted")
                )]
                if dates:
                    publication = min(dates, key=lambda d: d.earliest)
                    page_hash = store.raw(page.url, page.fetched_at, page.body)
                elif ashby:
                    publication = None
            except (FetchError, aiohttp.ClientError, TimeoutError, OSError, ValueError, etree.Error):
                publication = None
        if not publication or not publication.eligible(as_of):
            earlier = store.observe("JOB", identity, as_of, "unverified_publication", url)
            counts["newly_discovered_unverified" if not earlier else "publication_unverified"] += 1
            return
        if ashby:
            body = (job.get("descriptionPlain") or "").strip() or strip_html(job.get("descriptionHtml"))
            remote = job.get("isRemote")
            department = job.get("department")
            location = job.get("location")
        else:
            body = strip_html(unescape(job.get("content") or ""))
            location = (job.get("location") or {}).get("name")
            remote = None
            # The absence of the word Remote does not establish an on-site policy.
            if re.search(r"\bremote\b", location or "", flags=re.I):
                remote = True
            department = " | ".join(d["name"] for d in job.get("departments", []) if d.get("name"))
        if not body:
            counts["body_unavailable"] += 1
            return
        family, family_evidence = classify(title, department)
        content = {"company": company, "date": iso(publication.latest), "title": title,
                   "is_remote": remote if isinstance(remote, bool) else None, "role_family": family,
                   "location": location, "department": department, "full_text": body}
        if llm.enabled and family is None:
            extracted = await llm.extract(body, {"role": "An explicitly named job function or formal department."})
            if extracted:
                family, family_evidence = classify(extracted["value"])
                content["role_family"] = family
        store.mapping("company", source["name"], source["name"], company, decision, url)
        store.put(identity, record("JOB", source["name"], url, content, {
            "rawSha256": listing_hash, "pageSha256": page_hash, "boardUrl": response.url, "boardId": identifier,
            "dateMethod": publication.method, "rawDate": publication.value,
            "earliest": iso(publication.earliest), "latest": iso(publication.latest),
            "bodySha256": digest(body.encode()), "roleFamilyEvidence": family_evidence,
        }))
        counts["accepted"] += 1

    await map_batches(jobs, ingest)
    return dict(counts)
