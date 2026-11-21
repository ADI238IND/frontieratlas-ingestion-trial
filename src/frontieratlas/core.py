from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WINDOW = timedelta(hours=24)
KINDS = {"STARTUP", "PRODUCT", "RESEARCH_PAPER", "JOB", "NEWS"}


def utcnow():
    return datetime.now(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def clean(text):
    return " ".join(str(text or "").split())


def canonical_url(url):
    parts = urlsplit(str(url or ""))
    if parts.scheme not in {"https", "http"} or not parts.hostname or parts.username or parts.password:
        return None
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in {"gclid", "fbclid", "ref_src"}]
    return urlunsplit(("https", parts.netloc.casefold(), parts.path or "/", urlencode(sorted(query)), ""))


def domain(url):
    return (urlsplit(url or "").hostname or "").casefold().removeprefix("www.")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def name_key(name):
    value = unicodedata.normalize("NFKC", name).casefold().strip()
    return "".join(char for char in value if char.isalnum())


def company_name_key(name):
    value = re.sub(r"(?:[,\s]+(?:incorporated|inc\.?|llc|ltd\.?|limited|corp\.?|corporation))+$",
                   "", unicodedata.normalize("NFKC", name).casefold().strip())
    return name_key(value)


class Resolver:
    def __init__(self, path):
        self.aliases = {"company": {}, "product": {}}
        self.companies = {}
        for kind, entries in json.loads(Path(path).read_text()).items():
            for canonical, variants in entries.items():
                for name in [canonical, *variants]:
                    self._add(self.aliases[kind], self.key(name, kind), canonical)

    @staticmethod
    def key(name, kind):
        return company_name_key(name) if kind == "company" else name_key(name)

    @staticmethod
    def _add(table, key, value):
        if key:
            table.setdefault(key, set()).add(value)

    def observe_company(self, name, website, former_names=()):
        resolved, _ = self.resolve(name, "company")
        for alias in [name, *former_names]:
            self._add(self.aliases["company"], company_name_key(alias), resolved)
        self._add(self.companies, domain(website), resolved)

    def resolve(self, raw, kind):
        if not clean(raw):
            return None, "missing"
        matches = self.aliases[kind].get(self.key(raw, kind), set())
        if len(matches) == 1:
            canonical = next(iter(matches))
            return canonical, "canonical" if raw == canonical else "exact_normalized_alias"
        return clean(raw), "ambiguous" if matches else "unmapped"

    def vendor(self, website):
        names = self.companies.get(domain(website), set())
        return (next(iter(names)), "exact_company_domain") if len(names) == 1 else (
            None, "ambiguous_domain" if names else "no_company_evidence")


@dataclass(frozen=True)
class DateEvidence:
    earliest: datetime
    latest: datetime
    method: str
    value: str

    def eligible(self, as_of):
        return as_of - WINDOW <= self.earliest <= self.latest <= as_of


def parse_date(value, observed, method, *, relative=False):
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip()
    if relative:
        match = re.fullmatch(r"(\d+)\s+(minute|hour|day|second)s?\s+ago", value, re.I)
        if match:
            unit = timedelta(**{match[2].lower() + "s": 1})
            return DateEvidence(observed - (int(match[1]) + 1) * unit,
                                observed - int(match[1]) * unit, method + ":relative", value)
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        return None
    return DateEvidence(dt.astimezone(UTC), dt.astimezone(UTC), method, value)


def record(kind, source, url, content, evidence=None):
    url = canonical_url(url)
    if kind not in KINDS or not url:
        raise ValueError("Record requires a supported type and source URL")
    return {
        "schemaVersion": "1.0", "recordType": kind,
        "source": {"name": source, "url": url}, "content": content,
        "collectedAt": iso(utcnow()), "evidence": evidence or {},
    }


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS records (
            kind TEXT NOT NULL, identity TEXT NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY(kind, identity)
        );
        CREATE TABLE IF NOT EXISTS mapping (
            kind TEXT NOT NULL, raw TEXT NOT NULL, source TEXT NOT NULL, canonical TEXT,
            decision TEXT NOT NULL, url TEXT NOT NULL,
            PRIMARY KEY(kind, raw, source, url)
        );
        CREATE TABLE IF NOT EXISTS observations (
            kind TEXT NOT NULL, identity TEXT NOT NULL, first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL, reason TEXT NOT NULL, url TEXT NOT NULL,
            PRIMARY KEY(kind, identity)
        );
        CREATE TABLE IF NOT EXISTS raw (
            hash TEXT PRIMARY KEY, url TEXT NOT NULL, observed TEXT NOT NULL, body BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_pages_v2 (
            scope TEXT NOT NULL, page INTEGER NOT NULL, observed TEXT NOT NULL,
            payload BLOB NOT NULL, PRIMARY KEY(scope, page)
        );
        """)

    def put(self, identity, data):
        with self.db:
            self.db.execute(
                "INSERT INTO records VALUES (?, ?, ?) ON CONFLICT(kind, identity) DO UPDATE SET payload=excluded.payload",
                (data["recordType"], identity, json.dumps(data, ensure_ascii=False)),
            )

    def has(self, kind, identity):
        return self.db.execute("SELECT 1 FROM records WHERE kind=? AND identity=?", (kind, identity)).fetchone()

    def mapping(self, kind, raw, source, canonical, decision, url):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO mapping VALUES (?, ?, ?, ?, ?, ?)",
                (kind, raw or "", source, canonical, decision, url),
            )

    def observe(self, kind, identity, at, reason, url):
        previous = self.db.execute(
            "SELECT first_seen, last_seen FROM observations WHERE kind=? AND identity=?", (kind, identity)
        ).fetchone()
        with self.db:
            self.db.execute("""
            INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(kind, identity) DO UPDATE SET
                last_seen=excluded.last_seen, reason=excluded.reason
            """, (kind, identity, iso(at), iso(at), reason, url))
        return previous

    def raw(self, url, observed, body):
        content_hash = digest(body)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO raw VALUES (?, ?, ?, ?)",
                            (content_hash, url, iso(observed), body))
        return content_hash

    def rows(self, kind):
        for identity, payload in self.db.execute(
            "SELECT identity, payload FROM records WHERE kind=? ORDER BY identity", (kind,)
        ):
            yield identity, json.loads(payload)
