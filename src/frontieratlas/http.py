from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
import json
import logging
import random
import time
from urllib.parse import urljoin, urlsplit

import aiohttp

LOG = logging.getLogger(__name__)


def retry_delay(headers, attempt):
    delay = random.uniform(0, min(30, 2 ** (attempt + 1)))
    value = headers.get("Retry-After")
    if value:
        try:
            hint = float(value)
        except ValueError:
            try:
                hint = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
            except (TypeError, ValueError):
                hint = 0
        delay = max(delay, hint)
    if headers.get("X-RateLimit-Remaining") == "0":
        reset = headers.get("X-RateLimit-Reset", "")
        if reset.isdecimal():
            delay = max(delay, int(reset) - time.time())
    return max(0, delay)


@dataclass
class Response:
    status: int
    url: str
    headers: object
    body: bytes
    fetched_at: datetime

    def json(self):
        return json.loads(self.body)


class FetchError(RuntimeError):
    def __init__(self, response):
        self.response = response
        super().__init__(f"HTTP {response.status} from {host(response.url)}")


def host(url):
    return (urlsplit(url).hostname or "").casefold()


class Client:
    def __init__(self, session, total=16, source_lanes=4, seconds=None, attempts=3):
        self.session = session
        self.total = asyncio.Semaphore(total)
        self.lanes = defaultdict(lambda: asyncio.Semaphore(source_lanes))
        self.start_locks = defaultdict(asyncio.Lock)
        self.next_call = defaultdict(float)
        self.retry_after = defaultdict(float)
        self.deferred = {}
        self.seconds = seconds or {}
        self.attempts = attempts

    async def _one(self, method, url, headers, data, max_bytes):
        domain = host(url)
        async with self.lanes[domain]:
            async with self.start_locks[domain]:
                while True:
                    if self.retry_after[domain] - time.monotonic() > 60 and domain in self.deferred:
                        raise FetchError(self.deferred[domain])
                    delay = max(self.next_call[domain], self.retry_after[domain]) - time.monotonic()
                    if delay <= 0:
                        break
                    await asyncio.sleep(delay)
                self.next_call[domain] = time.monotonic() + self.seconds.get(domain, 0.10)
            async with self.total, self.session.request(
                method, url, headers=headers, data=data, allow_redirects=False
            ) as result:
                body = bytearray()
                async for block in result.content.iter_chunked(65536):
                    body.extend(block)
                    if len(body) > max_bytes:
                        raise ValueError(f"Response exceeds {max_bytes} bytes from {domain}")
                observed = datetime.now(UTC)
                edge_date = result.headers.get("Date")
                if edge_date:
                    try:
                        observed = min(observed, parsedate_to_datetime(edge_date).astimezone(UTC))
                    except (TypeError, ValueError):
                        pass
                return Response(result.status, str(result.url), result.headers, bytes(body), observed)

    async def request(self, url, *, method="GET", headers=None, data=None, redirects=(),
                      max_bytes=32_000_000, retry=True):
        initial_host = host(url)
        allowed = {initial_host, *redirects}
        headers = dict(headers or {})
        for _ in range(5):
            if urlsplit(url).scheme != "https" or host(url) not in allowed:
                raise ValueError("Redirect outside the source allowlist")
            current_host = host(url)
            if current_host != initial_host:
                headers = {}
            for attempt in range(self.attempts if retry else 1):
                try:
                    result = await self._one(method, url, headers, data, max_bytes)
                except (aiohttp.ClientError, TimeoutError):
                    if attempt + 1 >= (self.attempts if retry else 1):
                        raise
                    await asyncio.sleep(retry_delay({}, attempt))
                    continue
                challenge = result.headers.get("cf-mitigated") == "challenge"
                throttled = result.status == 429 or (
                    result.status == 403
                    and (result.headers.get("Retry-After") or result.headers.get("X-RateLimit-Remaining") == "0")
                )
                if (throttled or result.status == 503) and retry and not challenge:
                    delay = retry_delay(result.headers, attempt)
                    self.retry_after[current_host] = max(self.retry_after[current_host], time.monotonic() + delay)
                    if delay > 60:
                        self.deferred[current_host] = result
                    if delay > 60 or attempt + 1 >= self.attempts:
                        raise FetchError(result)
                    continue
                if retry and not challenge and 500 <= result.status < 600 and attempt + 1 < self.attempts:
                    await asyncio.sleep(retry_delay(result.headers, attempt))
                    continue
                break
            if result.status in {301, 302, 303, 307, 308} and method == "GET":
                if "Location" not in result.headers:
                    raise FetchError(result)
                url = urljoin(url, result.headers["Location"])
                continue
            if challenge or not (200 <= result.status < 300 or result.status == 304):
                raise FetchError(result)
            return result
        raise ValueError("Too many source redirects")

    async def get_json(self, url, **kwargs):
        return (await self.request(url, **kwargs)).json()
