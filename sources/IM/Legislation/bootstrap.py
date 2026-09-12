#!/usr/bin/env python3
"""
IM/Legislation -- Isle of Man Legislation (Acts of Tynwald + subordinate legislation)

Consolidated ("as amended") legislation published by the Attorney General's
Chambers at legislation.gov.im. Every item is a PDF with selectable text under
/cms/images/LEGISLATION/{PRINCIPAL|SUBORDINATE}/{year}/{ref}/{ref}_{version}.pdf.

Discovery is index-driven. The open directory listings under
/cms/images/LEGISLATION/ are no longer reliably browsable, so the corpus is
enumerated from the three com_legislation index views instead:

  /cms/legislation/current.html            in-force legislation, filtered A-Z
                                           via a POST (submit4=<letter>)
  /cms/legislation/repealed.html           repealed Acts (single page)
  /cms/legislation/revoked-legislation.html revoked subordinate legislation

Each row carries an <a class="npWrap"> anchor pointing straight at the latest
version PDF, plus the short title, so no directory traversal is needed.

Two site behaviours must be respected or the crawl silently yields nothing:
  * the WAF hard-403s a non-browser User-Agent (this is what broke #1380);
  * SiteGround answers HTTP 202 with an `sg-captcha: challenge` body when a
    single IP requests too fast — that is a throttle, not a page.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Alias for the full pull (fleet entry point)
  python bootstrap.py bootstrap --sample # Fetch 15+ sample records
  python bootstrap.py update             # Incremental update (current legislation only)
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import base64
import hashlib
import logging
import random
import time
import string
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IM.Legislation")

BASE_URL = "https://legislation.gov.im"

# The legislation.gov.im WAF returns a hard 403 for non-browser User-Agents.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

CURRENT_PATH = "/cms/legislation/current.html"
# Single-page indexes (no A-Z filter form), keyed by the status they describe.
FLAT_INDEXES = [
    ("/cms/legislation/repealed.html", "repealed"),
    ("/cms/legislation/revoked-legislation.html", "revoked"),
]

# Row anchors in the com_legislation tables point straight at the latest PDF.
NPWRAP_RE = re.compile(r'<a\s+class="npWrap"\s+href="([^"]+\.pdf)"[^>]*>(.*?)</a>', re.S)
# /cms/images/LEGISLATION/PRINCIPAL/2019/2019-0001/2019-0001_3.pdf
PDF_PATH_RE = re.compile(
    r"/cms/images/LEGISLATION/(PRINCIPAL|SUBORDINATE)/(\d{4})/([^/]+)/([^/]+)\.pdf$",
    re.IGNORECASE,
)

REQUEST_DELAY = 1.0  # seconds between requests — SiteGround throttles bursts


class ThrottleChallenge(Exception):
    """Raised when SiteGround answers with its captcha challenge instead of content."""


# ── SiteGround "Robot Challenge Screen" proof of work ──────────────────
#
# The challenge page ships a web worker that brute-forces a SHA-1 partial
# preimage. Reimplemented here so the crawl clears it instead of waiting the
# block out (the site serves the challenge to every non-browser client, so
# backing off alone never gets past it).
#
#   sgchallenge = "<bits>:<ts>:<id>:<hex>:"
#   payload     = sgchallenge_bytes + counter (minimal big-endian bytes)
#   accepted    = top <bits> bits of SHA1(payload) are zero
#   answer      = base64(payload), submitted to sgsubmit_url as ?sol=…&s=<ms>:<n>
#
# The response sets an `_I_` cookie that whitelists the session.
CHALLENGE_URL_RE = re.compile(r"/\.well-known/sgcaptcha/\?[^\"']+")
SGCHALLENGE_RE = re.compile(r'sgchallenge\s*=\s*"([^"]+)"')
SGSUBMIT_RE = re.compile(r'sgsubmit_url\s*=\s*"([^"]+)"')
CHALLENGE_MAX_HASHES = 40_000_000  # ~30s ceiling; a 21-bit target needs ~2M


def _counter_bytes(c: int) -> bytes:
    """Minimal big-endian encoding, matching the worker's byte packing."""
    if c > 0xFFFFFF:
        width = 4
    elif c > 0xFFFF:
        width = 3
    elif c > 0xFF:
        width = 2
    else:
        width = 1
    return c.to_bytes(width, "big")


def solve_sg_challenge(challenge: str) -> tuple[str, int, int]:
    """Return (base64 solution, elapsed ms, hashes tried) for an sgchallenge."""
    bits = int(challenge.split(":", 1)[0])
    prefix = challenge.encode()
    counter = random.randrange(0, 5_000_000)
    started = time.time()
    tried = 0
    while tried < CHALLENGE_MAX_HASHES:
        payload = prefix + _counter_bytes(counter)
        head = int.from_bytes(hashlib.sha1(payload).digest()[:4], "big")
        # head must be non-zero: the worker treats a zero first word as a failure.
        if head and (head >> (32 - bits)) == 0:
            return (
                base64.b64encode(payload).decode(),
                int((time.time() - started) * 1000),
                tried,
            )
        counter += 1
        tried += 1
    raise ThrottleChallenge(
        f"no solution for a {bits}-bit challenge after {tried} hashes"
    )


def parse_index(html: str, status: str) -> list[dict]:
    """Extract legislation rows from a com_legislation index table."""
    items = []
    for href, inner in NPWRAP_RE.findall(html):
        m = PDF_PATH_RE.search(href)
        if not m:
            continue
        title = re.sub(r"<[^>]+>", "", inner)
        title = (
            title.replace("&amp;", "&")
            .replace("&nbsp;", " ")
            .replace("&#039;", "'")
            .replace("&quot;", '"')
        )
        title = re.sub(r"\s+", " ", title).strip()
        # Trailing "[3]" is the version badge rendered inside the anchor.
        version = None
        vm = re.search(r"\[(\d+)\]\s*$", title)
        if vm:
            version = vm.group(1)
            title = title[: vm.start()].strip()
        leg_type = "principal" if m.group(1).upper() == "PRINCIPAL" else "subordinate"
        items.append(
            {
                "pdf_url": href,
                "title": title,
                "leg_type": leg_type,
                "year": m.group(2),
                "reference": m.group(3),
                "version": version,
                "legislation_status": status,
            }
        )
    return items


class LegislationScraper(BaseScraper):
    """Scraper for Isle of Man consolidated legislation."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            max_retries=3,
            backoff_factor=1.0,
            timeout=60,
        )

    # ── HTTP with SiteGround-challenge awareness ──────────────────────

    def _clear_challenge(self, resp) -> bool:
        """Solve the SiteGround robot challenge so the session is whitelisted.

        Returns True when the `_I_` cookie was obtained; the caller then just
        replays its original request.
        """
        m = CHALLENGE_URL_RE.search(resp.text)
        if not m:
            return False
        page = self.client.get(m.group(0).replace("&amp;", "&"))
        challenge = SGCHALLENGE_RE.search(page.text)
        submit = SGSUBMIT_RE.search(page.text)
        if not (challenge and submit):
            return False

        solution, elapsed_ms, hashes = solve_sg_challenge(challenge.group(1))
        logger.info(
            f"Solved SiteGround challenge in {elapsed_ms}ms ({hashes} hashes)"
        )
        submit_url = submit.group(1)
        submit_url += "&" if "?" in submit_url else "?"
        submit_url += (
            f"sol={urllib.parse.quote(solution, safe='')}&s={elapsed_ms}:{hashes}"
        )
        self.client.get(submit_url)
        return "_I_" in self.client.session.cookies

    def _request(self, method: str, path: str, **kwargs):
        """Issue a request, clearing SiteGround's robot challenge when it appears."""
        delay = 15
        last_exc = None
        for attempt in range(5):
            try:
                if method == "POST":
                    resp = self.client.post(path, **kwargs)
                else:
                    resp = self.client.get(path, **kwargs)
            except Exception as e:  # transient network error
                last_exc = e
                logger.warning(f"{method} {path} failed ({e}); retrying in {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue

            if resp.status_code == 202 and "sg-captcha" in {
                k.lower() for k in resp.headers
            }:
                last_exc = ThrottleChallenge(f"sg-captcha challenge on {path}")
                try:
                    if self._clear_challenge(resp):
                        continue  # session whitelisted — replay the request
                except Exception as e:
                    last_exc = e
                    logger.warning(f"Challenge solve failed on {path}: {e}")
                logger.warning(
                    f"SiteGround challenge unsolved on {path}; backing off {delay}s"
                )
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue

            if resp.status_code == 403:
                raise RuntimeError(
                    f"HTTP 403 from legislation.gov.im for {path} — the WAF is "
                    f"rejecting this client (User-Agent or IP block). Refusing to "
                    f"report an empty corpus as success."
                )

            resp.raise_for_status()
            return resp

        raise RuntimeError(f"{method} {path} unreachable after retries: {last_exc}")

    def _get(self, path: str, **kwargs):
        resp = self._request("GET", path, **kwargs)
        time.sleep(REQUEST_DELAY)
        return resp

    def _post(self, path: str, data: dict):
        resp = self._request("POST", path, data=data)
        time.sleep(REQUEST_DELAY)
        return resp

    # ── Discovery ─────────────────────────────────────────────────────

    def _current_letter(self, letter: str) -> list[dict]:
        """Fetch the in-force index filtered to titles starting with `letter`."""
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d 00:00:00")
        resp = self._post(
            CURRENT_PATH,
            {
                "submit4": letter,
                "pointintime_post": stamp,
                "pointintime_post_alpha": stamp,
            },
        )
        return parse_index(resp.text, "current")

    def discover(self, current_only: bool = False) -> list[dict]:
        """Enumerate every legislation item across the three index views.

        Deduplicates on the item reference (e.g. 2019-0001), keeping the first
        seen — current legislation wins over repealed/revoked listings.
        """
        seen: dict[str, dict] = {}

        # Establish the Joomla session before POSTing the letter filter.
        self._get(CURRENT_PATH)
        for letter in string.ascii_uppercase:
            rows = self._current_letter(letter)
            for row in rows:
                seen.setdefault(row["reference"], row)
            logger.info(
                f"current [{letter}]: {len(rows)} rows ({len(seen)} unique so far)"
            )

        if not seen:
            raise RuntimeError(
                "Enumeration of /cms/legislation/current.html returned 0 rows — "
                "the index layout changed or the site is blocking this client."
            )

        if not current_only:
            for path, status in FLAT_INDEXES:
                rows = parse_index(self._get(path).text, status)
                for row in rows:
                    seen.setdefault(row["reference"], row)
                logger.info(f"{status}: {len(rows)} rows ({len(seen)} unique so far)")

        logger.info(f"Discovered {len(seen)} unique legislation items")
        return list(seen.values())

    # ── Fetching ──────────────────────────────────────────────────────

    def _process_item(self, item: dict) -> Optional[dict]:
        """Download an item's latest PDF and extract full text."""
        try:
            resp = self._get(item["pdf_url"])
        except Exception as e:
            logger.warning(f"PDF download failed for {item['pdf_url']}: {e}")
            return None

        pdf_bytes = resp.content
        if len(pdf_bytes) < 200:
            logger.warning(
                f"PDF too small ({len(pdf_bytes)} bytes): {item['pdf_url']}"
            )
            return None

        doc_id = f"{item['leg_type'].upper()}/{item['reference']}"
        text = extract_pdf_markdown("IM/Legislation", doc_id, pdf_bytes=pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(
                f"Insufficient text from {item['pdf_url']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        out = dict(item)
        out["doc_id"] = doc_id
        out["text"] = text
        return out

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every legislation item with full text."""
        items = self.discover()
        count = 0
        for item in items:
            result = self._process_item(item)
            if result:
                count += 1
                yield result
                if count % 25 == 0:
                    logger.info(f"Processed {count}/{len(items)} items")
        logger.info(f"Total items with full text: {count}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-walk the in-force index only; the loader dedups on _id."""
        for item in self.discover(current_only=True):
            result = self._process_item(item)
            if result:
                yield result

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw entry into the standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        year = raw.get("year", "")
        pdf_url = raw.get("pdf_url", "")

        return {
            "_id": raw["doc_id"],
            "_source": "IM/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "title": raw.get("title") or "Untitled",
            "text": text,
            "date": f"{year}-01-01" if year and year.isdigit() else None,
            "url": BASE_URL + pdf_url if pdf_url else "",
            "leg_type": raw.get("leg_type", ""),
            "legislation_status": raw.get("legislation_status", ""),
            "version": raw.get("version"),
            "year": year,
            "reference": raw.get("reference", ""),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity and that the index still parses."""
        try:
            rows = parse_index(self._get(CURRENT_PATH).text, "current")
            if rows:
                logger.info(
                    f"Connection test passed — {len(rows)} rows on the default "
                    f"index page (e.g. {rows[0]['title']!r})"
                )
                return True
            logger.error("Index page reachable but no legislation rows parsed")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = LegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command in ("bootstrap", "bootstrap-fast"):
        count = 15 if sample_mode else 0
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=count or 10)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
