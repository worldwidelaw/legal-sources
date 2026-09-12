#!/usr/bin/env python3
"""
NA/BON-Regulations — Bank of Namibia (regulations, determinations, circulars…)

The Bank of Namibia (https://www.bon.com.na) publishes its legal framework on a
Kentico CMS. The /Regulations/ tree organises documents by enabling Act
(Banking Institutions Act, Bank of Namibia Act, Payment System Management Act,
Financial Intelligence Act, …); each Act page has leaf listings — Determinations,
Regulations, Circulars, Guidelines, Directives, Other Bylaws — and every listed
document is an official Government Gazette PDF served via the Kentico attachment
handler at /getattachment/{guid}/.aspx.

Strategy:
  1. Bounded breadth-first crawl of the /Regulations/ tree (plus a couple of
     Banking-Supervision legal-framework pages) to collect document links.
  2. For each /getattachment/{guid}/.aspx link, download the PDF and extract the
     full text with pdfplumber.
  3. Drop image-only scans with a Latin-text quality filter.

Content is in English (born-digital gazette notices extract cleanly). Free
access, no authentication.

Usage:
  python bootstrap.py bootstrap --sample   # sample records for validation
  python bootstrap.py bootstrap            # full pull
  python bootstrap.py update               # incremental (re-crawl)
  python bootstrap.py test-api             # connectivity / link-count check
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NA.BON-Regulations")

BASE_URL = "https://www.bon.com.na"
SOURCE_ID = "NA/BON-Regulations"

# Seed listing pages. The crawler also auto-discovers /Regulations/* sub-pages
# reachable from these (one extra level), which surfaces the per-Act leaf
# listings (Determinations / Circulars / Guidelines / Directives / Bylaws).
SEED_PAGES = [
    "/Regulations.aspx",
    "/Bank/Banking-Supervision/Legal-Frameworks.aspx",
    "/Bank/Banking-Supervision/Legal-Frameworks/Circulars.aspx",
]

MAX_CRAWL_PAGES = 120
MAX_PDF_BYTES = 30_000_000
MAX_PDF_PAGES = 80
MIN_TEXT_CHARS = 500

MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"])}


def _clean_text(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_clean(text: str) -> bool:
    """Reject image-only scans: real gazette prose has many long word tokens."""
    if len(text) < MIN_TEXT_CHARS:
        return False
    tokens = [t for t in text.split() if any(ch.isalpha() for ch in t)]
    if len(tokens) < 80:
        return False
    long_tokens = [t for t in tokens if len(t) >= 4]
    return (len(long_tokens) / len(tokens)) >= 0.35


def _parse_date(text: str) -> Optional[str]:
    """Best-effort ISO date from the gazette header, e.g. 'WINDHOEK - 26
    November 2024' or '5 April 2024'."""
    head = text[:1200]
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", head)
    if m:
        d = int(m.group(1)); mo = MONTHS.get(m.group(2).lower()); y = int(m.group(3))
        if mo and 1900 < y < 2100:
            try:
                return datetime(y, mo, d).date().isoformat()
            except ValueError:
                pass
    m = re.search(r"\b(19|20)\d{2}\b", head)
    if m:
        return f"{m.group(0)}-01-01"
    return None


class BONRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
            respect_robots=False,
        )

    def _get_html(self, path: str) -> Optional[str]:
        url = urljoin(BASE_URL, path)
        try:
            resp = self.http.get(url, timeout=60)
        except Exception as e:
            logger.warning("Page fetch error %s: %s", path, e)
            return None
        if resp.status_code != 200:
            return None
        return resp.text

    def _crawl_links(self) -> list[dict]:
        """BFS over the /Regulations/ tree; return [{guid,title,page}] docs."""
        visited: set[str] = set()
        queue: list[str] = list(SEED_PAGES)
        docs: dict[str, dict] = {}

        while queue and len(visited) < MAX_CRAWL_PAGES:
            path = queue.pop(0)
            key = path.lower().split("?")[0]
            if key in visited:
                continue
            visited.add(key)
            html = self._get_html(path)
            if not html:
                continue

            # Harvest document attachments (with anchor text as the title).
            for m in re.finditer(
                    r'<a[^>]+href="(/getattachment/([0-9a-fA-F\-]{36})/[^"]*)"[^>]*>(.*?)</a>',
                    html, re.I | re.S):
                href, guid, txt = m.group(1), m.group(2).lower(), m.group(3)
                title = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", txt))).strip()
                if guid not in docs:
                    docs[guid] = {
                        "guid": guid,
                        "url": urljoin(BASE_URL, href),
                        "title": title,
                        "page": path,
                    }
                elif title and len(title) > len(docs[guid]["title"]):
                    docs[guid]["title"] = title

            # Enqueue deeper /Regulations/ listing pages.
            for href in re.findall(r'href="(/Regulations/[^"]+\.aspx)"', html, re.I):
                nkey = href.lower().split("?")[0]
                if nkey not in visited:
                    queue.append(href)

            time.sleep(1)

        logger.info("Crawled %d pages, found %d document links", len(visited), len(docs))
        return list(docs.values())

    def _extract_pdf(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url, timeout=90)
        except Exception as e:
            logger.warning("PDF fetch error: %s (%s)", url[:90], e)
            return None
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data or not data[:5].startswith(b"%PDF"):
            return None
        if len(data) > MAX_PDF_BYTES:
            logger.info("Skip oversized PDF (%d bytes): %s", len(data), url[:90])
            return None
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                parts = [(p.extract_text() or "") for p in pdf.pages[:MAX_PDF_PAGES]]
        except Exception as e:
            logger.warning("PDF parse error: %s (%s)", url[:90], e)
            return None
        return _clean_text("\n".join(parts))

    def _build(self, doc: dict) -> Optional[dict]:
        text = self._extract_pdf(doc["url"])
        if not text or not _is_clean(text):
            return None
        title = doc["title"] or "Bank of Namibia document"
        return {
            "_id": "na-bon-" + doc["guid"],
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": _parse_date(text),
            "url": doc["url"],
            "source_page": urljoin(BASE_URL, doc["page"]),
            "language": "en",
        }

    # ── BaseScraper interface ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for doc in self._crawl_links():
            rec = self._build(doc)
            if rec:
                yield rec
            time.sleep(1.2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ─────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="NA/BON-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = BONRegulationsScraper()

    if args.command == "test-api":
        docs = scraper._crawl_links()
        logger.info("Total document links discovered: %d", len(docs))
        for d in docs[:15]:
            logger.info("  %s — %s", d["guid"], d["title"][:60])
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    limit = 15 if args.sample else None
    count = 0
    for record in scraper.fetch_all():
        count += 1
        if args.sample or count <= 15:
            with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("[%d] %s — %d chars (%s)", count,
                    record["title"][:55], len(record["text"]), record.get("date"))
        if limit and count >= limit:
            break
    logger.info("Done: %d records", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
