#!/usr/bin/env python3
"""
STUK — Radiation and Nuclear Safety Authority (Finland) YVL / VAL guides fetcher.

STUK is Finland's radiation and nuclear safety authority. Under the Nuclear
Energy Act it issues the YVL Guides (regulatory guides on nuclear safety,
series A–E) and VAL Guides (radiation safety), which set the detailed,
binding regulatory requirements for Finnish nuclear facilities and radiation
practices. STUKLEX (the authority's legal database) classifies each guide as a
"Regulation" — so they are legislation (regulations) for our purposes.

Enumeration: the public index pages at stuk.fi (/en/yvl-guides and
/en/val-guides) link every guide to its full-text page on STUKLEX at
https://www.stuklex.fi/en/ohje/{ID} (e.g. YVLA-1, YVLB-2, VAL1).

Full text: each STUKLEX guide page server-renders the complete guide body.
The main content lives in the `document-wrapper` block; its header line is
"{Title}, {dd.mm.yyyy}{ID}" from which the title, issue date and guide number
are parsed. Tags are stripped via a shared HTML cleaner.

License: STUKLEX material is public-authority regulatory text (Finnish open
government data). Reused with attribution to STUK.

Usage:
  python bootstrap.py test                # verify index + one guide page
  python bootstrap.py bootstrap --sample  # fetch 15 sample records
  python bootstrap.py bootstrap           # full run
"""

import hashlib
import html as html_lib
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

SOURCE_DIR = Path(__file__).parent
sys.path.insert(0, str(SOURCE_DIR.parent.parent.parent))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.STUK")

STUKLEX_BASE = "https://www.stuklex.fi"
INDEX_PAGES = [
    "https://stuk.fi/en/yvl-guides",
    "https://stuk.fi/en/val-guides",
]
GUIDE_RE = re.compile(r"stuklex\.fi/en/ohje/([A-Za-z0-9-]+)")


def _strip_tags(fragment: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", fragment, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _iso_date(dmy: Optional[str]) -> Optional[str]:
    """Convert Finnish 'd.m.yyyy' to ISO 'yyyy-mm-dd'."""
    if not dmy:
        return None
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", dmy)
    if not m:
        return None
    d, mo, y = m.groups()
    return f"{y}-{int(mo):02d}-{int(d):02d}"


class STUKScraper(BaseScraper):
    """Scraper for FI/STUK YVL / VAL regulatory guides."""

    def __init__(self):
        super().__init__(str(SOURCE_DIR))
        self.client = HttpClient(
            base_url=STUKLEX_BASE,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
        )

    def _load_guide_ids(self) -> list[str]:
        """Enumerate all guide IDs from the STUK index pages (order-preserving)."""
        ids: list[str] = []
        seen: set[str] = set()
        for page in INDEX_PAGES:
            try:
                resp = self.client.get(page)
                resp.raise_for_status()
            except Exception as e:
                logger.warning("Failed to load index %s: %s", page, e)
                continue
            html = resp.content.decode("utf-8", errors="replace")
            for gid in GUIDE_RE.findall(html):
                if gid not in seen:
                    seen.add(gid)
                    ids.append(gid)
        logger.info("Enumerated %d guide IDs from %d index pages", len(ids), len(INDEX_PAGES))
        return ids

    def _fetch_guide(self, gid: str) -> Optional[dict]:
        """Fetch and parse a single STUKLEX guide page."""
        url = f"{STUKLEX_BASE}/en/ohje/{gid}"
        resp = self.client.get(url)
        resp.raise_for_status()
        page = resp.content.decode("utf-8", errors="replace")

        # Main guide body: from the document-wrapper block up to the footer.
        w = page.find("document-wrapper")
        if w == -1:
            return None
        start = page.rfind("<div", 0, w)
        end = page.find("<footer")
        if end == -1:
            end = len(page)
        body = _strip_tags(page[start:end])
        if not body or len(body) < 300:
            return None

        # Header line: "{Title}, {d.m.yyyy}{ID}  Suomeksi ..."
        header = body[:400]
        m = re.match(
            r"(.*?),\s*(\d{1,2}\.\d{1,2}\.\d{4})\s*(YVL\s*[A-E]\.\d+|VAL\s*\d+)",
            header,
        )
        title = None
        date = None
        guide_no = None
        if m:
            title, dmy, guide_no = m.group(1).strip(), m.group(2), m.group(3).strip()
            date = _iso_date(dmy)
        if not title:
            # Fallback: page <title> "YVL A.1 | Regulation | Stuklex"
            tm = re.search(r"<title>([^<|]+)", page)
            title = (tm.group(1).strip() if tm else gid)
        if not guide_no:
            guide_no = gid

        return {
            "guide_id": gid,
            "guide_no": guide_no,
            "title": title,
            "date": date,
            "url": url,
            "text": body,
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"FI/STUK/{raw['guide_id']}",
            "_source": "FI/STUK",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "guide_id": raw["guide_id"],
            "guide_no": raw.get("guide_no", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        ids = self._load_guide_ids()
        limit = 15 if sample else None
        count = 0
        for gid in ids:
            if limit and count >= limit:
                break
            try:
                self.rate_limiter.wait()
                doc = self._fetch_guide(gid)
            except Exception as e:
                logger.warning("  Failed guide %s: %s", gid, e)
                continue
            if not doc:
                logger.warning("  Skipping %s — no/short body", gid)
                continue
            yield doc
            count += 1
            logger.info("  [%d] %s — %s (%d chars)", count, doc["guide_no"], doc["title"][:50], len(doc["text"]))
        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = STUKScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ids = scraper._load_guide_ids()
        if not ids:
            print("FAILED - no guide IDs found")
            sys.exit(1)
        print(f"Enumerated {len(ids)} guides. Testing first: {ids[0]}")
        doc = scraper._fetch_guide(ids[0])
        if not doc:
            print("FAILED - could not parse first guide")
            sys.exit(1)
        print(f"  {doc['guide_no']} | {doc['title']} | {doc['date']} | {len(doc['text'])} chars")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
