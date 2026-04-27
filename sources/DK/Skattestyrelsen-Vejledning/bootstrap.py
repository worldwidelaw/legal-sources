#!/usr/bin/env python3
"""
DK/Skattestyrelsen-Vejledning -- Danish Tax Legal Guidance Fetcher

Fetches the full text of Den Juridiske Vejledning (The Legal Guidance) from
info.skat.dk. This is a comprehensive, consolidated tax guidance document
published biannually by Skattestyrelsen (Danish Tax Agency).

Strategy:
  - Start from known chapter OIDs (24 chapters)
  - BFS crawl: follow all data.aspx?oid= links on each page
  - On leaf pages (children=0), extract full text from <div class='MPtext'>
  - ~9,200 leaf sections total across all chapters

Each page uses URL pattern: https://info.skat.dk/data.aspx?oid=NUMBER

Usage:
  python bootstrap.py bootstrap          # Full crawl
  python bootstrap.py bootstrap --sample # Fetch 15 sample leaf pages
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from collections import deque
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, Set

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.Skattestyrelsen-Vejledning")

PAGE_URL = "https://info.skat.dk/data.aspx"
DELAY = 2.0

# Known chapter OIDs
CHAPTER_OIDS = [
    ("74261", "A.A"), ("2061657", "A.B"), ("1919724", "A.C"), ("2048228", "A.D"),
    ("1920415", "C.A"), ("1945976", "C.B"), ("1920857", "C.C"), ("1899816", "C.D"),
    ("125", "C.E"), ("1977250", "C.F"), ("5585", "C.G"), ("1948070", "C.H"),
    ("934", "C.I"), ("68675", "C.J"), ("69325", "C.K"),
    ("1921126", "D.A"), ("1976930", "D.B"), ("1921338", "E.A"), ("2048578", "E.B"),
    ("2229597", "F.A"), ("9672", "G.A"), ("2048240", "H.A"), ("1948286", "I.A"),
    ("2111779", "J.A"),
]


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class SkatVejledning(BaseScraper):
    SOURCE_ID = "DK/Skattestyrelsen-Vejledning"

    def __init__(self):
        self.http = HttpClient(base_url="https://info.skat.dk")
        self.visited: Set[str] = set()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw

    def fetch_page(self, oid: str) -> Optional[str]:
        """Fetch a page by OID, return raw HTML."""
        try:
            resp = self.http.get(f"{PAGE_URL}?oid={oid}")
            time.sleep(DELAY)
            if resp is None or resp.status_code != 200:
                return None
            return resp.text
        except Exception as e:
            logger.warning("Error fetching OID %s: %s", oid, e)
            return None

    def parse_page(self, html_text: str) -> Dict[str, Any]:
        """Parse a page and extract metadata, content, and child links."""
        result = {"title": "", "text": "", "date": None, "children_count": 0, "child_oids": []}

        # Title from <title> tag
        m = re.search(r'<title>([^<]+)</title>', html_text, re.IGNORECASE)
        if m:
            result["title"] = html_module.unescape(m.group(1).replace(" - info.skat.dk", "").strip())

        # Children count
        m = re.search(r'name=["\']children["\']\s+(?:id=["\'][^"\']*["\']\s+)?value=["\'](\d+)', html_text, re.IGNORECASE)
        if m:
            result["children_count"] = int(m.group(1))

        # Date from pubDate meta
        m = re.search(r'name=["\']?pubDate["\']?\s+content=["\']([^"\']+)', html_text, re.IGNORECASE)
        if m:
            result["date"] = m.group(1).strip()[:10]

        # Full text from MPtext div — greedy match to end of div
        # The MPtext div contains the actual content
        m = re.search(r"<div\s+class=['\"]MPtext['\"]>(.*?)</div>\s*<div\s+class=['\"]MP", html_text, re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"<div\s+class=['\"]MPtext['\"]>(.*?)</div>\s*</td", html_text, re.DOTALL | re.IGNORECASE)
        if not m:
            # Broadest match: take everything after MPtext until MPbottom or footer
            m = re.search(r"<div\s+class=['\"]MPtext['\"]>(.*?)(?:<div\s+class=['\"]MPbottom|<div\s+class=['\"]footer|</body)", html_text, re.DOTALL | re.IGNORECASE)
        if m:
            result["text"] = strip_html(m.group(1))

        # Extract child OIDs from links on the page
        # Only follow links in the main content/navigation area
        all_oids = set(re.findall(r'data\.aspx\?oid=(\d+)', html_text))
        result["child_oids"] = list(all_oids)

        return result

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """BFS crawl of all chapters, yielding leaf pages with full text."""
        total_yielded = 0
        sample_limit = 15 if sample else None

        for chapter_oid, chapter_id in CHAPTER_OIDS:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Crawling chapter %s (OID %s)...", chapter_id, chapter_oid)
            queue = deque([chapter_oid])

            while queue:
                if sample_limit and total_yielded >= sample_limit:
                    break

                oid = queue.popleft()
                if oid in self.visited:
                    continue
                self.visited.add(oid)

                html_text = self.fetch_page(oid)
                if not html_text:
                    continue

                page = self.parse_page(html_text)

                # Only yield leaf pages (children=0) with substantial content
                if page["children_count"] == 0 and page["text"] and len(page["text"]) > 100:
                    title = page["title"]
                    # Extract section ID from title (e.g., "A.A.1.2.3 Description")
                    section_id = ""
                    sm = re.match(r'^([A-Z]\.\w[\w.]*)', title)
                    if sm:
                        section_id = sm.group(1)

                    yield {
                        "_id": f"skat-djv-{oid}",
                        "_source": self.SOURCE_ID,
                        "_type": "doctrine",
                        "_fetched_at": datetime.now(timezone.utc).isoformat(),
                        "title": title,
                        "text": page["text"],
                        "date": page["date"],
                        "url": f"{PAGE_URL}?oid={oid}",
                        "language": "da",
                        "oid": oid,
                        "chapter": chapter_id,
                        "section_id": section_id,
                    }
                    total_yielded += 1

                    if total_yielded % 50 == 0:
                        logger.info("  Progress: %d leaf sections fetched (%d pages visited)",
                                    total_yielded, len(self.visited))

                # If page has children, add unvisited child OIDs to queue
                if page["children_count"] > 0:
                    for child_oid in page["child_oids"]:
                        if child_oid not in self.visited:
                            queue.append(child_oid)

            logger.info("  Chapter %s done. Yielded: %d, Visited: %d",
                        chapter_id, total_yielded, len(self.visited))

        logger.info("Fetch complete. %d leaf sections from %d pages visited", total_yielded, len(self.visited))

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """DJV is replaced wholesale each version — just re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            html_text = self.fetch_page("124")
            if html_text and "juridiske vejledning" in html_text.lower():
                logger.info("Test passed: root page accessible")
                return True
            logger.error("Test failed: unexpected content")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DK/Skattestyrelsen-Vejledning bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SkatVejledning()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s | %s | text=%d chars",
                        count, record.get("section_id", ""), record["title"][:60], text_len)

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        count = sum(1 for _ in scraper.fetch_all())
        logger.info("Update complete: %d sections", count)


if __name__ == "__main__":
    main()
