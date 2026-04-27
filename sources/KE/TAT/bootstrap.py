#!/usr/bin/env python3
"""
KE/TAT -- Kenya Tax Appeals Tribunal decisions from Kenya Law

Fetches full-text TAT decisions from new.kenyalaw.org (PeachJam platform).
~2,000 judgments covering tax disputes between taxpayers and KRA.

Data access:
  - Listing at /judgments/KETAT/?page={N} (paginated, ~20 per page)
  - Each judgment has AKN URI: /akn/ke/judgment/ketat/{YEAR}/{NUM}/eng@{DATE}
  - PDF source: {akn_uri}/source.pdf
  - robots.txt specifies 5s crawl delay

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Same as bootstrap
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
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
logger = logging.getLogger("legal-data-hunter.KE.TAT")

BASE_URL = "https://new.kenyalaw.org"
COURT_CODE = "KETAT"
DELAY = 5.0  # robots.txt crawl delay


class KETATScraper(BaseScraper):
    """Scraper for Kenya Tax Appeals Tribunal decisions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            timeout=120,
        )

    def _get_page(self, url: str) -> str:
        """Fetch a page respecting crawl delay."""
        time.sleep(DELAY)
        resp = self.http.get(url, allow_redirects=True)
        resp.raise_for_status()
        return resp.text

    def _extract_judgment_links(self, html: str) -> list:
        """Extract judgment AKN links from a listing page."""
        links = []
        for match in re.finditer(r'href="(/akn/ke/judgment/[^"]+)"', html):
            path = match.group(1)
            if path not in [l["path"] for l in links]:
                links.append({"path": path, "url": f"{BASE_URL}{path}"})
        return links

    def _fetch_pdf(self, path: str) -> Optional[bytes]:
        """Download judgment PDF source file."""
        url = f"{BASE_URL}{path}/source.pdf"
        time.sleep(DELAY)
        try:
            resp = self.http.get(url, allow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning("PDF download failed for %s: %s", path, e)
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF."""
        return extract_pdf_markdown(
            source="KE/TAT",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _parse_akn_path(self, path: str) -> dict:
        """Parse AKN URI path into components."""
        m = re.match(
            r'/akn/ke/judgment/([^/]+)/(\d{4})/([^/]+)/eng@(\d{4}-\d{2}-\d{2})',
            path,
        )
        if m:
            return {
                "court": m.group(1),
                "year": m.group(2),
                "number": m.group(3),
                "date": m.group(4),
            }
        return {"court": COURT_CODE.lower(), "year": "", "number": "", "date": ""}

    def _extract_title_from_text(self, text: str) -> str:
        """Extract a title from the first meaningful line of PDF text."""
        for line in text[:500].split('\n'):
            line = line.strip()
            if len(line) > 10 and not line.startswith('http'):
                return line[:200]
        return ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw TAT judgment into standard schema."""
        path = raw.get("path", "")
        parsed = self._parse_akn_path(path)
        clean_path = path.replace("/akn/ke/judgment/", "").replace("/", "-").replace("@", "-")
        _id = f"KE-TAT-{clean_path}"

        return {
            "_id": _id,
            "_source": "KE/TAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": parsed.get("date"),
            "url": raw.get("url", ""),
            "court": "KETAT",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all TAT judgments with full text from PDF."""
        seen = set()
        page_num = 0
        empty_pages = 0

        while empty_pages < 3:
            page_num += 1
            url = f"{BASE_URL}/judgments/{COURT_CODE}/?page={page_num}"
            try:
                html = self._get_page(url)
            except Exception as e:
                logger.warning("Listing page %d failed: %s", page_num, e)
                empty_pages += 1
                continue

            links = self._extract_judgment_links(html)
            if not links:
                logger.info("No links on page %d, stopping", page_num)
                empty_pages += 1
                continue

            empty_pages = 0
            new_count = 0
            for link in links:
                if link["path"] in seen:
                    continue
                seen.add(link["path"])
                new_count += 1

                pdf_bytes = self._fetch_pdf(link["path"])
                if pdf_bytes is None:
                    continue

                text = self._extract_pdf_text(pdf_bytes)
                if not text or len(text) < 100:
                    logger.warning("Insufficient text from %s (%d chars)", link["path"], len(text) if text else 0)
                    continue

                parsed = self._parse_akn_path(link["path"])
                title = self._extract_title_from_text(text)
                if not title:
                    title = f"TAT Appeal {parsed.get('number', '')} of {parsed.get('year', '')}"

                yield {
                    "path": link["path"],
                    "url": link["url"],
                    "title": title,
                    "full_text": text,
                    "date": parsed.get("date"),
                }

            logger.info("Page %d: %d new judgments (total seen: %d)", page_num, new_count, len(seen))

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recent judgments (first 5 pages)."""
        seen = set()
        for page_num in range(1, 6):
            url = f"{BASE_URL}/judgments/{COURT_CODE}/?page={page_num}"
            try:
                html = self._get_page(url)
            except Exception as e:
                logger.warning("Update page %d failed: %s", page_num, e)
                break

            links = self._extract_judgment_links(html)
            if not links:
                break

            for link in links:
                if link["path"] in seen:
                    continue
                seen.add(link["path"])

                pdf_bytes = self._fetch_pdf(link["path"])
                if pdf_bytes is None:
                    continue

                text = self._extract_pdf_text(pdf_bytes)
                if not text or len(text) < 100:
                    continue

                parsed = self._parse_akn_path(link["path"])
                title = self._extract_title_from_text(text)
                if not title:
                    title = f"TAT Appeal {parsed.get('number', '')} of {parsed.get('year', '')}"

                yield {
                    "path": link["path"],
                    "url": link["url"],
                    "title": title,
                    "full_text": text,
                    "date": parsed.get("date"),
                }

    def test_connection(self) -> bool:
        """Test that we can access KETAT listings."""
        try:
            html = self._get_page(f"{BASE_URL}/judgments/{COURT_CODE}/?page=1")
            links = self._extract_judgment_links(html)
            logger.info("Connection test: %d judgment links on page 1", len(links))
            return len(links) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KE/TAT bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KETATScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    if args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            scraper.storage.save(record)
            count += 1
            if count % 10 == 0:
                logger.info("Saved %d records", count)
        logger.info("Update complete: %d records", count)
        return

    # bootstrap
    sample_dir = Path(__file__).resolve().parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.sample:
        count = 0
        target = 12
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            if record["text"] and len(record["text"]) > 100:
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(
                    "Sample %d/%d: %s (%d chars)",
                    count, target, record["_id"], len(record["text"])
                )
            if count >= target:
                break

        logger.info("Sample complete: %d records saved to %s", count, sample_dir)
    else:
        stats = scraper.bootstrap()
        logger.info("Bootstrap complete: %s", stats)


if __name__ == "__main__":
    main()
