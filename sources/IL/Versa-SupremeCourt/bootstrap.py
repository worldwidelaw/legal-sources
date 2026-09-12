#!/usr/bin/env python3
"""
IL/Versa-SupremeCourt -- English translations of Israeli Supreme Court decisions

Fetches curated English translations from the Cardozo Israeli Supreme Court
Project (versa.cardozo.yu.edu). Discovers opinion URLs via the site map page,
then scrapes individual opinion pages for metadata and full text.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import json
import logging
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IL.Versa-SupremeCourt")

BASE_URL = "https://versa.cardozo.yu.edu"
SITE_MAP_URL = f"{BASE_URL}/site-map"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/html, */*",
}


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True
        elif tag in ("br", "p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr"):
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        elif tag == "p":
            self._parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._parts)
        # Collapse runs of whitespace but preserve paragraph breaks
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def _strip_html(html: str) -> str:
    """Convert HTML to plain text."""
    parser = _HTMLTextExtractor()
    parser.feed(html)
    return parser.get_text()


def _extract_between(html: str, start_marker: str, end_marker: str) -> Optional[str]:
    """Extract content between two markers in HTML."""
    idx = html.find(start_marker)
    if idx == -1:
        return None
    idx += len(start_marker)
    end_idx = html.find(end_marker, idx)
    if end_idx == -1:
        return None
    return html[idx:end_idx]


class ILVersaSupremeCourtScraper(BaseScraper):
    """Scraper for IL/Versa-SupremeCourt."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_HEADERS)

            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _discover_opinion_urls(self) -> list:
        """Discover all opinion URLs from the site map page."""
        sess = self._get_session()
        logger.info(f"Fetching site map: {SITE_MAP_URL}")
        resp = sess.get(SITE_MAP_URL, timeout=60)
        resp.raise_for_status()

        # Extract all /opinions/... links
        pattern = re.compile(r'href="(/opinions/[^"]+)"')
        urls = sorted(set(pattern.findall(resp.text)))
        logger.info(f"Discovered {len(urls)} opinion URLs from site map")
        return urls

    def _parse_opinion_page(self, html: str, url: str) -> Optional[Dict[str, Any]]:
        """Parse a single opinion page HTML into a structured record."""
        record = {}

        # Title
        m = re.search(r'<h1[^>]*class="page__title[^"]*"[^>]*>(.*?)</h1>', html, re.DOTALL)
        if m:
            record["title"] = _strip_html(m.group(1)).strip()

        # Case/docket number
        m = re.search(
            r'views-field-field-case-docket-number.*?field-content">(.*?)</div>',
            html, re.DOTALL
        )
        if m:
            record["case_number"] = _strip_html(m.group(1)).strip()

        # Date decided
        m = re.search(r'content="(\d{4}-\d{2}-\d{2})T', html)
        if m:
            record["date"] = m.group(1)

        # Decision type
        m = re.search(
            r'views-field-field-decision-type.*?field-content">(.*?)</div>',
            html, re.DOTALL
        )
        if m:
            record["decision_type"] = _strip_html(m.group(1)).strip()

        # Topics
        m = re.search(
            r'views-field-field-topics.*?field-content">(.*?)</div>\s*</div>',
            html, re.DOTALL
        )
        if m:
            topics_raw = _strip_html(m.group(1)).strip()
            record["topics"] = [t.strip() for t in topics_raw.split(",") if t.strip()]

        # Abstract
        m = re.search(
            r'views-field-field-abstract.*?field-content">(.*?)</div>\s*</div>',
            html, re.DOTALL
        )
        if m:
            record["abstract"] = _strip_html(m.group(1)).strip()

        # Full opinion text (body field)
        m = re.search(
            r'field-name-body.*?field-item even[^>]*>(.*?)</div>\s*</div>\s*</div>',
            html, re.DOTALL
        )
        if m:
            record["text"] = _strip_html(m.group(1)).strip()

        # PDF link
        m = re.search(r"href='([^']*\.pdf)'", html)
        if not m:
            m = re.search(r'href="([^"]*\.pdf)"', html)
        if m:
            pdf_url = m.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url
            record["pdf_url"] = pdf_url

        # Original Hebrew link
        m = re.search(
            r'views-field-field-link.*?href="(http[^"]*)"',
            html, re.DOTALL
        )
        if m:
            record["original_url"] = m.group(1)

        record["url"] = url
        return record if record.get("title") else None

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all opinions."""
        sess = self._get_session()
        opinion_paths = self._discover_opinion_urls()

        if sample:
            opinion_paths = opinion_paths[:15]

        for i, path in enumerate(opinion_paths):
            url = f"{BASE_URL}{path}"
            try:
                self.rate_limiter.wait()
                resp = sess.get(url, timeout=60)
                resp.raise_for_status()

                record = self._parse_opinion_page(resp.text, url)
                if record and record.get("text"):
                    yield record
                    logger.info(
                        f"[{i+1}/{len(opinion_paths)}] {record.get('title', 'unknown')[:60]} "
                        f"({len(record.get('text', ''))} chars)"
                    )
                elif record:
                    logger.warning(f"[{i+1}] No full text for: {url}")
                else:
                    logger.warning(f"[{i+1}] Failed to parse: {url}")

            except Exception as e:
                logger.error(f"[{i+1}] Error fetching {url}: {e}")
                continue

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates — for this source, re-fetches all (small corpus)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record into standard schema."""
        title = raw.get("title", "")
        case_num = raw.get("case_number", "")

        # Build stable ID from case number or URL slug
        if case_num:
            id_seed = case_num
        else:
            id_seed = raw.get("url", title)
        _id = hashlib.sha256(id_seed.encode("utf-8")).hexdigest()[:16]

        return {
            "_id": _id,
            "_source": "IL/Versa-SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "case_number": case_num,
            "decision_type": raw.get("decision_type"),
            "topics": raw.get("topics", []),
            "abstract": raw.get("abstract", ""),
            "pdf_url": raw.get("pdf_url"),
            "original_url": raw.get("original_url"),
            "language": "en",
            "jurisdiction": "IL",
            "court": "Supreme Court of Israel",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IL/Versa-SupremeCourt bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILVersaSupremeCourtScraper()

    if args.command == "test":
        sess = scraper._get_session()
        resp = sess.get(SITE_MAP_URL, timeout=30)
        print(f"Site map status: {resp.status_code}")
        opinion_count = resp.text.count('href="/opinions/')
        print(f"Opinion links found: {opinion_count}")
        return

    sample_mode = args.sample and not args.full
    sample_dir = scraper.source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for raw in scraper.fetch_all(sample=sample_mode):
        record = scraper.normalize(raw)

        if not record.get("text"):
            continue

        out_path = sample_dir / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        count += 1
        if sample_mode and count >= 15:
            break

    logger.info(f"Done. Saved {count} records to {sample_dir}")
    print(f"\nTotal records: {count}")
    print(f"Sample dir: {sample_dir}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
