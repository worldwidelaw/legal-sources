#!/usr/bin/env python3
"""
BS/URCA-Decisions -- Bahamas URCA Regulatory Decisions

Fetches decisions from URCA via WordPress REST API custom post type.

Strategy:
  - Use WP REST API /wp-json/wp/v2/decisions to enumerate all decisions
  - Extract full text from HTML content field (most have substantial content)
  - For records with insufficient HTML, download PDF and extract text
  - Classify by sector (ECS, ES, NGS, General)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.URCA-Decisions")

BASE_URL = "https://urcabahamas.bs"
API_URL = f"{BASE_URL}/wp-json/wp/v2/decisions"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

MIN_TEXT_CHARS = 200


def _strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r"<br\s*/?>", "\n", raw_html)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"<li[^>]*>", "\n- ", text)
    text = re.sub(r"</?(?:ul|ol)[^>]*>", "\n", text)
    text = re.sub(r"<h[1-6][^>]*>", "\n## ", text)
    text = re.sub(r"</h[1-6]>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _classify_sector(title: str) -> str:
    """Classify decision by sector based on title prefix."""
    title_upper = title.upper().strip()
    if title_upper.startswith("ECS") or "ELECTRONIC COMMUNICATIONS" in title_upper:
        return "ECS"
    if title_upper.startswith("ES ") or "ELECTRICITY" in title_upper:
        return "ES"
    if title_upper.startswith("NGS") or "NATURAL GAS" in title_upper:
        return "NGS"
    return "General"


def _extract_pdf_urls(html_content: str) -> List[str]:
    """Extract PDF URLs from HTML content."""
    return re.findall(r'href="(https?://[^"]+\.pdf)"', html_content)


class BSURCAScraper(BaseScraper):
    SOURCE_ID = "BS/URCA-Decisions"

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)

    def _api_get(self, url: str, timeout: int = 30) -> Optional[Any]:
        """Fetch JSON from the WP REST API."""
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError) as e:
            logger.warning(f"API request failed: {url} - {e}")
            return None

    def _list_decisions(self) -> List[Dict[str, Any]]:
        """Enumerate all decisions via WP REST API."""
        all_decisions = []
        page = 1
        while True:
            url = f"{API_URL}?per_page=100&page={page}&_fields=id,title,date,link,content"
            data = self._api_get(url)
            if not data or len(data) == 0:
                break

            for item in data:
                wp_id = item.get("id", "")
                title_raw = item.get("title", {}).get("rendered", "")
                title = html.unescape(title_raw)
                date_str = item.get("date", "")
                link = item.get("link", "")
                content_html = item.get("content", {}).get("rendered", "")

                all_decisions.append({
                    "id": str(wp_id),
                    "title": title,
                    "date": date_str[:10] if date_str else None,
                    "link": link,
                    "content_html": content_html,
                    "sector": _classify_sector(title),
                })

            logger.info(f"API page {page}: {len(data)} decisions (total: {len(all_decisions)})")
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)

        return all_decisions

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions with full text."""
        decisions = self._list_decisions()
        logger.info(f"Total decisions to process: {len(decisions)}")

        for dec in decisions:
            doc_id = f"urca_{dec['id']}"

            # Try HTML content first
            text = _strip_html(dec["content_html"])

            # If HTML text is too short, try PDF extraction
            if len(text) < MIN_TEXT_CHARS:
                pdf_urls = _extract_pdf_urls(dec["content_html"])
                if pdf_urls:
                    time.sleep(1.5)
                    pdf_text = extract_pdf_markdown(
                        source=self.SOURCE_ID,
                        source_id=doc_id,
                        pdf_url=pdf_urls[0],
                        table="doctrine",
                    )
                    if pdf_text and len(pdf_text.strip()) > len(text):
                        text = pdf_text.strip()

            if len(text) < MIN_TEXT_CHARS:
                logger.warning(f"Skipping {dec['title'][:60]}: insufficient text ({len(text)} chars)")
                continue

            yield {
                "doc_id": doc_id,
                "title": dec["title"],
                "text": text,
                "url": dec["link"],
                "date": dec["date"],
                "sector": dec["sector"],
            }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions published after a given date."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        decisions = self._list_decisions()
        for dec in decisions:
            if dec.get("date") and dec["date"] >= since:
                doc_id = f"urca_{dec['id']}"
                text = _strip_html(dec["content_html"])
                if len(text) < MIN_TEXT_CHARS:
                    pdf_urls = _extract_pdf_urls(dec["content_html"])
                    if pdf_urls:
                        time.sleep(1.5)
                        pdf_text = extract_pdf_markdown(
                            source=self.SOURCE_ID,
                            source_id=doc_id,
                            pdf_url=pdf_urls[0],
                            table="doctrine",
                        )
                        if pdf_text and len(pdf_text.strip()) > len(text):
                            text = pdf_text.strip()
                if len(text) < MIN_TEXT_CHARS:
                    continue
                yield {
                    "doc_id": doc_id,
                    "title": dec["title"],
                    "text": text,
                    "url": dec["link"],
                    "date": dec["date"],
                    "sector": dec["sector"],
                }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "sector": raw.get("sector", "General"),
        }


# ─── CLI Entry Point ─────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="BS/URCA-Decisions bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = BSURCAScraper()

    if args.command == "test":
        decisions = scraper._list_decisions()
        print(f"OK: Found {len(decisions)} decisions via WP REST API")
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for raw in scraper.fetch_all():
        record = scraper.normalize(raw)
        count += 1
        fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        logger.info(f"[{count}] {record['title'][:60]} ({text_len} chars)")

        if count >= limit:
            logger.info(f"Sample limit reached ({limit} records)")
            break

    print(f"\nDone: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
