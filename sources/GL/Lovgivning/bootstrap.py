#!/usr/bin/env python3
"""
GL/Lovgivning -- Greenland Legislation (Nalunaarutit)

Fetches legislation from nalunaarutit.gl via the Sitecore search API.
Coverage: ~3,076 items (Greenlandic + Danish national legislation).
Full text extracted from HTML pages and PDF documents.

Data source: https://nalunaarutit.gl/
License: Public government data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import html as html_mod
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GL.Lovgivning")

SOURCE_ID = "GL/Lovgivning"
BASE_URL = "https://nalunaarutit.gl"
SEARCH_URL = f"{BASE_URL}/api/ruleset/search"


def _slugify(text: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s_]+", "-", slug)
    return slug[:80].strip("-")


class LovgivningScraper(BaseScraper):
    """
    Scraper for GL/Lovgivning -- Greenland Legislation (Nalunaarutit).
    Country: GL
    URL: https://nalunaarutit.gl/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (Legal Research)",
                "Accept": "application/json, text/html",
                "Accept-Language": "da,en;q=0.9",
            },
            max_retries=4,
            backoff_factor=3.0,
            timeout=60,
        )

    def _search_page(self, page: int, retries: int = 2) -> Optional[dict]:
        payload = {
            "PageNumber": page,
            "query": "",
            "number": "",
            "year": "",
            "orderdesc": True,
        }
        for attempt in range(1, retries + 1):
            try:
                resp = self.client.post(
                    f"{SEARCH_URL}?sc_lang=da",
                    json_data=payload,
                    timeout=90,
                )
                resp.raise_for_status()
                data = resp.json()
                # Guard: API sometimes returns a JSON string instead of object
                if not isinstance(data, dict):
                    logger.warning(
                        f"Search page {page}: expected dict, got {type(data).__name__} "
                        f"(preview: {str(data)[:120]})"
                    )
                    if attempt < retries:
                        time.sleep(5 * attempt)
                        continue
                    return None
                return data
            except Exception as e:
                logger.warning(f"Search page {page} attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    time.sleep(5 * attempt)
        return None

    def _iter_search_pages(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Yield one page of items at a time from the search API."""
        page = 1
        first = self._search_page(page)
        if not first:
            logger.error("Search API returned no data on page 1")
            return

        total = first.get("NumberOfResults", 0)
        total_pages = first.get("NumberOfPages", 1)
        first_items = first.get("Items", [])
        logger.info(f"Total: {total} items across {total_pages} pages")

        if first_items:
            yield first_items

        consecutive_failures = 0
        while page < total_pages:
            page += 1
            time.sleep(1.5)
            data = self._search_page(page)
            if not data or not data.get("Items"):
                consecutive_failures += 1
                logger.warning(f"Page {page}/{total_pages} empty (consecutive failures: {consecutive_failures})")
                if consecutive_failures >= 5:
                    logger.error(f"Stopping pagination after {consecutive_failures} consecutive failures")
                    break
                continue
            consecutive_failures = 0
            yield data["Items"]
            if page % 10 == 0:
                logger.info(f"Fetched page {page}/{total_pages}")

    def _extract_page_text(self, url: str) -> Optional[str]:
        full_url = f"{BASE_URL}{url}" if url.startswith("/") else url
        try:
            resp = self.client.get(full_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {full_url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["nav", "header", "footer", "script", "style", "noscript"]):
            tag.decompose()

        text_parts = []
        main = soup.find("main")
        if main:
            paragraphs = main.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"])
            lines = []
            for p in paragraphs:
                text = p.get_text(" ", strip=True)
                if not text or len(text) < 3:
                    continue
                if text in ("Del link", "Print", "Downloade", "Tilbage til søgning",
                            "Gældende", "Historisk", "kl-GL", "da"):
                    continue
                if re.match(r"^Nr\.\s*\d+$", text):
                    continue
                lines.append(text)

            if lines:
                deduped = [lines[0]]
                for line in lines[1:]:
                    if line != deduped[-1]:
                        deduped.append(line)
                html_text = "\n\n".join(deduped)
                if len(html_text) > 100:
                    text_parts.append(html_text)

        pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE))
        for pdf_link in pdf_links:
            href = pdf_link.get("href", "")
            if href.startswith("/"):
                href = BASE_URL + href
            if not href.startswith("http"):
                continue
            try:
                pdf_resp = self.client.get(href, timeout=90)
                pdf_resp.raise_for_status()
                if len(pdf_resp.content) > 100:
                    md = extract_pdf_markdown(
                        source=SOURCE_ID,
                        source_id=url.split("/")[-1].split("?")[0],
                        pdf_bytes=pdf_resp.content,
                        table="legislation",
                    )
                    if md and md.strip() and len(md.strip()) > 50:
                        text_parts.append(md.strip())
            except Exception:
                pass
            time.sleep(1.0)

        return "\n\n---\n\n".join(text_parts) if text_parts else None

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        item_id = raw.get("Id", "").strip("{}")
        name = raw.get("Name", "")
        title = raw.get("Title", name)
        url = raw.get("Url", "")
        text = raw.get("_extracted_text", "")

        if not text:
            return None

        pub_date = raw.get("PublicationDate")
        date_str = None
        if pub_date:
            try:
                dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass

        group = ""
        if raw.get("Group") and raw["Group"].get("Name"):
            group = raw["Group"]["Name"]

        state = ""
        if raw.get("State") and raw["State"].get("Name"):
            state = raw["State"]["Name"]

        subjects = []
        if raw.get("RulesetSubjects") and raw["RulesetSubjects"].get("Items"):
            for s in raw["RulesetSubjects"]["Items"]:
                if s.get("Name"):
                    subjects.append(s["Name"])

        parent_group = ""
        if raw.get("ParentGroup") and raw["ParentGroup"].get("Name"):
            parent_group = raw["ParentGroup"]["Name"]

        return {
            "_id": f"GL/Lovgivning/{item_id or _slugify(name)[:60]}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": html_mod.unescape(title) if title else "",
            "text": html_mod.unescape(text) if text else "",
            "date": date_str,
            "number": raw.get("Number"),
            "year": raw.get("YearValue"),
            "group": group,
            "parent_group": parent_group,
            "state": state,
            "subjects": subjects,
            "language": raw.get("Language", "da"),
            "url": f"{BASE_URL}{url}" if url.startswith("/") else url,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield items incrementally, processing each page before fetching the next."""
        count = 0
        for page_items in self._iter_search_pages():
            for item in page_items:
                if not isinstance(item, dict):
                    logger.warning(f"Skipping non-dict item: {type(item).__name__}")
                    continue

                count += 1
                title = item.get("Title", item.get("Name", "?"))[:70]
                logger.info(f"[{count}] {title}")

                url = item.get("Url", "")
                if not url:
                    continue

                text = self._extract_page_text(url)
                time.sleep(1.5)

                if not text or len(text) < 100:
                    logger.debug(f"Skipping: insufficient text ({len(text) if text else 0} chars)")
                    continue

                item["_extracted_text"] = text
                yield item

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        data = self._search_page(1)
        if isinstance(data, dict) and data.get("NumberOfResults", 0) > 0:
            logger.info(f"Test OK: {data['NumberOfResults']} total items")
            return True
        logger.error(f"Test failed: no results from search API (response type: {type(data).__name__})")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GL/Lovgivning Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for testing")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LovgivningScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample)

    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
