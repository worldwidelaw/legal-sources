#!/usr/bin/env python3
"""
MK/KZK -- North Macedonia Commission for Protection of Competition — Decisions

Fetches decisions from the Commission for Protection of Competition
(Комисија за заштита на конкуренцијата) at https://kzk.gov.mk/.

Strategy:
  1. Scrape paginated Joomla blog listings for each decision category
  2. Extract metadata (title, date, PDF URL) from article HTML
  3. Download each PDF and extract full text via pdfplumber
  4. Normalize into standard schema

Categories:
  - Competition: admin procedure, violation procedure, court decisions
  - State aid: acts/decisions, opinions
  - Unfair commercial practices: violation procedure, court decisions

License: Public Domain (Government decisions)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent pages)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MK.KZK")

BASE_URL = "https://kzk.gov.mk"
SOURCE_ID = "MK/KZK"

# Decision category listing pages (Joomla blog layout)
CATEGORIES = [
    {
        "name": "competition_admin",
        "label": "Competition — Administrative procedure",
        "path": "/одлуки/конкуренција/odluki-vo-upravna-postapka/",
    },
    {
        "name": "competition_violation",
        "label": "Competition — Violation procedure",
        "path": "/одлуки/конкуренција/odluki-vo-prekrsocna-postapka/",
    },
    {
        "name": "competition_court",
        "label": "Competition — Court decisions",
        "path": "/одлуки/конкуренција/odluki-vo-sudovite/",
    },
    {
        "name": "state_aid_acts",
        "label": "State aid — Acts/Decisions",
        "path": "/одлуки/државна-помош/2026-03-25-08-33-06/",
    },
    {
        "name": "state_aid_opinions",
        "label": "State aid — Opinions",
        "path": "/одлуки/државна-помош/2026-03-25-08-33-47/",
    },
    {
        "name": "unfair_practices_violation",
        "label": "Unfair commercial practices — Violation procedure",
        "path": "/одлуки/нефер-трговски-практики/odluki-vo-prekrsocna-postapka/",
    },
    {
        "name": "unfair_practices_court",
        "label": "Unfair commercial practices — Court decisions",
        "path": "/одлуки/нефер-трговски-практики/odluki-na-sudovi/",
    },
]


class KZKScraper(BaseScraper):
    """
    Scraper for MK/KZK — North Macedonia Competition Commission.
    Country: MK
    URL: https://kzk.gov.mk/

    Data types: case_law
    Auth: none
    """

    SOURCE_ID = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            verify=True,
        )

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.http.get(BASE_URL, timeout=15)
            if resp.status_code == 200 and "Комисија" in resp.text:
                logger.info("Connectivity OK — KZK homepage accessible")
                return True
            logger.error(f"Unexpected response: {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def _get_max_start(self, html: str) -> int:
        """Extract the maximum ?start= value from pagination links."""
        starts = re.findall(r"\?start=(\d+)", html)
        if starts:
            return max(int(s) for s in starts)
        return 0

    def _parse_listing_page(self, html: str, category: str) -> List[Dict[str, Any]]:
        """Parse a Joomla blog listing page, returning list of article dicts."""
        results = []
        # Each article is in <div class="article" ...>...</div>
        articles = re.split(r'<div class="article"[^>]*>', html)
        for article_html in articles[1:]:  # skip first (before first article)
            try:
                record = self._parse_article(article_html, category)
                if record:
                    results.append(record)
            except Exception as e:
                logger.warning(f"Failed to parse article: {e}")
        return results

    def _parse_article(self, html: str, category: str) -> Optional[Dict[str, Any]]:
        """Parse a single article block from the listing."""
        # Title and detail URL
        title_match = re.search(
            r'<h2>\s*<a\s+href="([^"]+)"[^>]*>\s*(.*?)\s*</a>\s*</h2>',
            html, re.DOTALL,
        )
        if not title_match:
            return None

        detail_path = title_match.group(1)
        title = re.sub(r"<[^>]+>", "", title_match.group(2)).strip()
        title = re.sub(r"\s+", " ", title)

        # Extract slug from URL path
        slug = detail_path.rstrip("/").split("/")[-1]

        # PDF URL from iframe
        pdf_match = re.search(r'<iframe[^>]+src="([^"]+\.pdf)"', html)
        pdf_url = None
        if pdf_match:
            pdf_url = pdf_match.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url

        # Date from <time datetime="...">
        date_match = re.search(r'<time\s+datetime="([^"]+)"', html)
        date_str = None
        if date_match:
            try:
                dt = datetime.fromisoformat(date_match.group(1).replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        if not pdf_url:
            logger.warning(f"No PDF found for: {title[:60]}")
            return None

        return {
            "slug": slug,
            "title": title,
            "pdf_url": pdf_url,
            "detail_url": BASE_URL + detail_path if not detail_path.startswith("http") else detail_path,
            "date": date_str,
            "category": category,
        }

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        if pdfplumber is None:
            logger.error("pdfplumber not installed — cannot extract PDF text")
            return None
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed: {resp.status_code} for {pdf_url}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
                tmp.write(resp.content)
                tmp.flush()
                with pdfplumber.open(tmp.name) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            pages_text.append(t)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass
                    return "\n\n".join(pages_text) if pages_text else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw decision record into standard schema."""
        slug = raw.get("slug") or "unknown"
        doc_id = f"MK-KZK-{slug}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("detail_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "category": raw.get("category", ""),
            "slug": slug,
        }

    def _scrape_category(
        self, cat: Dict[str, Any], sample: bool = False, sample_count: int = 0
    ) -> Generator[Dict[str, Any], None, None]:
        """Scrape all pages of a single category."""
        cat_name = cat["name"]
        cat_label = cat["label"]
        cat_path = cat["path"]
        url = BASE_URL + cat_path

        logger.info(f"Scraping category: {cat_label}")

        try:
            resp = self.http.get(url, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"Category {cat_label} returned {resp.status_code}")
                return
        except Exception as e:
            logger.warning(f"Failed to fetch category {cat_label}: {e}")
            return

        max_start = self._get_max_start(resp.text)
        items_per_page = 5  # Joomla default
        total_pages = (max_start // items_per_page) + 1 if max_start > 0 else 1
        logger.info(f"  {cat_label}: {total_pages} pages (max_start={max_start})")

        count = sample_count
        for page_idx in range(total_pages):
            start = page_idx * items_per_page
            page_url = f"{url}?start={start}" if start > 0 else url

            if page_idx > 0:
                try:
                    resp = self.http.get(page_url, timeout=15)
                    if resp.status_code != 200:
                        logger.warning(f"Page start={start} returned {resp.status_code}")
                        continue
                except Exception as e:
                    logger.warning(f"Failed to fetch page start={start}: {e}")
                    continue

            articles = self._parse_listing_page(resp.text, cat_name)
            if not articles:
                logger.warning(f"No articles found on page start={start}")
                continue

            for article in articles:
                text = self._extract_pdf_text(article["pdf_url"])
                if not text:
                    logger.warning(f"No text for: {article['title'][:60]}")
                    continue

                article["text"] = text
                record = self.normalize(article)
                yield record
                count += 1

                if sample and count >= 12:
                    return

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions from all categories."""
        count = 0
        for cat in CATEGORIES:
            for record in self._scrape_category(cat, sample=sample, sample_count=count):
                yield record
                count += 1
                if sample and count >= 12:
                    logger.info(f"Sample complete: {count} records")
                    return
        logger.info(f"Fetch complete: {count} records")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first page of each category)."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        logger.info(f"Fetching updates since {since}...")
        try:
            since_date = datetime.strptime(since, "%Y-%m-%d")
        except ValueError:
            since_date = datetime(2024, 1, 1)

        count = 0
        for cat in CATEGORIES:
            cat_path = cat["path"]
            url = BASE_URL + cat_path

            try:
                resp = self.http.get(url, timeout=15)
                if resp.status_code != 200:
                    continue
            except Exception:
                continue

            articles = self._parse_listing_page(resp.text, cat["name"])
            for article in articles:
                if article.get("date"):
                    try:
                        art_date = datetime.strptime(article["date"], "%Y-%m-%d")
                        if art_date < since_date:
                            continue
                    except ValueError:
                        pass

                text = self._extract_pdf_text(article["pdf_url"])
                if not text:
                    continue
                article["text"] = text
                yield self.normalize(article)
                count += 1

        logger.info(f"Update done: {count} records since {since}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MK/KZK Bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", type=str, help="Date for incremental update")
    args = parser.parse_args()

    scraper = KZKScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            if args.sample:
                out_file = sample_dir / f"{count:04d}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    f"[{count+1}] {record['slug']} — "
                    f"{len(record.get('text',''))} chars"
                )
            else:
                print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Done: {count} records")

    elif args.command == "update":
        since = args.since or "2024-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Update done: {count} records since {since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
