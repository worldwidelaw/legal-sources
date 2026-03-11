"""
World Wide Law - Azerbaijan Supreme Court Scraper

Fetches case law from the Azerbaijan Supreme Court (Ali Məhkəməsi).
Data source: https://sc.supremecourt.gov.az/decision-search/
Method: JSON API for search + detail endpoint for full text
Coverage: 39,000+ decisions from Supreme Court cassation panels
Language: Azerbaijani

The API provides:
- POST /decision-search/ - paginated list of decisions
- GET /decision-search/show/{work_no} - full text HTML for individual decisions
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from html import unescape
from urllib.parse import quote

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("AZ/SupremeCourt")


class AzerbaijanSupremeCourtScraper(BaseScraper):
    """
    Scraper for: Azerbaijan Supreme Court
    Country: AZ
    URL: https://sc.supremecourt.gov.az

    Data types: case_law
    Auth: none

    The court publishes decisions via a Vue.js search interface backed by
    a JSON API. Decisions are returned as HTML extracted from PDF documents.
    ~39,000+ decisions available covering cassation appeals.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", "https://sc.supremecourt.gov.az"),
            headers={
                **self._auth_headers,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "az,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            verify=True,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions by iterating through paginated API.
        Starts from page 1 (newest) through all available pages.
        """
        page = 1
        per_page = 50  # API supports up to 100
        seen_ids = set()
        total = None

        while True:
            logger.info(f"Fetching decisions page {page}")
            try:
                result = self._fetch_list_page(page, per_page)

                if not result or "tableDatas" not in result:
                    logger.warning(f"Invalid API response at page {page}")
                    break

                table_data = result["tableDatas"]
                decisions = table_data.get("data", [])

                if total is None:
                    total = table_data.get("total", 0)
                    logger.info(f"Total decisions available: {total}")

                if not decisions:
                    logger.info(f"No more decisions found at page {page}")
                    break

                for decision in decisions:
                    work_no = decision.get("am_regn") or decision.get("job_regn")
                    if not work_no or work_no in seen_ids:
                        continue

                    seen_ids.add(work_no)

                    try:
                        doc = self._fetch_decision_detail(work_no, decision)
                        if doc and doc.get("full_text"):
                            yield doc
                    except Exception as e:
                        logger.warning(f"Failed to fetch decision {work_no}: {e}")
                        continue

                page += 1

                # Stop if we've processed all available
                if len(seen_ids) >= total:
                    logger.info(f"Processed all {total} decisions")
                    break

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        Only fetches recent pages until we find older content.
        """
        page = 1
        per_page = 50
        seen_ids = set()

        while page <= 20:  # Limit update checks
            logger.info(f"Fetching updates page {page}")
            try:
                result = self._fetch_list_page(page, per_page)

                if not result or "tableDatas" not in result:
                    break

                decisions = result["tableDatas"].get("data", [])
                if not decisions:
                    break

                found_old = False
                for decision in decisions:
                    work_no = decision.get("am_regn") or decision.get("job_regn")
                    if not work_no or work_no in seen_ids:
                        continue

                    seen_ids.add(work_no)

                    # Check date
                    date_str = decision.get("am_date4", "")
                    if date_str:
                        doc_date = self._parse_date_az(date_str)
                        if doc_date and doc_date < since:
                            found_old = True
                            continue

                    try:
                        doc = self._fetch_decision_detail(work_no, decision)
                        if doc and doc.get("full_text"):
                            yield doc
                    except Exception as e:
                        logger.warning(f"Failed to fetch decision {work_no}: {e}")
                        continue

                if found_old:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

    def _fetch_list_page(self, page: int, per_page: int = 50) -> dict:
        """Fetch list of decisions from a single page."""
        self.rate_limiter.wait()

        # POST to search endpoint
        data = {
            "page": page,
            "perpage": per_page,
            "sort": "am_date4",
            "order": "desc",
        }

        resp = self.client.post(
            "/decision-search/",
            data=data,
        )

        return resp.json()

    def _fetch_decision_detail(self, work_no: str, list_data: dict) -> dict:
        """
        Fetch a single decision by work number and extract full text.

        Returns raw document dict with full text.
        """
        self.rate_limiter.wait()

        # URL encode the work_no (double-encoded as the API expects)
        encoded_work_no = quote(quote(work_no, safe=''), safe='')

        resp = self.client.get(
            f"/decision-search/show/{encoded_work_no}",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )

        detail = resp.json()

        # Extract decision details
        decision_data = detail.get("decision", {})

        # Get the HTML text content
        html_text = decision_data.get("text", "")

        if not html_text:
            logger.warning(f"No text content for decision {work_no}")
            return None

        # Clean HTML to plain text
        full_text = self._extract_clean_text(html_text)

        if not full_text or len(full_text) < 100:
            logger.warning(f"Insufficient text for decision {work_no}: {len(full_text) if full_text else 0} chars")
            return None

        # Parse date from list data (format: DD.MM.YYYY)
        date_str = list_data.get("am_date4", "")
        date_iso = None
        if date_str:
            date_iso = self._parse_date_az(date_str)
            if date_iso:
                date_iso = date_iso.strftime("%Y-%m-%d")

        # Get decision type
        decision_type = list_data.get("am_decision_type", "")

        # Get result/outcome
        result = list_data.get("am_result", "")

        # Get judge
        judge = list_data.get("am_judge", "")

        # Get category (am_col: 1=Criminal, 2=Civil, 3=Commercial, 4=Administrative)
        category_map = {
            "1": "criminal",
            "2": "civil",
            "3": "commercial",
            "4": "administrative",
        }
        category = category_map.get(str(list_data.get("am_col", "")), "unknown")

        # Get importance rating
        star_rating = list_data.get("am_star", 0) or detail.get("star", 0)

        return {
            "work_no": work_no,
            "title": decision_data.get("title", f"{work_no} məhkəmə aktı"),
            "decision_type": decision_type,
            "category": category,
            "date": date_iso,
            "result": result,
            "judge": judge,
            "star_rating": star_rating,
            "full_text": full_text,
            "url": f"https://sc.supremecourt.gov.az/decision-search/show/{encoded_work_no}",
            "pdf_link": list_data.get("am_decision_link"),
        }

    def _extract_clean_text(self, html_content: str) -> str:
        """Extract clean text from the HTML content."""
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, "html.parser")

        # Remove style tags
        for style in soup.find_all("style"):
            style.decompose()

        # Get all text with proper spacing
        paragraphs = soup.find_all("p")

        if paragraphs:
            # Extract text from paragraphs maintaining order
            text_parts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    text_parts.append(text)
            text = "\n\n".join(text_parts)
        else:
            # Fallback to simple text extraction
            text = soup.get_text(separator="\n", strip=True)

        # Decode HTML entities
        text = unescape(text)

        # Clean up excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = text.replace("\xa0", " ")

        return text.strip()

    def _parse_date_az(self, date_str: str) -> datetime:
        """Parse Azerbaijani date format (DD.MM.YYYY) to datetime."""
        if not date_str:
            return None

        try:
            return datetime.strptime(date_str, "%d.%m.%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

        return None

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from document content.
        """
        work_no = raw.get("work_no", "")

        # Build title
        title = raw.get("title", "")
        if not title:
            title = f"Supreme Court Decision {work_no}"

        # Get full text
        full_text = raw.get("full_text", "")

        # Build unique ID from work_no
        safe_id = re.sub(r'[^\w\-]', '_', work_no)

        return {
            "_id": f"AZ/SupremeCourt/{safe_id}",
            "_source": "AZ/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get("date"),
            "url": raw.get("url"),

            # Source-specific fields
            "work_no": work_no,
            "decision_type": raw.get("decision_type"),
            "category": raw.get("category"),
            "result": raw.get("result"),
            "judge": raw.get("judge"),
            "star_rating": raw.get("star_rating"),
            "pdf_link": raw.get("pdf_link"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# -- CLI Entry Point ---

def main():
    scraper = AzerbaijanSupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
