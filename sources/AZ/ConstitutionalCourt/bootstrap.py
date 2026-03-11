"""
World Wide Law - Azerbaijan Constitutional Court Scraper

Fetches case law from the Azerbaijan Constitutional Court (Konstitusiya Məhkəməsi).
Data source: https://www.constcourt.gov.az
Method: HTML scraping via decisions list pagination + detail pages
Coverage: 1998 onwards (Constitutional Court decisions)
Language: Azerbaijani

The website provides:
- Paginated list of decisions at /az/decisions?page=N
- Individual decision pages at /az/decision/{id}
- Word document downloads at /az/decisionDocx/{id}
- Full text embedded in HTML (no JS required)
"""

import re
import sys
import json
import logging
import ssl
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from html import unescape

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
logger = logging.getLogger("AZ/ConstitutionalCourt")


class AzerbaijanConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for: Azerbaijan Constitutional Court
    Country: AZ
    URL: https://www.constcourt.gov.az

    Data types: case_law
    Auth: none

    The court publishes decisions in Azerbaijani language.
    Decisions are organized in paginated lists with ~15 per page.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", "https://www.constcourt.gov.az"),
            headers={
                **self._auth_headers,
                "Accept-Language": "az,en;q=0.9",
            },
            verify=False,  # Site has certificate issues
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions by iterating through paginated list.
        Starts from page 1 (newest) through all available pages.
        """
        page = 1
        seen_ids = set()

        while True:
            logger.info(f"Fetching decisions page {page}")
            try:
                decision_ids = self._fetch_list_page(page)

                if not decision_ids:
                    logger.info(f"No more decisions found at page {page}")
                    break

                # Filter out already seen IDs (in case of duplicates)
                new_ids = [d for d in decision_ids if d not in seen_ids]
                if not new_ids:
                    logger.info(f"All decisions on page {page} already seen")
                    break

                for decision_id in new_ids:
                    seen_ids.add(decision_id)
                    try:
                        doc = self._fetch_decision(decision_id)
                        if doc and doc.get("full_text"):
                            yield doc
                    except Exception as e:
                        logger.warning(f"Failed to fetch decision {decision_id}: {e}")
                        continue

                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        Only fetches recent pages until we find older content.
        """
        page = 1
        seen_ids = set()

        while page <= 10:  # Limit update checks to first 10 pages
            logger.info(f"Fetching updates page {page}")
            try:
                decision_ids = self._fetch_list_page(page)

                if not decision_ids:
                    break

                new_ids = [d for d in decision_ids if d not in seen_ids]
                if not new_ids:
                    break

                found_old = False
                for decision_id in new_ids:
                    seen_ids.add(decision_id)
                    try:
                        doc = self._fetch_decision(decision_id)
                        if doc:
                            # Check if document is newer than since date
                            date_str = doc.get("date", "")
                            if date_str:
                                doc_date = self._parse_date(date_str)
                                if doc_date and doc_date < since:
                                    found_old = True
                                    continue
                            if doc.get("full_text"):
                                yield doc
                    except Exception as e:
                        logger.warning(f"Failed to fetch decision {decision_id}: {e}")
                        continue

                if found_old:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

    def _fetch_list_page(self, page: int) -> list:
        """Fetch list of decision IDs from a single page."""
        self.rate_limiter.wait()
        resp = self.client.get(f"/az/decisions?page={page}")
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find all decision links in format /az/decision/XXXX
        decision_ids = []
        links = soup.find_all("a", href=re.compile(r"/az/decision/\d+"))

        for link in links:
            href = link.get("href", "")
            match = re.search(r"/az/decision/(\d+)", href)
            if match:
                decision_id = match.group(1)
                if decision_id not in decision_ids:
                    decision_ids.append(decision_id)

        logger.info(f"Found {len(decision_ids)} decisions on page {page}")
        return decision_ids

    def _fetch_decision(self, decision_id: str) -> dict:
        """
        Fetch a single decision by ID and extract all content.

        Returns raw document dict with full text.
        """
        self.rate_limiter.wait()
        resp = self.client.get(f"/az/decision/{decision_id}")
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title from the italic paragraph
        title = ""
        title_elem = soup.find("p", class_="italic")
        if title_elem:
            title = title_elem.get_text(strip=True)

        # If no title found, try to get from first blog-post link
        if not title:
            link_elem = soup.find("a", href=re.compile(f"/az/decision/{decision_id}"))
            if link_elem:
                title = link_elem.get_text(strip=True)

        # Extract date from title - format like "18.02.26" or "04.12.25"
        date_str = ""
        date_match = re.search(r"(\d{2})\.(\d{2})\.(\d{2})", title)
        if date_match:
            day, month, year_short = date_match.groups()
            # Convert 2-digit year to 4-digit (20XX or 19XX)
            year = int(year_short)
            if year >= 90:
                year += 1900
            else:
                year += 2000
            date_str = f"{year}-{month}-{day}"

        # Extract document content from post-entry div
        content_div = soup.find("div", class_=re.compile(r"post-entry"))
        if not content_div:
            # Try alternative container
            content_div = soup.find("div", class_="blog-post")

        if not content_div:
            logger.warning(f"No content found for decision {decision_id}")
            return None

        # Extract full text - clean HTML content
        full_text = self._extract_clean_text(content_div)

        if not full_text or len(full_text) < 100:
            logger.warning(f"Insufficient text for decision {decision_id}: {len(full_text) if full_text else 0} chars")
            return None

        # Try to determine decision type from title
        decision_type = self._determine_decision_type(title)

        # Check for Word document availability
        has_docx = bool(soup.find("a", href=re.compile(f"/az/decisionDocx/{decision_id}")))

        return {
            "decision_id": decision_id,
            "title": title,
            "decision_type": decision_type,
            "date": date_str,
            "full_text": full_text,
            "url": f"https://www.constcourt.gov.az/az/decision/{decision_id}",
            "docx_url": f"https://www.constcourt.gov.az/az/decisionDocx/{decision_id}" if has_docx else None,
        }

    def _extract_clean_text(self, content_div) -> str:
        """Extract clean text from the document, removing HTML artifacts."""
        if not content_div:
            return ""

        # Remove script and style elements
        for element in content_div.find_all(["script", "style", "img", "button", "input"]):
            element.decompose()

        # Get text with line breaks preserved
        text = content_div.get_text(separator="\n", strip=True)

        # Decode HTML entities
        text = unescape(text)

        # Clean up MSO/Word artifacts
        text = re.sub(r"mso-[^;:]+[;:]?", "", text)
        text = re.sub(r"font-family:[^;]+;?", "", text)
        text = re.sub(r"font-size:[^;]+;?", "", text)
        text = re.sub(r"line-height:[^;]+;?", "", text)
        text = re.sub(r"margin[^:]*:[^;]+;?", "", text)

        # Clean up excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Remove non-breaking spaces and other entities
        text = text.replace("\xa0", " ")
        text = text.replace("&nbsp;", " ")
        text = text.replace("<o:p></o:p>", "")
        text = re.sub(r"<[^>]+>", "", text)  # Remove any remaining HTML tags

        return text.strip()

    def _determine_decision_type(self, title: str) -> str:
        """Determine the type of decision from the title."""
        title_lower = title.lower()

        if "qərar" in title_lower:
            return "decision"  # Decision
        elif "qərardad" in title_lower:
            return "ruling"  # Ruling/Decree
        elif "rəy" in title_lower:
            return "opinion"  # Opinion
        elif "şərh" in title_lower:
            return "interpretation"  # Interpretation
        elif "konstitusiya" in title_lower and "yoxlanılması" in title_lower:
            return "constitutionality_review"
        else:
            return "decision"  # Default

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object."""
        if not date_str:
            return None

        try:
            # Try ISO format first (YYYY-MM-DD)
            return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

        try:
            # Try DD.MM.YY format
            return datetime.strptime(date_str, "%d.%m.%y").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

        return None

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from document content.
        """
        decision_id = raw.get("decision_id", "")

        # Get date
        date_iso = raw.get("date")

        # Build title
        title = raw.get("title", "")
        if not title:
            title = f"Constitutional Court Decision {decision_id}"

        # Get full text
        full_text = raw.get("full_text", "")

        return {
            "_id": f"AZ/ConstitutionalCourt/{decision_id}",
            "_source": "AZ/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": raw.get("url"),

            # Source-specific fields
            "decision_id": decision_id,
            "decision_type": raw.get("decision_type"),
            "docx_url": raw.get("docx_url"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# -- CLI Entry Point ---

def main():
    scraper = AzerbaijanConstitutionalCourtScraper()

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
