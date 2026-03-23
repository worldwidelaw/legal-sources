"""
Legal Data Hunter - Bulgarian Constitutional Court Scraper

Fetches case law from the Bulgarian Constitutional Court (Konstitutsionyat sud).
Data source: https://www.constcourt.bg
Method: HTML scraping via year-based search and act detail pages
Coverage: 1991 onwards
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

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
logger = logging.getLogger("BG/ConstitutionalCourt")


class BulgarianConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for: Bulgarian Constitutional Court
    Country: BG
    URL: https://www.constcourt.bg

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", "https://www.constcourt.bg"),
            headers=self._auth_headers,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents by iterating through years from current back to 1991.
        """
        current_year = datetime.now().year
        start_year = 1991  # Constitutional Court established in 1991

        for year in range(current_year, start_year - 1, -1):
            logger.info(f"Fetching acts for year {year}")
            yield from self._fetch_year(year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        Fetches recent years only.
        """
        current_year = datetime.now().year
        since_year = since.year

        for year in range(current_year, since_year - 1, -1):
            logger.info(f"Fetching updates for year {year}")
            for doc in self._fetch_year(year):
                # Filter by date if available
                date_str = doc.get("date", "")
                if date_str:
                    try:
                        doc_date = datetime.strptime(date_str, "%d-%m-%Y")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date >= since:
                            yield doc
                    except Exception:
                        # Include if we can't parse the date
                        yield doc
                else:
                    yield doc

    def _fetch_year(self, year: int) -> Generator[dict, None, None]:
        """Fetch all acts for a specific year."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/?mode=search_acts&year={year}")
            soup = BeautifulSoup(resp.text, "html.parser")

            # Find all act links in format /bg/act-XXXXX
            act_links = soup.find_all("a", href=re.compile(r"/bg/act-\d+"))
            act_ids = set()

            for link in act_links:
                href = link.get("href", "")
                match = re.search(r"/bg/act-(\d+)", href)
                if match:
                    act_ids.add(match.group(1))

            logger.info(f"Found {len(act_ids)} unique acts for year {year}")

            for act_id in sorted(act_ids, reverse=True):
                try:
                    doc = self._fetch_act(act_id)
                    if doc:
                        yield doc
                except Exception as e:
                    logger.warning(f"Failed to fetch act {act_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to fetch year {year}: {e}")

    def _fetch_act(self, act_id: str) -> dict:
        """
        Fetch a single act by ID and extract all content.

        Returns raw document dict with full text.
        """
        self.rate_limiter.wait()
        resp = self.client.get(f"/bg/act-{act_id}")
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract document content from the main container
        content_div = soup.find("div", id="document-content")
        if not content_div:
            content_div = soup.find("div", class_="document-container")

        if not content_div:
            logger.warning(f"No content found for act {act_id}")
            return None

        # Extract title/type from the first centered paragraph
        title = ""
        title_elem = content_div.find("p", style=lambda x: x and "text-align: center" in x)
        if title_elem:
            title = title_elem.get_text(strip=True)

        # Extract date - look for pattern like "София, DD месец YYYY г."
        date_str = ""
        date_elem = content_div.find_all("p", style=lambda x: x and "text-align: center" in x)
        for elem in date_elem:
            text = elem.get_text(strip=True)
            # Match patterns like "09-02-2026" or "16 декември 2025 г."
            date_match = re.search(r"(\d{1,2}[-./]\d{1,2}[-./]\d{4})", text)
            if date_match:
                date_str = date_match.group(1)
                break
            # Bulgarian date format
            bg_date = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})\s*г\.?", text)
            if bg_date:
                date_str = f"{bg_date.group(1)}-{bg_date.group(2)}-{bg_date.group(3)}"
                break

        # Extract case number - pattern like "к.д. №X/YYYY" or "конституционно дело №X/YYYY"
        case_number = ""
        page_text = content_div.get_text()
        case_match = re.search(r"(?:к\.д\.|конституционно дело)\s*№?\s*(\d+/\d+)", page_text, re.IGNORECASE)
        if case_match:
            case_number = case_match.group(1)

        # Extract judges list
        judges = []
        # Look for judge names in specific patterns
        member_elem = content_div.find(string=re.compile(r"Членове:", re.IGNORECASE))
        if member_elem:
            # Find the following element with judge names
            parent = member_elem.parent
            if parent:
                next_elem = parent.find_next_sibling()
                if next_elem:
                    judge_text = next_elem.get_text(strip=True)
                    # Split by common delimiters
                    names = re.split(r"[,;\n]", judge_text)
                    judges = [n.strip() for n in names if n.strip() and len(n.strip()) > 3]

        # Also try to find "Председател:" for the chair
        chair_elem = content_div.find(string=re.compile(r"Председател:", re.IGNORECASE))
        if chair_elem:
            parent = chair_elem.parent
            if parent:
                next_elem = parent.find_next_sibling()
                if next_elem:
                    chair_name = next_elem.get_text(strip=True)
                    if chair_name and chair_name not in judges:
                        judges.insert(0, chair_name)

        # Extract full text - clean HTML content
        full_text = self._extract_clean_text(content_div)

        # Determine act type from title
        act_type = self._determine_act_type(title)

        return {
            "act_id": act_id,
            "title": title,
            "act_type": act_type,
            "date": date_str,
            "case_number": case_number,
            "judges": judges,
            "full_text": full_text,
            "url": f"https://www.constcourt.bg/bg/act-{act_id}",
            "pdf_url": f"https://www.constcourt.bg/generate_document.php?act_id={act_id}",
        }

    def _extract_clean_text(self, content_div) -> str:
        """Extract clean text from the document, removing HTML artifacts."""
        if not content_div:
            return ""

        # Remove script and style elements
        for element in content_div.find_all(["script", "style", "img"]):
            element.decompose()

        # Get text with line breaks preserved
        text = content_div.get_text(separator="\n", strip=True)

        # Clean up excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Remove any remaining HTML entities
        text = text.replace("\xa0", " ")
        text = text.replace("&nbsp;", " ")

        return text.strip()

    def _determine_act_type(self, title: str) -> str:
        """Determine the type of act from the title."""
        title_lower = title.lower()

        if "решение" in title_lower:
            return "decision"
        elif "определение по допустимост" in title_lower:
            return "admissibility_ruling"
        elif "определение" in title_lower:
            return "order"
        elif "особено мнение" in title_lower:
            return "dissenting_opinion"
        elif "тълкувателно" in title_lower:
            return "interpretative_decision"
        elif "разпореждане" in title_lower:
            return "directive"
        else:
            return "other"

    def _parse_bulgarian_date(self, date_str: str) -> str:
        """
        Parse Bulgarian date format and return ISO format.
        Input formats:
            - "09-02-2026"
            - "16 декември 2025 г."
        """
        if not date_str:
            return None

        # Already in DD-MM-YYYY format
        match = re.match(r"(\d{1,2})[-./](\d{1,2})[-./](\d{4})", date_str)
        if match:
            day, month, year = match.groups()
            try:
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Bulgarian month names
        bg_months = {
            "януари": 1, "февруари": 2, "март": 3, "април": 4,
            "май": 5, "юни": 6, "юли": 7, "август": 8,
            "септември": 9, "октомври": 10, "ноември": 11, "декември": 12
        }

        # Try Bulgarian format with spaces or hyphens: "16 декември 2025" or "05-февруари-2026"
        match = re.search(r"(\d{1,2})[\s\-]+(\w+)[\s\-]+(\d{4})", date_str)
        if match:
            day, month_name, year = match.groups()
            month = bg_months.get(month_name.lower())
            if month:
                try:
                    dt = datetime(int(year), month, int(day))
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return None

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from document content.
        """
        act_id = raw.get("act_id", "")

        # Parse date to ISO format
        date_iso = self._parse_bulgarian_date(raw.get("date", ""))

        # Build title - include act type if different from title
        title = raw.get("title", "")
        if not title:
            title = f"Constitutional Court Act {act_id}"

        # Get full text
        full_text = raw.get("full_text", "")

        return {
            "_id": f"BG/ConstitutionalCourt/{act_id}",
            "_source": "BG/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": raw.get("url"),

            # Source-specific fields
            "act_id": act_id,
            "act_type": raw.get("act_type"),
            "case_number": raw.get("case_number"),
            "judges": raw.get("judges", []),
            "pdf_url": raw.get("pdf_url"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = BulgarianConstitutionalCourtScraper()

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
