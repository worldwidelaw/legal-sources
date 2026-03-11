"""
World Wide Law - Bulgarian Supreme Court of Cassation Scraper

Fetches case law from the Bulgarian Supreme Court of Cassation (ВКС).
Data source: http://domino.vks.bg (Domino database) and https://www.vks.bg (JSP system)
Method: HTML scraping via category views and document detail pages
Coverage: 2008 onwards (full text decisions)
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
logger = logging.getLogger("BG/SupremeCourt")


class BulgarianSupremeCourtScraper(BaseScraper):
    """
    Scraper for: Bulgarian Supreme Court of Cassation (Върховен касационен съд)
    Country: BG
    URL: https://www.vks.bg, http://domino.vks.bg

    Data types: case_law
    Auth: none
    """

    # Bulgarian month names for date parsing
    BG_MONTHS = {
        "януари": 1, "февруари": 2, "март": 3, "април": 4,
        "май": 5, "юни": 6, "юли": 7, "август": 8,
        "септември": 9, "октомври": 10, "ноември": 11, "декември": 12
    }

    # Category IDs in the Domino view (Expand=N)
    # These are the expandable sections in the case law view
    CATEGORIES = list(range(1, 19))  # Categories 1-18

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Primary client for Domino database
        self.domino_client = HttpClient(
            base_url="http://domino.vks.bg",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "bg,en-US;q=0.9,en;q=0.8",
                "Accept-Charset": "windows-1251,utf-8;q=0.7,*;q=0.3",
            },
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents by iterating through categories in the Domino database.
        """
        for category in self.CATEGORIES:
            logger.info(f"Fetching category {category}")
            yield from self._fetch_category(category)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        Fetches all categories but filters by date.
        """
        for category in self.CATEGORIES:
            logger.info(f"Fetching updates from category {category}")
            for doc in self._fetch_category(category):
                date_str = doc.get("date", "")
                if date_str:
                    try:
                        doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date >= since:
                            yield doc
                    except Exception:
                        yield doc  # Include if can't parse date
                else:
                    yield doc

    def _fetch_category(self, category: int) -> Generator[dict, None, None]:
        """
        Fetch all documents from a specific category in the Domino view.

        Now includes pagination support - Domino uses Start=N parameter.
        Each page returns up to 500 documents.
        """
        all_doc_ids = set()
        start = 1
        page_size = 500
        empty_pages = 0

        while True:
            try:
                self.rate_limiter.wait()
                url = f"/bcap/scc/webdata.nsf/bbeda36093eefe07c2257a7b002d7313?OpenView&Start={start}&Count={page_size}&Expand={category}"
                resp = self.domino_client.get(url)

                # Handle encoding - Domino uses Windows-1251
                content = resp.content.decode('windows-1251', errors='replace')
                soup = BeautifulSoup(content, "html.parser")

                # Find all document links (OpenDocument)
                doc_links = soup.find_all("a", href=re.compile(r"\?OpenDocument"))
                new_doc_ids = set()

                for link in doc_links:
                    href = link.get("href", "")
                    # Extract document ID from URL like /bcap/scc/webdata.nsf/.../docid?OpenDocument
                    match = re.search(r"/([a-f0-9]{32})\?OpenDocument", href, re.IGNORECASE)
                    if match:
                        doc_id = match.group(1)
                        if doc_id not in all_doc_ids:
                            new_doc_ids.add(doc_id)

                if start == 1:
                    logger.info(f"Category {category}: found {len(new_doc_ids)} documents on first page")

                # If no new documents, we've reached the end
                if not new_doc_ids:
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                else:
                    empty_pages = 0
                    all_doc_ids.update(new_doc_ids)

                # Move to next page
                start += page_size

                # Log progress for large categories
                if len(all_doc_ids) % 1000 == 0 and len(all_doc_ids) > 0:
                    logger.info(f"  Category {category}: {len(all_doc_ids)} documents discovered so far")

            except Exception as e:
                logger.error(f"Failed to fetch category {category} page {start}: {e}")
                break

        logger.info(f"Category {category}: total {len(all_doc_ids)} unique documents")

        # Now fetch each document
        for doc_id in sorted(all_doc_ids):
            try:
                doc = self._fetch_document(doc_id)
                if doc and doc.get("full_text"):
                    yield doc
            except Exception as e:
                logger.warning(f"Failed to fetch document {doc_id}: {e}")
                continue

    def _fetch_document(self, doc_id: str) -> dict:
        """
        Fetch a single document by ID from the Domino database.

        Returns raw document dict with full text.
        """
        self.rate_limiter.wait()
        url = f"/bcap/scc/webdata.nsf/bbeda36093eefe07c2257a7b002d7313/{doc_id}?OpenDocument"
        resp = self.domino_client.get(url)
        
        # Handle encoding
        content = resp.content.decode('windows-1251', errors='replace')
        soup = BeautifulSoup(content, "html.parser")

        # Find the main content table
        content_table = soup.find("table")
        if not content_table:
            logger.warning(f"No content table found for document {doc_id}")
            return None

        # Get all text content
        page_text = content_table.get_text(separator="\n", strip=True)
        
        if len(page_text) < 100:
            logger.warning(f"Document {doc_id} has very short content, skipping")
            return None

        # Extract title - usually the first centered element with "РЕШЕНИЕ" or "ОПРЕДЕЛЕНИЕ"
        title = ""
        title_patterns = [
            r"(РЕШЕНИЕ\s*№?\s*\d+)",
            r"(ОПРЕДЕЛЕНИЕ\s*№?\s*\d+)",
            r"(РАЗПОРЕЖДАНЕ\s*№?\s*\d+)",
        ]
        for pattern in title_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                break

        if not title:
            # Try to find the first line with decision type
            first_lines = page_text[:500].split("\n")
            for line in first_lines:
                line_clean = line.strip()
                if any(word in line_clean.upper() for word in ["РЕШЕНИЕ", "ОПРЕДЕЛЕНИЕ", "РАЗПОРЕЖДАНЕ"]):
                    title = line_clean[:100]
                    break

        # Extract date - look for Bulgarian date format
        date_str = ""
        # Pattern: "DD месец YYYY г." or "DD.MM.YYYY"
        date_patterns = [
            r"(\d{1,2})\s+(януари|февруари|март|април|май|юни|юли|август|септември|октомври|ноември|декември)\s+(\d{4})\s*г?\.?",
            r"(\d{1,2})\.(\d{1,2})\.(\d{4})",
            r"(\d{1,2})/(\d{1,2})/(\d{4})",
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    if groups[1] in self.BG_MONTHS:
                        # Bulgarian month name
                        date_str = f"{groups[0]}-{groups[1]}-{groups[2]}"
                    else:
                        # Numeric format
                        date_str = f"{groups[0]}-{groups[1]}-{groups[2]}"
                break

        # Extract case number - pattern like "гр.д.№ XXXX/YYYY" or "к.д. № X/YYYY"
        case_number = ""
        case_patterns = [
            r"(?:гр\.д\.|гр\.дело|н\.д\.|к\.д\.|търг\.д\.)\s*№?\s*(\d+/\d+)",
            r"дело\s*№?\s*(\d+/\d+)",
            r"№\s*(\d+/\d+)\s*г\.",
        ]
        for pattern in case_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                case_number = match.group(1)
                break

        # Determine chamber
        chamber = ""
        if "граждански" in page_text.lower() or "гр.д" in page_text.lower():
            chamber = "civil"
        elif "наказателен" in page_text.lower() or "н.д" in page_text.lower():
            chamber = "criminal"
        elif "търговски" in page_text.lower() or "търг.д" in page_text.lower():
            chamber = "commercial"

        # Determine act type
        act_type = self._determine_act_type(title if title else page_text[:200])

        # Extract full text - clean it up
        full_text = self._extract_clean_text(page_text)

        return {
            "doc_id": doc_id,
            "title": title,
            "act_type": act_type,
            "date": date_str,
            "case_number": case_number,
            "chamber": chamber,
            "full_text": full_text,
            "url": f"http://domino.vks.bg/bcap/scc/webdata.nsf/bbeda36093eefe07c2257a7b002d7313/{doc_id}?OpenDocument",
        }

    def _extract_clean_text(self, text: str) -> str:
        """Clean up extracted text."""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Remove special characters
        text = text.replace("\xa0", " ")
        text = text.replace("\u2003", " ")  # Em space
        text = text.replace("\u00a0", " ")  # Non-breaking space

        return text.strip()

    def _determine_act_type(self, title: str) -> str:
        """Determine the type of act from the title."""
        title_lower = title.lower()

        if "решение" in title_lower:
            return "decision"
        elif "определение" in title_lower:
            return "order"
        elif "разпореждане" in title_lower:
            return "directive"
        elif "тълкувателно" in title_lower:
            return "interpretative"
        else:
            return "other"

    def _parse_bulgarian_date(self, date_str: str) -> str:
        """
        Parse Bulgarian date format and return ISO format.
        Input formats:
            - "09-02-2026"
            - "09.02.2026"
            - "16-декември-2025"
            - "16 декември 2025"
        """
        if not date_str:
            return None

        # Numeric format DD-MM-YYYY or DD.MM.YYYY
        match = re.match(r"(\d{1,2})[-./](\d{1,2})[-./](\d{4})", date_str)
        if match:
            day, month, year = match.groups()
            try:
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Bulgarian month format
        match = re.search(r"(\d{1,2})[\s\-]+(\w+)[\s\-]+(\d{4})", date_str)
        if match:
            day, month_name, year = match.groups()
            month = self.BG_MONTHS.get(month_name.lower())
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
        doc_id = raw.get("doc_id", "")

        # Parse date to ISO format
        date_iso = self._parse_bulgarian_date(raw.get("date", ""))

        # Build title
        title = raw.get("title", "")
        if not title:
            title = f"Supreme Court Document {doc_id}"

        # Get full text
        full_text = raw.get("full_text", "")

        return {
            "_id": f"BG/SupremeCourt/{doc_id}",
            "_source": "BG/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": raw.get("url"),

            # Source-specific fields
            "doc_id": doc_id,
            "act_type": raw.get("act_type"),
            "case_number": raw.get("case_number"),
            "chamber": raw.get("chamber"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = BulgarianSupremeCourtScraper()

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
