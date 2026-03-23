"""
Legal Data Hunter — Bulgarian State Gazette Scraper

Fetches legislative materials from the Bulgarian State Gazette (Държавен вестник)
published by the Bulgarian National Assembly.

Data source: https://dv.parliament.bg
Method: RSS feed + HTML scraping
Coverage: 2003 onwards
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings since the site has certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BG/StateGazette")


class BulgarianStateGazetteScraper(BaseScraper):
    """
    Scraper for: Bulgarian State Gazette (Държавен вестник)
    Country: BG
    URL: https://dv.parliament.bg

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Set up HTTP client - must disable SSL verification due to cert issues
        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", ""),
            headers=self._auth_headers,
        )
        # Disable SSL verification for this site due to certificate issues
        self.client.session.verify = False

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents by iterating through document IDs (idMat).

        The Bulgarian State Gazette uses sequential document IDs starting
        from ~1000 (2005) to ~241500+ (current). We iterate through these
        IDs to fetch all documents systematically.

        NOTE: Document IDs have gaps (some ranges are empty). We use a high
        threshold for consecutive failures (500) to handle these gaps.
        """
        # Load checkpoint if exists
        checkpoint_file = Path(__file__).parent / ".checkpoint_fetch_all"
        start_id = 1000  # Earliest known document ID (2005)

        if checkpoint_file.exists():
            try:
                start_id = int(checkpoint_file.read_text().strip())
                logger.info(f"Resuming from checkpoint: idMat={start_id}")
            except:
                pass

        # Get the latest document ID (approximate current max)
        # Current ID is ~241538 as of Feb 2026
        end_id = 250000  # Safety margin for future documents

        logger.info(f"Fetching documents from idMat={start_id} to {end_id}")

        consecutive_failures = 0
        max_consecutive_failures = 500  # Higher threshold due to ID gaps
        docs_found = 0

        for doc_id in range(start_id, end_id + 1):
            try:
                doc_data = self._fetch_document_by_id(doc_id)
                if doc_data:
                    consecutive_failures = 0
                    docs_found += 1
                    yield doc_data

                    # Save checkpoint every 100 documents
                    if docs_found % 100 == 0:
                        checkpoint_file.write_text(str(doc_id))
                        logger.info(f"Checkpoint saved: idMat={doc_id}, total docs={docs_found}")
                else:
                    consecutive_failures += 1
                    # Log every 100 consecutive failures for debugging
                    if consecutive_failures % 100 == 0:
                        logger.debug(f"Consecutive failures: {consecutive_failures} at idMat={doc_id}")

            except Exception as e:
                logger.warning(f"Error fetching idMat={doc_id}: {e}")
                consecutive_failures += 1

            # Stop if too many consecutive failures (we've likely reached the end)
            if consecutive_failures >= max_consecutive_failures:
                logger.info(f"Stopping after {max_consecutive_failures} consecutive failures at idMat={doc_id}")
                break

        logger.info(f"fetch_all complete: {docs_found} documents found")

        # Remove checkpoint file when complete
        if checkpoint_file.exists():
            checkpoint_file.unlink()

    def _fetch_document_by_id(self, doc_id: int):
        """
        Fetch a document directly by its idMat value.
        Returns None if document doesn't exist.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/showMaterialDV.jsp?idMat={doc_id}")

            # Check for valid response
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} for idMat={doc_id}")
                return None

            # Check if it's a valid document page (contains content markers)
            html = resp.text
            if len(html) < 500:  # Too short to be a real document
                return None

            if "titleHead" not in html and "tdHead1" not in html:
                # Not a document page
                return None

            soup = BeautifulSoup(html, "html.parser")

            # Extract title from titleHead div
            title_elem = soup.find("div", class_="titleHead")
            title = title_elem.get_text(strip=True) if title_elem else ""

            # Extract issue info (брой: XX, от дата DD.MM.YYYY г.)
            issue_number = None
            issue_date = None
            category = ""

            # Find all mark spans for metadata extraction
            mark_spans = soup.find_all("span", class_="mark")
            for span in mark_spans:
                text = span.get_text()
                # Extract issue number
                if "брой:" in text:
                    parts = text.split(",")
                    if parts:
                        issue_number = parts[0].replace("брой:", "").strip()
                # Extract date
                date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', text)
                if date_match and not issue_date:
                    issue_date = date_match.group(1)
                # Extract category
                if "Официален раздел" in text or "Неофициален раздел" in text:
                    if "/" in text:
                        category = text.split("/")[-1].strip()

            # Extract description from tdHead1 span
            desc_elem = soup.find("span", class_="tdHead1")
            description = desc_elem.get_text(strip=True) if desc_elem else ""

            # Extract full text content - try multiple approaches
            full_text = ""

            # Method 1: Look for div with width: 100% style
            content_div = soup.find("div", style=lambda x: x and "width: 100%" in x)
            if content_div:
                full_text = content_div.get_text(separator='\n', strip=True)

            # Method 2: If that fails, try to get text from main table
            if not full_text or len(full_text) < 20:
                # Find the table containing the document content
                tables = soup.find_all("table", {"cellpadding": "0", "width": "840px"})
                for table in tables:
                    text = table.get_text(separator='\n', strip=True)
                    if len(text) > len(full_text):
                        full_text = text

            # Method 3: Fall back to body text minus navigation
            if not full_text or len(full_text) < 20:
                body = soup.find("body")
                if body:
                    # Remove navigation elements
                    for nav in body.find_all(["script", "noscript", "style", "nav"]):
                        nav.decompose()
                    full_text = body.get_text(separator='\n', strip=True)

            # Clean up excessive whitespace
            if full_text:
                full_text = re.sub(r'\n\s*\n', '\n\n', full_text)
                full_text = re.sub(r' +', ' ', full_text)
                full_text = full_text.strip()

            if not full_text or len(full_text) < 50:
                # Document exists but has no meaningful content
                logger.debug(f"Document {doc_id} has insufficient content ({len(full_text) if full_text else 0} chars)")
                return None

            return {
                "doc_id": str(doc_id),
                "title": title,
                "description": description,
                "category": category,
                "issue_number": issue_number,
                "issue_date": issue_date,
                "full_text": full_text,
            }

        except Exception as e:
            logger.debug(f"Failed to fetch document {doc_id}: {e}")
            return None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.

        For updates, we start from a recent document ID and work backwards
        until we find documents older than 'since'.
        """
        logger.info(f"Fetching updates since {since}")

        # Start from a high ID (current max) and work backwards
        # until we find documents older than 'since'
        start_id = 250000
        found_old_doc = False
        consecutive_failures = 0

        for doc_id in range(start_id, 1000, -1):
            try:
                doc_data = self._fetch_document_by_id(doc_id)
                if doc_data:
                    consecutive_failures = 0
                    # Parse issue date
                    issue_date_str = doc_data.get("issue_date", "")
                    if issue_date_str:
                        try:
                            doc_date = datetime.strptime(issue_date_str, "%d.%m.%Y")
                            doc_date = doc_date.replace(tzinfo=timezone.utc)

                            if doc_date < since:
                                # Found a document older than 'since', stop
                                found_old_doc = True
                                logger.info(f"Found document from {issue_date_str}, stopping")
                                break
                            else:
                                yield doc_data
                        except:
                            # Can't parse date, include it
                            yield doc_data
                    else:
                        yield doc_data
                else:
                    consecutive_failures += 1
            except Exception as e:
                logger.warning(f"Error fetching idMat={doc_id}: {e}")
                consecutive_failures += 1

            if consecutive_failures >= 50:
                logger.info(f"Too many consecutive failures, moving to earlier IDs")
                consecutive_failures = 0

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Full text is now fetched during fetch_all/fetch_updates,
        stored in 'full_text' field.
        """
        # Get document ID
        doc_id = raw.get("doc_id", "")
        if not doc_id:
            # Generate from content hash if no ID available
            import hashlib
            content = f"{raw.get('category', '')}{raw.get('description', '')}{raw.get('issue_date', '')}"
            doc_id = hashlib.md5(content.encode()).hexdigest()[:16]

        # Parse publication date
        pub_date_str = raw.get("issue_date", "")
        pub_date_iso = None
        if pub_date_str:
            try:
                # Format: "8.2.2026"
                pub_date = datetime.strptime(pub_date_str, "%d.%m.%Y")
                pub_date_iso = pub_date.replace(tzinfo=timezone.utc).isoformat()
            except Exception as e:
                logger.warning(f"Failed to parse date '{pub_date_str}': {e}")

        # Build source URL
        source_url = f"https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat={doc_id}"

        # Use title or category for title field
        title = raw.get("title") or raw.get("category", "")

        # Full text was already fetched in fetch_all/fetch_updates
        full_text = raw.get("full_text", "")

        return {
            "_id": f"BG/StateGazette/{doc_id}",
            "_source": "BG/StateGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": pub_date_iso,
            "url": source_url,

            # Additional fields
            "doc_id": doc_id,
            "description": raw.get("description", ""),
            "category": raw.get("category", ""),
            "publication_date": pub_date_iso,

            # Issue metadata
            "issue_number": raw.get("issue_number"),
            "issue_date": raw.get("issue_date"),

            # Links
            "source_url": source_url,

            # Keep all raw fields
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = BulgarianStateGazetteScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
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
