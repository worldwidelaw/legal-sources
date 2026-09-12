#!/usr/bin/env python3
"""
GA/JournalOfficiel -- Journal Officiel de la République Gabonaise

Fetches legislation (laws, decrees, ordinances, arrêtés) from the Official
Gazette of the Gabonese Republic at journal-officiel.ga.

Strategy:
  - Documents are at sequential URLs: /{id}-x/ (IDs ~10000–21600)
  - ~95% of IDs contain valid documents
  - Extract full text from HTML between </nav> and <footer>
  - Skip subscription-only pages (< 200 chars useful text)

Data:
  - ~10,000+ legal documents
  - Language: French
  - Coverage: 1984–present (decrees, laws, ordinances, arrêtés)
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update (recent IDs)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GA.JournalOfficiel")

BASE_URL = "https://journal-officiel.ga"

# ID range: oldest observed ~738, newest ~21600 (Jan 2025)
ID_START = 21600  # Start from newest
ID_END = 10000    # Go back to ~1984


class GabonJournalOfficielScraper(BaseScraper):
    """
    Scraper for GA/JournalOfficiel — Journal Officiel de la République Gabonaise.
    Country: GA
    URL: https://journal-officiel.ga

    Data types: legislation
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr,en;q=0.5",
            },
            timeout=60,
        )

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean up whitespace."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html_mod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_page_content(self, html_content: str) -> Dict[str, Any]:
        """
        Extract structured content from a document page.
        Returns dict with: title, text, date, doc_type, doc_number, journal_number.
        """
        result = {
            "title": "",
            "text": "",
            "date": "",
            "doc_type": "",
            "doc_number": "",
            "journal_number": "",
        }

        # Extract content between </nav> and <footer (or end of document)
        body_match = re.search(r'</nav>(.*?)(?:<footer|$)', html_content, re.DOTALL)
        if not body_match:
            return result

        main_html = body_match.group(1)
        main_text = self._clean_html(main_html)

        # Skip if it's just a subscription page
        if len(main_text) < 200:
            return result
        if "Abonnez-vous" in main_text and len(main_text) < 400:
            return result

        # Find the JOURNAL OFFICIEL header to locate start of real content
        lines = [l.strip() for l in main_text.split('\n') if l.strip()]

        content_start = 0
        for i, line in enumerate(lines):
            if 'JOURNAL OFFICIEL' in line.upper() and ('N°' in line or 'N°' in line or 'DU' in line.upper()):
                content_start = i
                break

        if content_start == 0:
            # No JO header found — might be a different page type
            # Try to find content after search/nav elements
            for i, line in enumerate(lines):
                if any(kw in line.lower() for kw in ['décret', 'loi n°', 'arrêté', 'ordonnance', 'article']):
                    content_start = i
                    break

        if content_start == 0:
            return result

        # Extract journal number and date from header lines
        jo_line = lines[content_start] if content_start < len(lines) else ""
        jo_match = re.search(r'JOURNAL\s+OFFICIEL\s+N°\s*(.+)', jo_line, re.IGNORECASE)
        if jo_match:
            result["journal_number"] = jo_match.group(1).strip()

        # Date line is usually next
        date_line_idx = content_start + 1
        if date_line_idx < len(lines):
            date_line = lines[date_line_idx]
            date_match = re.search(r'DU\s+(\d{1,2})\s+(\w+)\s+(\d{4})', date_line, re.IGNORECASE)
            if date_match:
                day = date_match.group(1).zfill(2)
                month_name = date_match.group(2).lower()
                year = date_match.group(3)
                months_fr = {
                    "janvier": "01", "février": "02", "fevrier": "02",
                    "mars": "03", "avril": "04", "mai": "05", "juin": "06",
                    "juillet": "07", "août": "08", "aout": "08",
                    "septembre": "09", "octobre": "10", "novembre": "11",
                    "décembre": "12", "decembre": "12",
                }
                month = months_fr.get(month_name, "")
                if month:
                    result["date"] = f"{year}-{month}-{day}"
                # Content starts after date line
                content_start = date_line_idx + 1
            else:
                content_start += 1

        # The document title is the first substantive line after the header
        content_lines = lines[content_start:]

        # Remove trailing subscription text
        clean_content = []
        for line in content_lines:
            if "Abonnez-vous" in line or "ABONNEZ VOUS" in line:
                break
            if "Inscrivez-vous et recevez" in line:
                break
            clean_content.append(line)

        if not clean_content:
            return result

        # First line is usually the document title/number
        result["title"] = clean_content[0]

        # Full text is all content lines joined
        result["text"] = '\n'.join(clean_content)

        # Extract date from document text if not found in header
        if not result["date"]:
            text_sample = result["title"] + " " + (result["text"][:500] if result["text"] else "")
            date_match = re.search(r'du\s+(\d{1,2})/(\d{1,2})/(\d{4})', text_sample)
            if date_match:
                day = date_match.group(1).zfill(2)
                month = date_match.group(2).zfill(2)
                year = date_match.group(3)
                result["date"] = f"{year}-{month}-{day}"

        # Determine document type
        title_lower = result["title"].lower()
        if "décret" in title_lower or "decret" in title_lower:
            result["doc_type"] = "decret"
        elif "loi" in title_lower:
            result["doc_type"] = "loi"
        elif "ordonnance" in title_lower:
            result["doc_type"] = "ordonnance"
        elif "arrêté" in title_lower or "arrete" in title_lower or "arrête" in title_lower:
            result["doc_type"] = "arrete"
        elif "décision" in title_lower or "decision" in title_lower:
            result["doc_type"] = "decision"
        elif "constitution" in title_lower:
            result["doc_type"] = "constitution"
        else:
            result["doc_type"] = "autre"

        # Extract document number
        num_match = re.search(
            r'N°\s*([\d/\-]+(?:/\w+)*)',
            result["title"],
        )
        if num_match:
            result["doc_number"] = num_match.group(1).strip()

        return result

    def _fetch_document(self, doc_id: int) -> Dict[str, Any]:
        """Fetch and parse a single document by ID."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/{doc_id}-x/")
            if resp.status_code != 200:
                return None
            if len(resp.text) < 2000:
                return None

            content = self._extract_page_content(resp.text)
            if not content["text"] or len(content["text"]) < 100:
                return None

            return {
                "doc_id": str(doc_id),
                "url": f"/{doc_id}-x/",
                **content,
            }
        except Exception as e:
            logger.debug(f"Failed to fetch ID {doc_id}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents by scanning IDs from newest to oldest."""
        consecutive_misses = 0
        max_consecutive_misses = 50

        current_id = ID_START
        while current_id >= ID_END:
            doc = self._fetch_document(current_id)
            if doc:
                consecutive_misses = 0
                yield doc
            else:
                consecutive_misses += 1
                if consecutive_misses >= max_consecutive_misses:
                    logger.info(
                        f"Hit {max_consecutive_misses} consecutive misses at ID {current_id}, stopping"
                    )
                    break

            current_id -= 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date (scan recent IDs)."""
        since_str = since.strftime("%Y-%m-%d")
        consecutive_misses = 0
        max_misses = 30

        current_id = ID_START
        while current_id >= ID_END:
            doc = self._fetch_document(current_id)
            if doc:
                consecutive_misses = 0
                if doc.get("date") and doc["date"] < since_str:
                    logger.info(f"Reached document older than {since_str}, stopping")
                    break
                yield doc
            else:
                consecutive_misses += 1
                if consecutive_misses >= max_misses:
                    break

            current_id -= 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        text = raw.get("text", "")

        if not text or len(text) < 200:
            return None

        return {
            "id": f"GA/JournalOfficiel/{doc_id}",
            "_id": f"GA/JournalOfficiel/{doc_id}",
            "_source": "GA/JournalOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": raw.get("date", ""),
            "url": f"{BASE_URL}{raw.get('url', '')}",
            "doc_type": raw.get("doc_type", ""),
            "doc_number": raw.get("doc_number", ""),
            "journal_number": raw.get("journal_number", ""),
            "language": "fr",
            "authority": "République Gabonaise",
            "country": "GA",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing GA/JournalOfficiel endpoints...")

        print("\n1. Testing homepage...")
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/")
            resp.raise_for_status()
            print(f"   Status: {resp.status_code}, Length: {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing known document (ID 21491 - decree)...")
        doc = self._fetch_document(21491)
        if doc:
            print(f"   Title: {doc['title'][:80]}")
            print(f"   Date: {doc['date']}")
            print(f"   Type: {doc['doc_type']}")
            print(f"   Text length: {len(doc['text'])} chars")
            print(f"   Text sample: {doc['text'][:150]}...")
        else:
            print("   ERROR: Could not fetch document")

        print("\n3. Testing older document (ID 10389 - 1984 law)...")
        doc = self._fetch_document(10389)
        if doc:
            print(f"   Title: {doc['title'][:80]}")
            print(f"   Date: {doc['date']}")
            print(f"   Type: {doc['doc_type']}")
            print(f"   Text length: {len(doc['text'])} chars")
        else:
            print("   ERROR: Could not fetch document")

        print("\n4. Testing recent document (ID 21554 - electoral code)...")
        doc = self._fetch_document(21554)
        if doc:
            print(f"   Title: {doc['title'][:80]}")
            print(f"   Date: {doc['date']}")
            print(f"   Type: {doc['doc_type']}")
            print(f"   Text length: {len(doc['text'])} chars")
        else:
            print("   ERROR: Could not fetch document")

        print("\nTest complete!")


def main():
    scraper = GabonJournalOfficielScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
