"""
Legal Data Hunter - Cyprus Administrative Court Case Law Scraper

Fetches case law from CyLaw (cylaw.org) - Administrative Court Decisions.
Data source: http://www.cylaw.org/administrative/
Method: HTML scraping via year index pages and document detail pages
Coverage: 2016-present (5,863+ decisions)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("CY/AdminCourt")


class CyprusAdminCourtScraper(BaseScraper):
    """
    Scraper for: Cyprus Administrative Court Case Law via CyLaw
    Country: CY
    URL: http://www.cylaw.org/administrative/

    Data types: case_law
    Auth: none
    """

    BASE_URL = "http://www.cylaw.org"
    INDEX_URL = "/administrative/"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "el-GR,el;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Charset": "windows-1253,utf-8;q=0.7,*;q=0.3",
            },
        )

    def _get_available_years(self) -> list[int]:
        """Fetch the list of available years from the main index page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(self.INDEX_URL)
            content = resp.content.decode('windows-1253', errors='replace')

            years = []
            for match in re.finditer(r'index_(\d{4})\.html', content):
                years.append(int(match.group(1)))

            years = sorted(set(years), reverse=True)
            logger.info(f"Found {len(years)} years available: {years[0]} to {years[-1]}")
            return years
        except Exception as e:
            logger.error(f"Failed to fetch year list: {e}")
            return []

    def _get_case_links_for_year(self, year: int) -> list[str]:
        """Fetch all case document links for a given year."""
        try:
            self.rate_limiter.wait()
            url = f"/administrative/index_{year}.html"
            resp = self.client.get(url)
            content = resp.content.decode('windows-1253', errors='replace')

            links = []
            for match in re.finditer(r'/cgi-bin/open\.pl\?file=([^"\']+)', content):
                file_path = match.group(1)
                if file_path.endswith('.html') or file_path.endswith('.htm'):
                    links.append(file_path)

            unique_links = list(dict.fromkeys(links))
            logger.info(f"Found {len(unique_links)} cases for year {year}")
            return unique_links
        except Exception as e:
            logger.error(f"Failed to fetch cases for year {year}: {e}")
            return []

    def _fetch_document(self, file_path: str) -> Optional[dict]:
        """Fetch a single case document by file path."""
        try:
            self.rate_limiter.wait()
            url = f"/cgi-bin/open.pl?file={file_path}"
            resp = self.client.get(url)

            content = resp.content.decode('windows-1253', errors='replace')

            metadata = self._extract_metadata_from_comments(content)

            soup = BeautifulSoup(content, "html.parser")

            title_tag = soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else ""

            body = soup.find("body")
            if body:
                for script in body.find_all(["script", "style"]):
                    script.decompose()
                full_text = body.get_text(separator="\n", strip=True)
            else:
                full_text = soup.get_text(separator="\n", strip=True)

            full_text = self._clean_text(full_text)

            if len(full_text) < 200:
                logger.warning(f"Document {file_path} has very short content ({len(full_text)} chars), skipping")
                return None

            doc_id = self._extract_doc_id(file_path)

            date_str = metadata.get("date", "")
            if not date_str and title:
                date_str = self._extract_date_from_title(title)

            return {
                "doc_id": doc_id,
                "file_path": file_path,
                "title": title or metadata.get("number", f"Case {doc_id}"),
                "full_text": full_text,
                "date_raw": date_str,
                "case_number": metadata.get("number", ""),
                "court": metadata.get("court", "Administrative Court of Cyprus"),
                "plaintiff": metadata.get("plaintiff", ""),
                "defendant": metadata.get("defendant", ""),
                "jurisdiction": metadata.get("jurisdiction", ""),
                "part": metadata.get("part", ""),
                "url": f"{self.BASE_URL}{url}",
            }
        except Exception as e:
            logger.error(f"Failed to fetch document {file_path}: {e}")
            return None

    def _extract_metadata_from_comments(self, html_content: str) -> dict:
        """Extract metadata from HTML comments like <!--sino date 9/1/2024-->."""
        metadata = {}
        patterns = {
            "date": r'<!--sino date\s+([^-]+)-->',
            "number": r'<!--number\s+([^-]+)-->',
            "part": r'<!--part\s+([^-]+)-->',
            "plaintiff": r'<!--plaintiff\s+([^-]+)-->',
            "defendant": r'<!--defendant\s+([^-]+)-->',
            "court": r'<!--court\s+([^-]+)-->',
            "jurisdiction": r'<!--jurisdiction\s+([^-]+)-->',
        }
        for key, pattern in patterns.items():
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                metadata[key] = match.group(1).strip()
        return metadata

    def _extract_doc_id(self, file_path: str) -> str:
        """Extract document ID from file path."""
        match = re.search(r'/([^/]+)\.html?$', file_path)
        if match:
            return match.group(1)
        return file_path.replace("/", "_").replace(".html", "").replace(".htm", "")

    def _extract_date_from_title(self, title: str) -> str:
        """Extract date from title string."""
        match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})\s*$', title)
        if match:
            return f"{match.group(1)}/{match.group(2)}/{match.group(3)}"
        return ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format."""
        if not date_str:
            return None
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
        if match:
            try:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            try:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass
        return None

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text and remove CyLaw navigation boilerplate."""
        if not text:
            return ""

        markers = [
            "ΔΙΟΙΚΗΤΙΚΟ ΔΙΚΑΣΤΗΡΙΟ",
            "ΑΝΩΤΑΤΟ ΔΙΚΑΣΤΗΡΙΟ",
            "ΑΝΩΤΑΤΟ ΣΥΝΤΑΓΜΑΤΙΚΟ",
            "ΕΦΕΤΕΙΟ",
            "ΚΑΚΟΥΡΓΙΟΔΙΚΕΙΟ",
            "ΠΡΩΤΟΔΙΚΕΙΟ",
            "ΔΙΚΑΣΤΗΡΙΟ ΕΡΓΑΤΙΚΩΝ",
            "ΟΙΚΟΓΕΝΕΙΑΚΟ ΔΙΚΑΣΤΗΡΙΟ",
        ]

        for marker in markers:
            idx = text.find(marker)
            if idx > 0 and idx < 300:
                text = text[idx:]
                break

        footer_markers = [
            "cylaw.org:",
            "ΚΙΝΟΠ/CyLii",
            "Παγκύπριος Δικηγορικός Σύλλογος",
        ]
        for marker in footer_markers:
            idx = text.rfind(marker)
            if idx > len(text) - 300:
                text = text[:idx]
                break

        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
        text = text.replace('\xa0', ' ')
        text = text.replace('\u2003', ' ')
        text = text.replace('\u00a0', ' ')

        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents by iterating through years."""
        years = self._get_available_years()
        for year in years:
            logger.info(f"Fetching cases from year {year}")
            links = self._get_case_links_for_year(year)
            for link in links:
                doc = self._fetch_document(link)
                if doc and doc.get("full_text"):
                    yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given datetime."""
        current_year = datetime.now().year
        years_to_check = [y for y in range(current_year, since.year - 1, -1)]

        for year in years_to_check:
            logger.info(f"Checking year {year} for updates since {since.date()}")
            links = self._get_case_links_for_year(year)
            for link in links:
                doc = self._fetch_document(link)
                if doc and doc.get("full_text"):
                    date_str = doc.get("date_raw", "")
                    date_iso = self._parse_date(date_str)
                    if date_iso:
                        try:
                            doc_date = datetime.strptime(date_iso, "%Y-%m-%d")
                            doc_date = doc_date.replace(tzinfo=timezone.utc)
                            if doc_date >= since:
                                yield doc
                        except Exception:
                            yield doc
                    else:
                        yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document into the standard schema."""
        doc_id = raw.get("doc_id", "")
        date_iso = self._parse_date(raw.get("date_raw", ""))
        title = raw.get("title", "")
        if not title:
            title = f"Administrative Court Case {doc_id}"
        full_text = raw.get("full_text", "")

        return {
            "_id": f"CY/AdminCourt/{doc_id}",
            "_source": "CY/AdminCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": raw.get("url"),
            "doc_id": doc_id,
            "file_path": raw.get("file_path"),
            "case_number": raw.get("case_number"),
            "court": raw.get("court"),
            "plaintiff": raw.get("plaintiff"),
            "defendant": raw.get("defendant"),
            "jurisdiction": raw.get("jurisdiction"),
            "part": raw.get("part"),
            "_raw": raw,
        }


# CLI Entry Point

def main():
    scraper = CyprusAdminCourtScraper()

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
