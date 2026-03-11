"""
World Wide Law - Turkish Court of Cassation (Yargitay) Scraper

Fetches case law from the Turkish Court of Cassation (Yargıtay).
Data source: Bedesten API (https://bedesten.adalet.gov.tr)
Method: JSON API with HTML content extraction
Coverage: Civil and criminal supreme court decisions (~6 million total)
"""

import re
import sys
import json
import html
import base64
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TR/Yargitay")


class TurkishCourtOfCassationScraper(BaseScraper):
    """
    Scraper for: Turkish Court of Cassation (Yargıtay)
    Country: TR
    URL: https://www.yargitay.gov.tr

    Data types: case_law
    Auth: none

    Uses the Bedesten API (bedesten.adalet.gov.tr) which provides:
    - Search across all Yargıtay decisions by keyword, date, chamber
    - Full decision text in HTML format (base64 encoded)
    - ~6 million total decisions available
    """

    API_BASE = "https://bedesten.adalet.gov.tr"
    SEARCH_ENDPOINT = "/emsal-karar/searchDocuments"
    DOCUMENT_ENDPOINT = "/emsal-karar/getDocumentContent"

    # Chamber mappings for filtering
    CIVIL_CHAMBERS = [f"{i}. Hukuk Dairesi" for i in range(1, 24)]  # 1-23. Hukuk Dairesi
    CRIMINAL_CHAMBERS = [f"{i}. Ceza Dairesi" for i in range(1, 24)]  # 1-23. Ceza Dairesi
    GENERAL_COUNCILS = [
        "Hukuk Genel Kurulu",
        "Ceza Genel Kurulu",
        "Büyük Genel Kurul",
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.API_BASE,
            headers={
                "Accept": "*/*",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "AdaletApplicationName": "UyapMevzuat",
                "Content-Type": "application/json; charset=utf-8",
                "Origin": "https://mevzuat.adalet.gov.tr",
                "Referer": "https://mevzuat.adalet.gov.tr/",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            },
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Yargıtay decisions from the Bedesten API.

        Uses paginated search with common legal terms to retrieve decisions.
        The API has ~6 million decisions total.
        """
        logger.info("Fetching Yargıtay decisions from Bedesten API...")

        # Use common legal terms to get broad coverage
        # Each term will fetch sorted by decision date (newest first)
        search_terms = ["dava", "hukuk", "ceza", "karar", "mahkeme", "borç", "tazminat", "iş", "sözleşme"]
        seen_ids = set()

        for term in search_terms:
            logger.info(f"Searching for decisions with term: {term}")
            for decision in self._fetch_decisions_by_term(term, max_pages=50):
                doc_id = decision.get("documentId")
                if doc_id and doc_id not in seen_ids:
                    seen_ids.add(doc_id)
                    yield decision

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions published since the given datetime.
        """
        logger.info(f"Fetching updates since {since.isoformat()}")

        # Search with date range
        start_date = since.strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        yield from self._fetch_decisions_by_date_range(start_date, end_date)

    def _fetch_decisions_by_year(
        self, year: int, max_pages: int = 100
    ) -> Generator[dict, None, None]:
        """Fetch decisions for a specific year."""
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        yield from self._fetch_decisions_by_date_range(start_date, end_date, max_pages)

    def _fetch_decisions_by_date_range(
        self, start_date: str, end_date: str, max_pages: int = 100
    ) -> Generator[dict, None, None]:
        """Fetch decisions within a date range with pagination."""
        # Note: The Bedesten API doesn't support "*" wildcard with date filters
        # Use common legal terms to get broad coverage
        search_terms = ["dava", "hukuk", "ceza", "karar", "mahkeme"]

        for term in search_terms:
            yield from self._fetch_decisions_by_term(term, max_pages=max_pages // len(search_terms))

    def _fetch_decisions_by_term(
        self, search_term: str, max_pages: int = 20
    ) -> Generator[dict, None, None]:
        """Fetch decisions matching a search term with pagination."""
        page = 1
        page_size = 10  # API limit
        total_fetched = 0
        seen_ids = set()

        while page <= max_pages:
            try:
                self.rate_limiter.wait()

                payload = {
                    "data": {
                        "pageSize": page_size,
                        "pageNumber": page,
                        "itemTypeList": ["YARGITAYKARARI"],
                        "phrase": search_term,
                        "sortFields": ["KARAR_TARIHI"],
                        "sortDirection": "desc",
                    },
                    "applicationName": "UyapMevzuat",
                    "paging": True,
                }

                resp = self.client.post(self.SEARCH_ENDPOINT, json_data=payload)
                data = resp.json()

                if not data.get("data") or not data["data"].get("emsalKararList"):
                    break

                decisions = data["data"]["emsalKararList"]
                total = data["data"].get("total", 0)

                if not decisions:
                    break

                logger.info(
                    f"Term '{search_term}' page {page}: {len(decisions)} decisions (total available: {total})"
                )

                for decision in decisions:
                    # Skip duplicates
                    doc_id = decision.get("documentId")
                    if doc_id in seen_ids:
                        continue
                    seen_ids.add(doc_id)

                    if doc_id:
                        full_doc = self._fetch_document_content(doc_id)
                        if full_doc:
                            decision["full_text"] = full_doc
                            yield decision
                            total_fetched += 1
                        else:
                            logger.warning(f"Could not fetch content for {doc_id}")

                page += 1

            except Exception as e:
                logger.error(f"Error fetching page {page} for term '{search_term}': {e}")
                break

    def _search_decisions(
        self,
        phrase: str = "*",
        page: int = 1,
        page_size: int = 10,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        chamber: Optional[str] = None,
    ) -> dict:
        """
        Search for decisions using the Bedesten API.

        Args:
            phrase: Search query (supports AND/OR/NOT, "exact phrase", +required, -exclude)
            page: Page number (1-indexed)
            page_size: Results per page (max 10)
            start_date: Start date filter (ISO 8601)
            end_date: End date filter (ISO 8601)
            chamber: Chamber filter (birimAdi)
        """
        payload = {
            "data": {
                "pageSize": min(page_size, 10),
                "pageNumber": page,
                "itemTypeList": ["YARGITAYKARARI"],
                "phrase": phrase,
                "sortFields": ["KARAR_TARIHI"],
                "sortDirection": "desc",
            },
            "applicationName": "UyapMevzuat",
            "paging": True,
        }

        if start_date:
            payload["data"]["kararTarihiStart"] = start_date
        if end_date:
            payload["data"]["kararTarihiEnd"] = end_date
        if chamber and chamber != "ALL":
            payload["data"]["birimAdi"] = chamber

        self.rate_limiter.wait()
        resp = self.client.post(self.SEARCH_ENDPOINT, json_data=payload)
        return resp.json()

    def _fetch_document_content(self, document_id: str) -> Optional[str]:
        """
        Fetch full document content from the API.
        Returns cleaned text extracted from the HTML content.
        """
        try:
            self.rate_limiter.wait()

            payload = {
                "data": {"documentId": document_id},
                "applicationName": "UyapMevzuat",
            }

            resp = self.client.post(self.DOCUMENT_ENDPOINT, json_data=payload)
            data = resp.json()

            if not data.get("data") or not data["data"].get("content"):
                return None

            content_b64 = data["data"]["content"]
            mime_type = data["data"].get("mimeType", "text/html")

            # Decode base64 content
            content_bytes = base64.b64decode(content_b64)

            if mime_type == "text/html":
                html_content = content_bytes.decode("utf-8")
                return self._clean_html(html_content)
            elif mime_type == "application/pdf":
                # PDF extraction would require additional libraries
                logger.warning(f"PDF content for {document_id} - skipping")
                return None
            else:
                return content_bytes.decode("utf-8", errors="ignore")

        except Exception as e:
            logger.error(f"Error fetching document {document_id}: {e}")
            return None

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content to plain text."""
        if not html_content:
            return ""

        # Decode HTML entities
        text = html.unescape(html_content)

        # Remove script and style elements
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)

        # Replace common block elements with newlines
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</?(?:p|div|tr|li|h[1-6])[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</?(?:td|th)[^>]*>", " ", text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # Clean up whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        return text.strip()

    def _parse_decision_date(self, date_str: str) -> Optional[str]:
        """
        Parse decision date from API format.
        API returns ISO format like "2026-01-21T21:00:00.000+00:00"
        Returns date in YYYY-MM-DD format.
        """
        if not date_str:
            return None

        try:
            # Parse ISO format
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%d")

            # Try other formats
            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue

        except Exception:
            pass

        return None

    def _classify_chamber(self, chamber_name: str) -> dict:
        """Classify chamber as civil, criminal, or general council."""
        if not chamber_name:
            return {"division_type": "unknown", "division_number": None}

        # Civil chamber (Hukuk Dairesi)
        hukuk_match = re.search(r"(\d+)\.\s*Hukuk\s*Dairesi", chamber_name, re.IGNORECASE)
        if hukuk_match:
            return {"division_type": "civil", "division_number": int(hukuk_match.group(1))}

        # Criminal chamber (Ceza Dairesi)
        ceza_match = re.search(r"(\d+)\.\s*Ceza\s*Dairesi", chamber_name, re.IGNORECASE)
        if ceza_match:
            return {"division_type": "criminal", "division_number": int(ceza_match.group(1))}

        # General councils
        if "Hukuk Genel Kurulu" in chamber_name:
            return {"division_type": "civil_general_council", "division_number": None}
        if "Ceza Genel Kurulu" in chamber_name:
            return {"division_type": "criminal_general_council", "division_number": None}
        if "Büyük Genel Kurul" in chamber_name:
            return {"division_type": "grand_general_council", "division_number": None}

        return {"division_type": "other", "division_number": None}

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw API document into the standard schema.

        CRITICAL: Includes FULL TEXT from HTML documents.
        """
        doc_id = raw.get("documentId", "")
        full_text = raw.get("full_text", "")
        chamber_name = raw.get("birimAdi", "")

        # Parse dates
        decision_date = self._parse_decision_date(raw.get("kararTarihi", ""))
        decision_date_str = raw.get("kararTarihiStr", "")

        # Build case/decision numbers
        case_number = raw.get("esasNo", "")
        decision_number = raw.get("kararNo", "")

        # Build title
        title_parts = []
        if chamber_name:
            title_parts.append(chamber_name)
        if case_number:
            title_parts.append(f"E. {case_number}")
        if decision_number:
            title_parts.append(f"K. {decision_number}")

        title = " - ".join(title_parts) if title_parts else f"Yargıtay Kararı {doc_id}"

        # Classify chamber
        chamber_info = self._classify_chamber(chamber_name)

        # Extract years from case/decision numbers
        esas_yil = raw.get("esasNoYil")
        karar_yil = raw.get("kararNoYil")

        return {
            "_id": f"TR/Yargitay/{doc_id}",
            "_source": "TR/Yargitay",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": decision_date,
            "url": f"https://mevzuat.adalet.gov.tr/ictihat/{doc_id}",

            # Source-specific fields
            "document_id": doc_id,
            "chamber": chamber_name,
            "division_type": chamber_info["division_type"],
            "division_number": chamber_info["division_number"],
            "case_number": case_number,
            "decision_number": decision_number,
            "case_year": esas_yil,
            "decision_year": karar_yil,
            "decision_date_display": decision_date_str,

            # Keep raw metadata
            "_raw_metadata": {
                "documentId": doc_id,
                "birimAdi": chamber_name,
                "esasNo": case_number,
                "kararNo": decision_number,
                "kararTarihi": raw.get("kararTarihi"),
            },
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = TurkishCourtOfCassationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|sample] [--sample] [--sample-size N]")
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
    elif command == "sample":
        # Direct sample mode
        stats = scraper.run_sample(n=sample_size)
        print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
