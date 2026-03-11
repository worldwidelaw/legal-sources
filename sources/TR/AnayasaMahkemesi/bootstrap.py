"""
World Wide Law - Turkish Constitutional Court Scraper

Fetches case law from the Turkish Constitutional Court (Anayasa Mahkemesi).
Data sources:
  - https://normkararlarbilgibankasi.anayasa.gov.tr (Norm Review Decisions)
  - https://kararlarbilgibankasi.anayasa.gov.tr (Individual Applications)
Method: HTML scraping via pagination and detail pages
Coverage: 1961+ (norm review), 2012+ (individual applications)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
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
logger = logging.getLogger("TR/AnayasaMahkemesi")


class TurkishConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for: Turkish Constitutional Court (Anayasa Mahkemesi)
    Country: TR
    URL: https://www.anayasa.gov.tr

    Data types: case_law
    Auth: none

    Two databases:
    - Norm Review (normkararlarbilgibankasi): ~5470 decisions
    - Individual Applications (kararlarbilgibankasi): ~16464 decisions
    """

    NORM_REVIEW_BASE = "https://normkararlarbilgibankasi.anayasa.gov.tr"
    INDIVIDUAL_APP_BASE = "https://kararlarbilgibankasi.anayasa.gov.tr"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.norm_client = HttpClient(
            base_url=self.NORM_REVIEW_BASE,
            headers=self._auth_headers,
        )
        self.individual_client = HttpClient(
            base_url=self.INDIVIDUAL_APP_BASE,
            headers=self._auth_headers,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from both databases.
        Start with norm review, then individual applications.
        """
        logger.info("Fetching Norm Review decisions...")
        yield from self._fetch_norm_review_all()

        logger.info("Fetching Individual Application decisions...")
        yield from self._fetch_individual_applications_all()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        """
        logger.info(f"Fetching updates since {since.isoformat()}")

        # Fetch norm review updates (starts from most recent)
        for doc in self._fetch_norm_review_all():
            date_str = doc.get("decision_date", "")
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%d/%m/%Y")
                    doc_date = doc_date.replace(tzinfo=timezone.utc)
                    if doc_date >= since:
                        yield doc
                    else:
                        # Results are ordered newest first, so stop when we hit old ones
                        break
                except Exception:
                    yield doc

        # Same for individual applications
        for doc in self._fetch_individual_applications_all():
            date_str = doc.get("decision_date", "")
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%d/%m/%Y")
                    doc_date = doc_date.replace(tzinfo=timezone.utc)
                    if doc_date >= since:
                        yield doc
                    else:
                        break
                except Exception:
                    yield doc

    # ═══════════════════════════════════════════════════════════════
    # Norm Review Database
    # ═══════════════════════════════════════════════════════════════

    def _fetch_norm_review_all(self) -> Generator[dict, None, None]:
        """Fetch all norm review decisions via pagination."""
        page = 1
        max_pages = 600  # Safety limit (5470 / 10 = ~547 pages)

        while page <= max_pages:
            try:
                self.rate_limiter.wait()
                resp = self.norm_client.get(f"/Ara?page={page}")
                soup = BeautifulSoup(resp.text, "html.parser")

                # Find decision links with pattern /ND/YEAR/NUMBER
                links = soup.find_all("a", href=re.compile(r"/ND/\d+/\d+"))
                if not links:
                    logger.info(f"No more decisions found at page {page}")
                    break

                # Extract unique decision URLs
                seen_urls = set()
                for link in links:
                    href = link.get("href", "")
                    # Normalize URL
                    if href.startswith("/"):
                        href = f"{self.NORM_REVIEW_BASE}{href}"
                    elif not href.startswith("http"):
                        href = f"{self.NORM_REVIEW_BASE}/{href}"

                    if href not in seen_urls:
                        seen_urls.add(href)
                        try:
                            doc = self._fetch_norm_review_detail(href)
                            if doc and doc.get("full_text"):
                                yield doc
                        except Exception as e:
                            logger.warning(f"Failed to fetch {href}: {e}")

                logger.info(f"Page {page}: processed {len(seen_urls)} decisions")
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch norm review page {page}: {e}")
                break

    def _fetch_norm_review_detail(self, url: str) -> Optional[dict]:
        """Fetch a single norm review decision."""
        self.rate_limiter.wait()
        resp = self.norm_client.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract year and number from URL
        match = re.search(r"/ND/(\d+)/(\d+)", url)
        if not match:
            return None
        year, number = match.groups()
        decision_id = f"ND/{year}/{number}"

        # Extract metadata from page description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        meta_content = meta_desc.get("content", "") if meta_desc else ""

        # Extract decision text from KararMetni or kararHtml div
        full_text = ""

        # Try KararMetni div first
        karar_div = soup.find("div", class_="KararMetni")
        if karar_div:
            full_text = self._extract_clean_text(karar_div)

        # Also try kararHtml span
        if not full_text or len(full_text) < 100:
            karar_html = soup.find("span", class_="kararHtml")
            if karar_html:
                full_text = self._extract_clean_text(karar_html)

        if not full_text or len(full_text) < 100:
            logger.warning(f"No or minimal content found for {decision_id}")
            return None

        # Extract case number (Esas Sayısı) and decision number (Karar Sayısı)
        case_number = ""
        decision_number = ""
        decision_date = ""
        gazette_date = ""
        gazette_number = ""

        # Parse from text
        esas_match = re.search(r"Esas\s+Sayısı[:\s]*(\d+/\d+)", full_text, re.IGNORECASE)
        if esas_match:
            case_number = esas_match.group(1)

        karar_match = re.search(r"Karar\s+Sayısı[:\s]*(\d+/\d+)", full_text, re.IGNORECASE)
        if karar_match:
            decision_number = karar_match.group(1)

        date_match = re.search(r"Karar\s+Tarihi[:\s]*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE)
        if date_match:
            decision_date = date_match.group(1)

        rg_match = re.search(r"R\.G\.\s+Tarih\s*[-–]\s*Sayı[:\s]*(\d{1,2}/\d{1,2}/\d{4})\s*[-–]\s*(\d+)", full_text, re.IGNORECASE)
        if rg_match:
            gazette_date = rg_match.group(1)
            gazette_number = rg_match.group(2)

        # Extract title - usually first centered paragraph or "ANAYASA MAHKEMESİ KARARI"
        title = "Anayasa Mahkemesi Kararı"
        if case_number:
            title = f"E.{case_number}"
            if decision_number:
                title += f", K.{decision_number}"

        return {
            "decision_id": decision_id,
            "database": "norm_review",
            "title": title,
            "case_number": case_number,
            "decision_number": decision_number,
            "decision_date": decision_date,
            "official_gazette_date": gazette_date,
            "official_gazette_number": gazette_number,
            "full_text": full_text,
            "url": url,
            "pdf_url": f"{self.NORM_REVIEW_BASE}/Dosyalar/Kararlar/KararPDF/K.{year}-{number}.nrm.pdf",
        }

    # ═══════════════════════════════════════════════════════════════
    # Individual Applications Database
    # ═══════════════════════════════════════════════════════════════

    def _fetch_individual_applications_all(self) -> Generator[dict, None, None]:
        """Fetch all individual application decisions via pagination."""
        page = 1
        max_pages = 1700  # Safety limit (16464 / 10 = ~1647 pages)

        while page <= max_pages:
            try:
                self.rate_limiter.wait()
                resp = self.individual_client.get(f"/Ara?page={page}")
                soup = BeautifulSoup(resp.text, "html.parser")

                # Find decision links with pattern /BB/YEAR/NUMBER
                links = soup.find_all("a", href=re.compile(r"/BB/\d+/\d+"))
                if not links:
                    logger.info(f"No more decisions found at page {page}")
                    break

                # Extract unique decision URLs
                seen_urls = set()
                for link in links:
                    href = link.get("href", "")
                    # Normalize URL
                    if href.startswith("/"):
                        href = f"{self.INDIVIDUAL_APP_BASE}{href}"
                    elif not href.startswith("http"):
                        href = f"{self.INDIVIDUAL_APP_BASE}/{href}"

                    if href not in seen_urls:
                        seen_urls.add(href)
                        try:
                            doc = self._fetch_individual_application_detail(href)
                            if doc and doc.get("full_text"):
                                yield doc
                        except Exception as e:
                            logger.warning(f"Failed to fetch {href}: {e}")

                logger.info(f"Page {page}: processed {len(seen_urls)} decisions")
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch individual applications page {page}: {e}")
                break

    def _fetch_individual_application_detail(self, url: str) -> Optional[dict]:
        """Fetch a single individual application decision."""
        self.rate_limiter.wait()
        resp = self.individual_client.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract year and application number from URL
        match = re.search(r"/BB/(\d+)/(\d+)", url)
        if not match:
            return None
        year, app_number = match.groups()
        decision_id = f"BB/{year}/{app_number}"

        # Extract metadata from page description
        # Format: "(Name [Panel], B. No: YEAR/NUMBER, DATE, § …)"
        meta_desc = soup.find("meta", attrs={"name": "description"})
        meta_content = meta_desc.get("content", "") if meta_desc else ""

        applicant = ""
        panel = ""
        decision_date = ""

        if meta_content:
            # Parse applicant and panel
            applicant_match = re.search(r"\(([^[]+)\s*\[([^\]]+)\]", meta_content)
            if applicant_match:
                applicant = applicant_match.group(1).strip()
                panel = applicant_match.group(2).strip()

            # Parse date
            date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", meta_content)
            if date_match:
                decision_date = date_match.group(1)

        # Extract decision text from kararHtml span
        full_text = ""
        karar_html = soup.find("span", class_="kararHtml")
        if karar_html:
            full_text = self._extract_clean_text(karar_html)

        # Also try KararMetni div
        if not full_text or len(full_text) < 100:
            karar_div = soup.find("div", class_="KararMetni")
            if karar_div:
                full_text = self._extract_clean_text(karar_div)

        if not full_text or len(full_text) < 100:
            logger.warning(f"No or minimal content found for {decision_id}")
            return None

        # Extract application number from text if not in meta
        if not decision_date:
            date_match = re.search(r"Karar\s+Tarihi[:\s]*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE)
            if date_match:
                decision_date = date_match.group(1)

        # Build title
        title = applicant if applicant else f"Bireysel Başvuru {year}/{app_number}"

        return {
            "decision_id": decision_id,
            "database": "individual_applications",
            "title": title,
            "applicant": applicant,
            "panel": panel,
            "application_number": f"{year}/{app_number}",
            "decision_date": decision_date,
            "full_text": full_text,
            "url": url,
        }

    # ═══════════════════════════════════════════════════════════════
    # Utility Methods
    # ═══════════════════════════════════════════════════════════════

    def _extract_clean_text(self, element) -> str:
        """Extract clean text from HTML element, removing tags and formatting."""
        if not element:
            return ""

        # Remove script, style, and img elements
        for tag in element.find_all(["script", "style", "img", "meta"]):
            tag.decompose()

        # Get text with line breaks preserved
        text = element.get_text(separator="\n", strip=True)

        # Clean up excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Remove HTML entities
        text = unescape(text)
        text = text.replace("\xa0", " ")

        return text.strip()

    def _parse_turkish_date(self, date_str: str) -> Optional[str]:
        """
        Parse Turkish date format and return ISO format.
        Input formats: "09/02/2026", "09.02.2026", "9/2/2026"
        """
        if not date_str:
            return None

        # Try DD/MM/YYYY or DD.MM.YYYY format
        match = re.match(r"(\d{1,2})[/.](\d{1,2})[/.](\d{4})", date_str)
        if match:
            day, month, year = match.groups()
            try:
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return None

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from document content.
        """
        decision_id = raw.get("decision_id", "")
        database = raw.get("database", "unknown")

        # Parse date to ISO format
        date_iso = self._parse_turkish_date(raw.get("decision_date", ""))

        # Get title
        title = raw.get("title", "")
        if not title:
            title = f"Constitutional Court Decision {decision_id}"

        # Get full text
        full_text = raw.get("full_text", "")

        # Determine decision type from text
        decision_type = "unknown"
        text_lower = full_text.lower() if full_text else ""
        if "kabul edilebilirlik" in text_lower or "inadmissibility" in text_lower:
            decision_type = "inadmissibility"
        elif "ihlal" in text_lower and ("karar verilmiştir" in text_lower or "hükmedilmiştir" in text_lower):
            decision_type = "violation"
        elif "iptal" in text_lower:
            decision_type = "annulment"
        elif "ret" in text_lower or "reddi" in text_lower:
            decision_type = "rejection"
        elif "karar" in text_lower:
            decision_type = "decision"

        return {
            "_id": f"TR/AnayasaMahkemesi/{decision_id}",
            "_source": "TR/AnayasaMahkemesi",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": raw.get("url"),

            # Source-specific fields
            "decision_id": decision_id,
            "database": database,
            "case_number": raw.get("case_number"),
            "decision_number": raw.get("decision_number"),
            "decision_type": decision_type,
            "applicant": raw.get("applicant"),
            "panel": raw.get("panel"),
            "official_gazette_date": raw.get("official_gazette_date"),
            "official_gazette_number": raw.get("official_gazette_number"),
            "pdf_url": raw.get("pdf_url"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = TurkishConstitutionalCourtScraper()

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
