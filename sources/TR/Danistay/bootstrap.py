"""
Legal Data Hunter - Turkish Council of State (Danistay) Scraper

Fetches case law from the Turkish Council of State (Danıştay).
Data sources:
  - https://api.danistay.gov.tr/api/v1/tr/guncelKararlar (current decisions API)
  - PDFs at https://danistay.gov.tr/assets/pdf/guncelKararlar/{dokuman}
Method: JSON API + PDF text extraction
Coverage: Recent precedent-setting decisions from administrative chambers
"""

import re
import sys
import json
import logging
import tempfile
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
logger = logging.getLogger("TR/Danistay")


class TurkishCouncilOfStateScraper(BaseScraper):
    """
    Scraper for: Turkish Council of State (Danıştay)
    Country: TR
    URL: https://www.danistay.gov.tr

    Data types: case_law
    Auth: none

    This scraper fetches decisions from the public API which provides
    recent precedent-setting decisions (İçtihat Kararları) with:
    - Decision summaries (ozet)
    - PDF documents containing full decision text
    - Decision dates
    """

    API_BASE = "https://api.danistay.gov.tr/api/v1"
    PDF_BASE = "https://danistay.gov.tr/assets/pdf/guncelKararlar"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.api_client = HttpClient(
            base_url=self.API_BASE,
            headers={
                **self._auth_headers,
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )
        self.pdf_client = HttpClient(
            base_url=self.PDF_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions from the public API.
        Includes current decisions (guncelKararlar) and decision bulletins.
        """
        logger.info("Fetching current decisions (guncelKararlar)...")
        yield from self._fetch_current_decisions()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions published since the given datetime.
        """
        logger.info(f"Fetching updates since {since.isoformat()}")

        for doc in self._fetch_current_decisions():
            date_str = doc.get("tarih", "")
            if date_str:
                try:
                    doc_date = self._parse_api_date(date_str)
                    if doc_date and doc_date >= since:
                        yield doc
                    elif doc_date and doc_date < since:
                        # API returns newest first, so stop when we hit old ones
                        break
                except Exception:
                    yield doc

    def _fetch_current_decisions(self) -> Generator[dict, None, None]:
        """Fetch decisions from the guncelKararlar API endpoint."""
        try:
            self.rate_limiter.wait()
            resp = self.api_client.get("/tr/guncelKararlar")
            decisions = resp.json()

            if not isinstance(decisions, list):
                logger.warning("Unexpected API response format")
                return

            logger.info(f"Found {len(decisions)} decisions in API")

            for decision in decisions:
                try:
                    # Extract PDF full text
                    pdf_filename = decision.get("dokuman", "")
                    full_text = ""

                    if pdf_filename:
                        full_text = self._extract_pdf_text(pdf_filename)

                    if not full_text or len(full_text) < 100:
                        # Fall back to summary if PDF extraction fails
                        full_text = decision.get("ozet", "")
                        logger.warning(
                            f"Could not extract PDF text for {pdf_filename}, using summary"
                        )

                    decision["full_text"] = full_text
                    decision["pdf_url"] = f"{self.PDF_BASE}/{pdf_filename}" if pdf_filename else None
                    yield decision

                except Exception as e:
                    logger.warning(f"Failed to process decision {decision.get('id')}: {e}")

        except Exception as e:
            logger.error(f"Failed to fetch current decisions: {e}")

    def _extract_pdf_text(self, pdf_filename: str) -> str:
        """
        Download PDF and extract text content.
        Uses pdfplumber for reliable text extraction.
        """
        try:
            self.rate_limiter.wait()
            resp = self.pdf_client.get(f"/{pdf_filename}", stream=True)

            if resp.status_code != 200:
                logger.warning(f"PDF download failed: {resp.status_code}")
                return ""

            # Save to temp file for pdfplumber
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp_path = tmp.name

            try:
                import pdfplumber

                text_parts = []
                with pdfplumber.open(tmp_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)

                full_text = "\n\n".join(text_parts)
                return self._clean_text(full_text)

            except ImportError:
                # Fallback to PyPDF2 if pdfplumber not available
                try:
                    import PyPDF2

                    text_parts = []
                    with open(tmp_path, "rb") as f:
                        reader = PyPDF2.PdfReader(f)
                        for page in reader.pages:
                            page_text = page.extract_text()
                            if page_text:
                                text_parts.append(page_text)

                    full_text = "\n\n".join(text_parts)
                    return self._clean_text(full_text)

                except ImportError:
                    logger.error("No PDF library available (install pdfplumber or PyPDF2)")
                    return ""

            finally:
                # Clean up temp file
                Path(tmp_path).unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"PDF extraction error: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Remove common PDF artifacts
        text = text.replace("\x00", "")
        text = re.sub(r"[\uf0b7\uf0a7]", "•", text)  # Replace common bullet chars

        return text.strip()

    def _parse_api_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse date from API format: "09 02 2026" (day month year)
        Returns datetime in UTC.
        """
        if not date_str:
            return None

        parts = date_str.strip().split()
        if len(parts) >= 3:
            try:
                day = int(parts[0])
                month = int(parts[1])
                year = int(parts[2])
                return datetime(year, month, day, tzinfo=timezone.utc)
            except (ValueError, IndexError):
                pass

        return None

    def _parse_date_to_iso(self, date_str: str) -> Optional[str]:
        """Convert API date format to ISO 8601."""
        dt = self._parse_api_date(date_str)
        if dt:
            return dt.strftime("%Y-%m-%d")
        return None

    def _extract_decision_info(self, text: str) -> dict:
        """Extract structured info from decision text."""
        info = {}

        # Extract case number (Esas Sayısı)
        esas_match = re.search(
            r"(?:Esas\s*(?:Sayısı|No)[:\s]*|E[:\s]*)(20\d{2}/\d+)",
            text,
            re.IGNORECASE,
        )
        if esas_match:
            info["case_number"] = esas_match.group(1)

        # Extract decision number (Karar Sayısı)
        karar_match = re.search(
            r"(?:Karar\s*(?:Sayısı|No)[:\s]*|K[:\s]*)(20\d{2}/\d+)",
            text,
            re.IGNORECASE,
        )
        if karar_match:
            info["decision_number"] = karar_match.group(1)

        # Extract decision date
        date_match = re.search(
            r"(?:Karar\s*Tarihi|Tarih)[:\s]*(\d{1,2}[/.]\d{1,2}[/.]\d{4})",
            text,
            re.IGNORECASE,
        )
        if date_match:
            info["decision_date_text"] = date_match.group(1)

        # Identify chamber/daire
        daire_match = re.search(
            r"(?:Danıştay\s+)?(\d+)\.\s*Daire",
            text,
            re.IGNORECASE,
        )
        if daire_match:
            info["chamber"] = f"{daire_match.group(1)}. Daire"

        # Check for İDDK (Administrative Chambers Board)
        if "İdari Dava Daireleri" in text or "İDDK" in text:
            info["chamber"] = "İdari Dava Daireleri Kurulu"

        # Check for VDDK (Tax Chambers Board)
        if "Vergi Dava Daireleri" in text or "VDDK" in text:
            info["chamber"] = "Vergi Dava Daireleri Kurulu"

        return info

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw API document into the standard schema.

        CRITICAL: Includes FULL TEXT from PDF documents.
        """
        decision_id = str(raw.get("id", ""))
        full_text = raw.get("full_text", "")
        summary = raw.get("ozet", "")
        date_str = raw.get("tarih", "")

        # Extract additional info from full text
        extracted = self._extract_decision_info(full_text) if full_text else {}

        # Build title from summary or extracted info
        title = summary[:200] if summary else f"Danıştay Kararı {decision_id}"
        if len(summary) > 200:
            title = title.rsplit(" ", 1)[0] + "..."

        # Determine decision type
        decision_type = "precedent"  # These are İçtihat (precedent) decisions
        text_lower = (full_text or summary).lower()
        if "aykırılığın giderilmesi" in text_lower:
            decision_type = "unification"  # İçtihadı Birleştirme
        elif "iptal" in text_lower:
            decision_type = "annulment"

        return {
            "_id": f"TR/Danistay/{decision_id}",
            "_source": "TR/Danistay",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text if full_text else summary,  # MANDATORY FULL TEXT
            "date": self._parse_date_to_iso(date_str),
            "url": raw.get("pdf_url") or f"https://www.danistay.gov.tr/guncel-karar-arsiv",

            # Source-specific fields
            "decision_id": decision_id,
            "summary": summary,
            "case_number": extracted.get("case_number"),
            "decision_number": extracted.get("decision_number"),
            "chamber": extracted.get("chamber"),
            "decision_type": decision_type,
            "pdf_filename": raw.get("dokuman"),
            "pdf_url": raw.get("pdf_url"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = TurkishCouncilOfStateScraper()

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
