#!/usr/bin/env python3
"""
INTL/IACtHR -- Inter-American Court of Human Rights Judgments

Fetches contentious case judgments from the IACtHR (Corte IDH).

Strategy:
  - POST to AJAX endpoint to get full case listing (HTML fragment)
  - Parse case citations, Series C numbers, and document URLs
  - Download PDF (preferred) or DOCX for each judgment
  - Extract full text using pdfminer or python-docx
  - Normalize records to standard schema

Data:
  - 585+ contentious case judgments (Series C)
  - Full text from PDF/DOCX documents
  - Spanish and English versions
  - Cases from 1987-2025

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IACtHR")

LISTING_URL = "https://corteidh.or.cr/get_jurisprudencia_search_tipo.cfm"
BASE_URL = "https://corteidh.or.cr"


class IACtHRScraper(BaseScraper):
    """
    Scraper for INTL/IACtHR -- Inter-American Court of Human Rights.
    Country: INTL
    URL: https://corteidh.or.cr

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://corteidh.or.cr/casos_sentencias.cfm?lang=en",
        })

    def _fetch_case_listing(self) -> str:
        """Fetch full case listing from AJAX endpoint."""
        data = {
            "lang": "en",
            "Texto_busqueda_TXT": "",
            "nId_estado_NUM": "T",
            "sYear": "1987",
            "sYear2": "2026",
            "page_rows": "3000",
            "nId_Tipo_Jurisprudencia": "CC",
            "startrow": "1",
            "search_param": "name",
        }
        r = self.session.post(LISTING_URL, data=data, timeout=60)
        r.raise_for_status()
        return r.text

    def _parse_case_listing(self, html: str) -> list:
        """Parse the HTML listing into case records with document URLs."""
        soup = BeautifulSoup(html, "html.parser")
        cases = []

        for li in soup.find_all("li", class_="search-result"):
            div = li.find("div", class_="col-12")
            if not div:
                continue

            # Extract full citation text
            raw_text = div.get_text(separator=" ", strip=True)

            # Extract Series C number from citation or from PDF URLs
            series_c = None
            pdf_urls = []
            docx_urls = []

            for a in li.find_all("a", href=True):
                href = a["href"]
                if "/seriec_" in href:
                    m = re.search(r"seriec_(\d+)", href)
                    if m:
                        series_c = int(m.group(1))
                    if href.endswith(".pdf") and "resumen" not in href and "voto" not in href and "vsc_" not in href:
                        pdf_urls.append(href)
                    elif href.endswith(".docx") and "voto" not in href and "vsc_" not in href:
                        docx_urls.append(href)
                    elif href.endswith(".doc") and not href.endswith(".docx") and "voto" not in href and "vsc_" not in href:
                        docx_urls.append(href)

            if not series_c:
                continue

            # Parse citation for case name, date, judgment type
            # Pattern: "I/A Court H.R., Case of X v. Y. TYPE. Judgment of DATE. Series C No. NNN."
            case_name = ""
            judgment_type = ""
            date_str = ""

            m = re.search(r"Case of (.+?)(?:\.\s*(?:Merits|Preliminary|Reparations|Interpretation|Monitoring|Compliance|Supervision))", raw_text)
            if m:
                case_name = m.group(1).strip().rstrip(".")
            else:
                # Try broader pattern
                m = re.search(r"Case of (.+?)(?:\.\s*Judgment|\.\s*Series)", raw_text)
                if m:
                    case_name = m.group(1).strip().rstrip(".")

            # Extract judgment type
            type_patterns = [
                r"(Merits(?:,?\s*(?:Reparations|and\s+Reparations)(?:,?\s*(?:and\s+)?Costs)?)?)",
                r"(Preliminary Objections(?:,?\s*Merits(?:,?\s*(?:Reparations|and\s+Reparations)(?:,?\s*(?:and\s+)?Costs)?)?)?)",
                r"(Reparations(?:\s+and\s+Costs)?)",
                r"(Interpretation of the Judgment.*?(?=\.\s*(?:Judgment|Series)))",
                r"(Monitoring Compliance.*?(?=\.\s*(?:Judgment|Series|$)))",
            ]
            for pat in type_patterns:
                m = re.search(pat, raw_text)
                if m:
                    judgment_type = m.group(1).strip()
                    break

            # Extract date
            m = re.search(r"Judgment of (\w+ \d{1,2},?\s*\d{4})", raw_text)
            if m:
                date_str = m.group(1)
            else:
                m = re.search(r"Sentencia de (\d{1,2} de \w+ de \d{4})", raw_text)
                if m:
                    date_str = m.group(1)

            # Determine language preference: English PDF > English DOCX > Spanish PDF > Spanish DOCX
            eng_pdfs = [u for u in pdf_urls if "_ing" in u or "_eng" in u]
            esp_pdfs = [u for u in pdf_urls if "_esp" in u]
            eng_docs = [u for u in docx_urls if "_ing" in u or "_eng" in u]
            esp_docs = [u for u in docx_urls if "_esp" in u]

            # Pick best document URL
            doc_url = None
            doc_lang = None
            if eng_pdfs:
                doc_url = eng_pdfs[0]
                doc_lang = "en"
            elif esp_pdfs:
                doc_url = esp_pdfs[0]
                doc_lang = "es"
            elif eng_docs:
                doc_url = eng_docs[0]
                doc_lang = "en"
            elif esp_docs:
                doc_url = esp_docs[0]
                doc_lang = "es"
            elif pdf_urls:
                doc_url = pdf_urls[0]
                doc_lang = "es"
            elif docx_urls:
                doc_url = docx_urls[0]
                doc_lang = "es"

            if not doc_url:
                logger.warning(f"No document URL for Series C No. {series_c}")
                continue

            # Make URL absolute
            if not doc_url.startswith("http"):
                doc_url = BASE_URL + doc_url

            cases.append({
                "series_c": series_c,
                "case_name": case_name,
                "judgment_type": judgment_type,
                "date_str": date_str,
                "doc_url": doc_url,
                "doc_lang": doc_lang,
                "citation": raw_text[:500],
            })

        return cases

    def _extract_text_from_pdf(self, content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/IACtHR",
            source_id="",
            pdf_bytes=content,
            table="case_law",
        ) or ""

    def _extract_text_from_docx(self, content: bytes) -> str:
        """Extract text from DOCX bytes."""
        try:
            import docx
            doc = docx.Document(io.BytesIO(content))
            paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
            return "\n\n".join(paragraphs)
        except Exception as e:
            logger.warning(f"DOCX extraction failed: {e}")
            return ""

    def _download_and_extract(self, url: str) -> str:
        """Download document and extract full text."""
        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return ""

        ct = r.headers.get("Content-Type", "").lower()

        # Check if we got HTML instead of a document (404 page)
        if "text/html" in ct or r.content[:6] == b"<html " or r.content[:15] == b"<!DOCTYPE html>":
            logger.warning(f"Got HTML instead of document for {url}")
            return ""

        if url.endswith(".pdf") and r.content[:5] == b"%PDF-":
            return self._extract_text_from_pdf(r.content)
        elif url.endswith((".docx", ".doc")):
            return self._extract_text_from_docx(r.content)
        elif r.content[:5] == b"%PDF-":
            return self._extract_text_from_pdf(r.content)
        else:
            logger.warning(f"Unknown format for {url}, ct={ct}")
            return ""

    def _parse_date_to_iso(self, date_str: str) -> Optional[str]:
        """Parse judgment date to ISO 8601."""
        if not date_str:
            return None

        # English format: "Month DD, YYYY" or "Month DD YYYY"
        for fmt in ["%B %d, %Y", "%B %d %Y"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Spanish format: "DD de Month de YYYY"
        es_months = {
            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
            "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
            "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
        }
        m = re.match(r"(\d{1,2}) de (\w+) de (\d{4})", date_str)
        if m:
            day, month_name, year = m.groups()
            month_num = es_months.get(month_name.lower())
            if month_num:
                return f"{year}-{month_num:02d}-{int(day):02d}"

        return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw case data into standard schema."""
        series_c = raw["series_c"]
        case_name = raw.get("case_name", "")
        judgment_type = raw.get("judgment_type", "")

        title = f"Case of {case_name}" if case_name else f"Series C No. {series_c}"
        if judgment_type:
            title += f". {judgment_type}"
        title += f". Series C No. {series_c}"

        date = self._parse_date_to_iso(raw.get("date_str", ""))

        case_url = f"https://corteidh.or.cr/docs/casos/articulos/seriec_{series_c}_{'ing' if raw.get('doc_lang') == 'en' else 'esp'}.pdf"

        return {
            "_id": f"IACtHR-C-{series_c}",
            "_source": "INTL/IACtHR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": case_url,
            "series_c_number": series_c,
            "case_name": case_name,
            "judgment_type": judgment_type,
            "language": raw.get("doc_lang", ""),
            "court": "Inter-American Court of Human Rights",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all IACtHR judgments (yields raw records)."""
        logger.info("Fetching case listing from AJAX endpoint...")
        html = self._fetch_case_listing()
        cases = self._parse_case_listing(html)
        logger.info(f"Found {len(cases)} cases in listing")

        for i, case in enumerate(cases):
            logger.info(f"[{i+1}/{len(cases)}] Downloading Series C No. {case['series_c']} from {case['doc_url']}")
            text = self._download_and_extract(case["doc_url"])

            if not text:
                logger.warning(f"No text extracted for Series C No. {case['series_c']}")
                # Try alternative formats
                series_c = case["series_c"]
                alt_urls = [
                    f"https://corteidh.or.cr/docs/casos/articulos/seriec_{series_c}_esp.pdf",
                    f"https://corteidh.or.cr/docs/casos/articulos/seriec_{series_c}_ing.pdf",
                    f"https://corteidh.or.cr/docs/casos/articulos/seriec_{series_c}_esp.docx",
                    f"https://corteidh.or.cr/docs/casos/articulos/seriec_{series_c}_ing.docx",
                ]
                for alt_url in alt_urls:
                    if alt_url == case["doc_url"]:
                        continue
                    logger.info(f"  Trying fallback: {alt_url}")
                    text = self._download_and_extract(alt_url)
                    if text:
                        case["doc_url"] = alt_url
                        if "_ing" in alt_url or "_eng" in alt_url:
                            case["doc_lang"] = "en"
                        else:
                            case["doc_lang"] = "es"
                        break
                    time.sleep(1)

            case["text"] = text
            yield case

            # Rate limit
            time.sleep(2)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch cases updated since a date."""
        since_dt = datetime.fromisoformat(since)
        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_dt = datetime.fromisoformat(record["date"])
                    if rec_dt >= since_dt:
                        yield record
                except ValueError:
                    yield record
            else:
                yield record


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/IACtHR data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IACtHRScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            html = scraper._fetch_case_listing()
            cases = scraper._parse_case_listing(html)
            logger.info(f"OK: Found {len(cases)} cases in listing")
            if cases:
                logger.info(f"First: Series C No. {cases[0]['series_c']} - {cases[0]['case_name']}")
                logger.info(f"Last: Series C No. {cases[-1]['series_c']} - {cases[-1]['case_name']}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
