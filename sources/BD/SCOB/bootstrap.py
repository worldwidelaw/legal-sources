#!/usr/bin/env python3
"""
BD/SCOB -- Supreme Court Online Bulletin (Bangladesh)

Fetches curated judgments from the SCOB law report published by the
Supreme Court of Bangladesh.

Strategy:
  - Iterate 20 volumes (2015-2025) × 2 divisions (Appellate, High Court)
  - Parse HTML listing pages for metadata (case name, judge, citation, keywords, summary)
  - Download each judgment's PDF from /resources/bulletin/
  - Extract full text using PyMuPDF (fitz)

Endpoints:
  - Listing (vol 1-14): https://www.supremecourt.gov.bd/web/?page=bulletin/bulletin_list.php&menu=00&div_id={1|2}&issue={vol}
  - Listing (vol 15-20 AD): https://www.supremecourt.gov.bd/web/?page=bulletin/bulletin_list_AD_{vol}.php&menu=00&div_id=1&issue={vol}
  - Listing (vol 15-20 HD): https://www.supremecourt.gov.bd/web/?page=bulletin/bulletin_list_HD_{vol}.php&menu=00&div_id=2&issue={vol}
  - PDF: https://www.supremecourt.gov.bd/resources/bulletin/{path_from_href}

Data:
  - ~456 curated judgments across 20 volumes
  - Full text in English (primary) and Bengali
  - License: Public court records

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

import requests
try:
    import fitz  # PyMuPDF — optional; falls back to shared extractor if absent (issue #816)
except ImportError:
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BD.SCOB")

BASE_URL = "https://www.supremecourt.gov.bd"
BULLETIN_PAGE = BASE_URL + "/web/?page=bulletin.php&menu=10"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,bn;q=0.8",
    "Referer": BULLETIN_PAGE,
}

DIVISIONS = {1: "Appellate Division", 2: "High Court Division"}
VOLUME_YEARS = {i: 2014 + i for i in range(1, 21)}  # vol 1 = 2015 ... vol 20 = 2025
MAX_VOLUME = 20


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF.

    Prefers PyMuPDF (fitz) when installed; otherwise falls back to the shared
    pdfplumber/pypdf extractor in common.pdf_extract, so the scraper still runs
    on hosts without PyMuPDF (issue #816).
    """
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            parts = []
            for page in doc:
                text = page.get_text()
                if text:
                    parts.append(text.strip())
            doc.close()
            joined = "\n\n".join(parts)
            if joined.strip():
                return joined
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
    # Fallback: shared extractor (pdfplumber/pypdf — always in requirements.txt)
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source="BD/SCOB", source_id="_pdf", pdf_bytes=pdf_bytes, force=True
        )
        return text or ""
    except Exception as e:
        logger.warning(f"pdf_extract fallback failed: {e}")
        return ""


def _build_listing_url(volume: int, div_id: int) -> str:
    """Build the listing URL for a given volume and division."""
    if volume >= 15:
        tag = "AD" if div_id == 1 else "HD"
        page = f"bulletin/bulletin_list_{tag}_{volume}.php"
    else:
        page = "bulletin/bulletin_list.php"
    return f"{BASE_URL}/web/?page={page}&menu=00&div_id={div_id}&issue={volume}"


def _clean_html(c: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    c = re.sub(r"<[^>]+>", " ", c)
    c = html_mod.unescape(c)
    c = re.sub(r"\s+", " ", c).strip()
    return c


def _extract_pdf_url_from_row(row: str) -> Optional[str]:
    """Extract PDF URL from a table row's HTML."""
    pdf_links = re.findall(
        r'href=["\'](\.\./resources/bulletin/[^"\']+\.pdf)["\']', row, re.IGNORECASE
    )
    if not pdf_links:
        pdf_links = re.findall(
            r'href=["\'](https?://[^"\']*resources/bulletin/[^"\']+\.pdf)["\']', row, re.IGNORECASE
        )
    if not pdf_links:
        return None
    raw_link = pdf_links[0]
    if raw_link.startswith("../"):
        return f"{BASE_URL}/{raw_link.replace('../', '')}"
    elif raw_link.startswith("http"):
        return raw_link
    return f"{BASE_URL}/{raw_link}"


def _extract_a_tag_text(html_cell: str) -> str:
    """Extract cleaned text from <a> tags in an HTML cell."""
    a_match = re.findall(r"<a[^>]*>(.*?)</a>", html_cell, re.DOTALL)
    if a_match:
        return _clean_html(a_match[0])
    return ""


def _extract_metadata(text: str) -> Dict[str, str]:
    """Extract judge, citation, and keywords from a text block."""
    meta: Dict[str, str] = {"judge": "", "citation": "", "keywords": ""}

    # Judge: in parentheses, typically containing "J" or "CJ" or "Justice"
    judge_match = re.search(r"\(([^)]*(?:J\b|CJ\b|Justice)[^)]*)\)", text, re.IGNORECASE)
    if judge_match:
        meta["judge"] = judge_match.group(1).strip()

    # Citation: "X SCOB [YYYY] AD/HCD N"
    cite_match = re.search(r"(\d+\s+SCOB\s*\[\d{4}\]\s*(?:AD|HCD?)\s*\d*)", text)
    if cite_match:
        meta["citation"] = cite_match.group(1).strip()

    # Keywords
    kw_match = re.search(r"Key\s*[Ww]ords?\s*:?\s*(.*?)$", text, re.IGNORECASE)
    if kw_match:
        meta["keywords"] = kw_match.group(1).strip()

    return meta


def _parse_listing(html_content: str, volume: int, div_id: int) -> List[Dict[str, Any]]:
    """Parse an SCOB listing page and extract case metadata + PDF URLs.

    Handles two formats:
    - Old (volumes 1-14): 6 cells — serial, volume, year, case+link, keywords, summary
    - New (volumes 15-20): 3-4 cells — serial, case+judge+cite+keywords+link, summary, ratio
    """
    results = []
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html_content, re.DOTALL)

    for row in rows:
        if "resources/bulletin" not in row:
            continue

        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 2:
            continue

        pdf_url = _extract_pdf_url_from_row(row)
        if not pdf_url:
            continue

        serial = _clean_html(cells[0])
        serial_num = re.sub(r"[^0-9]", "", serial) or "0"
        div_tag = "AD" if div_id == 1 else "HCD"

        # Detect format by cell count
        if len(cells) >= 6:
            # Old format (volumes 1-14): serial, vol, year, case+link, keywords, summary
            case_cell_raw = cells[3]
            case_name = _extract_a_tag_text(case_cell_raw) or _clean_html(case_cell_raw)
            case_text = _clean_html(case_cell_raw)
            meta = _extract_metadata(case_text)
            keywords = _clean_html(cells[4])
            if meta["keywords"]:
                keywords = meta["keywords"] + "; " + keywords if keywords else meta["keywords"]
            summary = _clean_html(cells[5])
            ratio = ""
        else:
            # New format (volumes 15-20): serial, case+everything, summary, [ratio]
            case_cell_raw = cells[1]
            case_name = _extract_a_tag_text(case_cell_raw)
            case_text = _clean_html(case_cell_raw)
            meta = _extract_metadata(case_text)
            keywords = meta["keywords"]
            summary = _clean_html(cells[2]) if len(cells) > 2 else ""
            ratio = _clean_html(cells[3]) if len(cells) > 3 else ""

            # If case_name is empty, fall back to full cell text
            if not case_name:
                case_name = case_text

        # Clean up case_name: remove citation, judge, keywords from it
        for pattern in [r"\d+\s+SCOB\s*\[\d{4}\]\s*(?:AD|HCD?)\s*\d*", r"Key\s*[Ww]ords?\s*:.*$"]:
            case_name = re.sub(pattern, "", case_name).strip()
        # Remove trailing judge info if still present
        paren_idx = case_name.rfind("(")
        if paren_idx > 10 and re.search(r"\b(?:J\b|CJ\b|Justice)", case_name[paren_idx:], re.IGNORECASE):
            case_name = case_name[:paren_idx].strip()

        case_id = f"SCOB-{volume}-{div_tag}-{serial_num}"
        year = VOLUME_YEARS.get(volume)

        results.append({
            "case_id": case_id,
            "case_name": case_name,
            "judge": meta["judge"],
            "citation": meta["citation"],
            "keywords": keywords,
            "summary": summary,
            "ratio": ratio,
            "volume": volume,
            "year": year,
            "div_id": div_id,
            "division": DIVISIONS[div_id],
            "serial": serial_num,
            "pdf_url": pdf_url,
        })

    return results


class BDSCOBScraper(BaseScraper):
    """
    Scraper for BD/SCOB -- Supreme Court Online Bulletin.
    Country: BD
    URL: https://www.supremecourt.gov.bd
    Data types: case_law
    Auth: none (public court records)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, url: str) -> str:
        """Fetch an HTML page with rate limiting."""
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _fetch_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
                logger.warning(f"Not a PDF response from {pdf_url}: {content_type}")
                return None

            text = extract_pdf_text(resp.content)
            if not text or len(text.strip()) < 100:
                logger.warning(f"Insufficient text from {pdf_url}: {len(text) if text else 0} chars")
                return None

            return text.strip()

        except requests.exceptions.RequestException as e:
            logger.error(f"PDF download failed for {pdf_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all SCOB judgments across all volumes and divisions."""
        for volume in range(1, MAX_VOLUME + 1):
            for div_id in [1, 2]:
                url = _build_listing_url(volume, div_id)
                div_name = "AD" if div_id == 1 else "HCD"
                logger.info(f"Fetching Vol {volume} {div_name}: {url}")

                try:
                    html_content = self._fetch_page(url)
                except Exception as e:
                    logger.error(f"Failed to fetch listing for Vol {volume} {div_name}: {e}")
                    continue

                cases = _parse_listing(html_content, volume, div_id)
                logger.info(f"  Found {len(cases)} cases in Vol {volume} {div_name}")

                for case in cases:
                    pdf_url = case["pdf_url"]
                    logger.info(f"  Downloading PDF: {case['case_id']}")

                    text = self._fetch_pdf_text(pdf_url)
                    if text:
                        case["text"] = text
                        yield case
                    else:
                        logger.warning(f"  Skipping {case['case_id']} — no text extracted")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently added volumes only."""
        # SCOB publishes one volume per year; check latest volume
        current_year = datetime.now().year
        latest_vol = min(current_year - 2014, MAX_VOLUME)
        for div_id in [1, 2]:
            url = _build_listing_url(latest_vol, div_id)
            try:
                html_content = self._fetch_page(url)
            except Exception:
                continue
            cases = _parse_listing(html_content, latest_vol, div_id)
            for case in cases:
                text = self._fetch_pdf_text(case["pdf_url"])
                if text:
                    case["text"] = text
                    yield case

    def normalize(self, raw: dict) -> dict:
        """Transform raw SCOB case data into standard schema."""
        title = raw.get("case_name", "").strip()
        if not title:
            title = f"SCOB Vol {raw['volume']} {raw['division']} No. {raw['serial']}"

        date_str = None
        if raw.get("year"):
            date_str = f"{raw['year']}-01-01"

        # Combine summary and ratio for the summary field
        summary_parts = []
        if raw.get("summary"):
            summary_parts.append(raw["summary"])
        if raw.get("ratio"):
            summary_parts.append(f"Key Ratio: {raw['ratio']}")
        summary = "\n\n".join(summary_parts) if summary_parts else None

        return {
            "_id": raw["case_id"],
            "_source": "BD/SCOB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": raw["case_id"],
            "title": title,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("pdf_url", ""),
            "division": raw["division"],
            "volume": raw["volume"],
            "year": raw.get("year"),
            "citation": raw.get("citation", ""),
            "keywords": raw.get("keywords", ""),
            "summary": summary,
            "judge": raw.get("judge", ""),
            "court": "Supreme Court of Bangladesh",
            "country": "BD",
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="BD/SCOB scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10+ records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = BDSCOBScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        url = _build_listing_url(20, 1)
        html = scraper._fetch_page(url)
        cases = _parse_listing(html, 20, 1)
        logger.info(f"Test OK: {len(cases)} cases found in Vol 20 AD")
        if cases:
            pdf_url = cases[0]["pdf_url"]
            text = scraper._fetch_pdf_text(pdf_url)
            logger.info(f"PDF test: {len(text) if text else 0} chars from {cases[0]['case_id']}")
        sys.exit(0)

    if args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        logger.info(f"Bootstrap result: {result}")

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {result}")
