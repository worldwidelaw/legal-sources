#!/usr/bin/env python3
"""
CG/SGG-TextesOfficiels -- Congo-Brazzaville Official Legal Texts (SGG)

Fetches legislation from the Secrétariat Général du Gouvernement search page.
Parses HTML search results, downloads individual PDFs, extracts full text.

Types fetched (in priority order):
  - Loi (12):        ~2,145 laws
  - Ordonnance (13): ~713 ordinances
  - Décret (7):      ~34,775 decrees
  - Décision (5):    ~121 decisions

Strategy:
  - HTML scrape of search results at /droit-congolais/recherche.html
  - Paginate through results (20 per page)
  - Each result may have a link-pdf (individual PDF) and/or link-jo (JO PDF)
  - Download individual PDFs, extract text with pdfminer
  - Skip results without individual PDF links (JO-only entries)

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CG.SGG-TextesOfficiels")

BASE_URL = "https://sgg.cg"
SEARCH_URL = f"{BASE_URL}/droit-congolais/recherche.html"

# Document types to fetch: (id, label, data_type_category)
DOC_TYPES = [
    (12, "Loi"),
    (13, "Ordonnance"),
    (7,  "Décret"),
    (5,  "Décision"),
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (open-data research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
})


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------

def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        text = pdfminer_extract(io.BytesIO(pdf_bytes))
        return text.strip()
    except ImportError:
        pass
    # Fallback to PyPDF2
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
        return "\n\n".join(parts).strip()
    except ImportError:
        logger.error("No PDF library available (install pdfminer.six or PyPDF2)")
        return ""
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


def _clean_text(text: str) -> str:
    """Clean up extracted PDF text."""
    # Fix common ligature issues from PDF extraction
    text = text.replace("\ufb01", "fi").replace("\ufb02", "fl")
    text = text.replace("Þ ", "fi").replace("Þ", "fi")
    # Normalize whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# HTML parsing helpers
# ---------------------------------------------------------------------------

def _parse_search_results(html: str) -> List[Dict[str, Any]]:
    """Parse search result items from HTML."""
    results = []
    # Each result is in <li> within <div class="card-content">
    # Pattern: <li>...<p class="name">TITLE</p>...</li>
    li_pattern = re.compile(
        r'<li>\s*<div class="link-dc">(.*?)</div>\s*<div class="links">(.*?)</div>\s*</li>',
        re.DOTALL
    )
    name_pattern = re.compile(r'<p class="name">\s*(.*?)\s*</p>', re.DOTALL)
    pdf_pattern = re.compile(r"class='link-pdf'\s+href='([^']+)'")
    jo_pattern = re.compile(r"class='link-jo'\s+href='([^']+)'")

    for match in li_pattern.finditer(html):
        link_dc = match.group(1)
        links_div = match.group(2)

        name_match = name_pattern.search(link_dc)
        if not name_match:
            continue

        title = unescape(name_match.group(1).strip())
        # Collapse whitespace
        title = re.sub(r"\s+", " ", title)

        pdf_match = pdf_pattern.search(links_div)
        jo_match = jo_pattern.search(links_div)

        pdf_url = pdf_match.group(1) if pdf_match else None
        jo_url = jo_match.group(1) if jo_match else None

        results.append({
            "title": title,
            "pdf_url": pdf_url,
            "jo_url": jo_url,
        })

    return results


def _get_total_results(html: str) -> int:
    """Extract total result count from pagination."""
    match = re.search(r'row=(\d+)', html)
    if match:
        return int(match.group(1))
    return 0


def _parse_date_from_title(title: str) -> Optional[str]:
    """Extract date from title like 'Loi n°30-2025 du 22 août 2025 ...'"""
    months_fr = {
        "janvier": "01", "février": "02", "mars": "03", "avril": "04",
        "mai": "05", "juin": "06", "juillet": "07", "août": "08",
        "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    }
    # Match patterns like "du 22 août 2025"
    m = re.search(r'du\s+(\d{1,2})\s+(' + '|'.join(months_fr.keys()) + r')\s+(\d{4})', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = months_fr.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"
    # Try just year
    m = re.search(r'n[°o]\s*\d+-(\d{4})', title)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _parse_reference_from_title(title: str) -> str:
    """Extract reference number like 'Loi n°30-2025' from title."""
    m = re.search(r'((?:Loi|Décret|Ordonnance|Arrêté|Décision|Délibération|Circulaire|Avis)\s+(?:organique\s+)?n[°o]\s*\S+)', title, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Try simpler pattern
    m = re.search(r'n[°o]\s*(\S+)', title)
    if m:
        return f"n°{m.group(1)}"
    return title[:80]


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class SGGTextesOfficielsScraper(BaseScraper):

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "").strip()
        ref = _parse_reference_from_title(title)
        date_iso = _parse_date_from_title(title)

        # Build a stable ID from reference
        ref_id = re.sub(r'[^a-zA-Z0-9-]', '_', ref)

        pdf_url = raw.get("pdf_url", "")
        jo_url = raw.get("jo_url", "")
        doc_type_label = raw.get("doc_type_label", "")

        return {
            "_id": ref_id,
            "_source": "CG/SGG-TextesOfficiels",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": pdf_url or jo_url or SEARCH_URL,
            "reference_number": ref,
            "document_type": doc_type_label,
            "jo_url": jo_url,
        }

    def _fetch_documents(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents from all types."""
        max_per_type = 15 if sample else 999999
        total_yielded = 0
        max_total = 15 if sample else 999999
        seen_urls = set()

        for type_id, type_label in DOC_TYPES:
            if total_yielded >= max_total:
                break
            logger.info("=== Fetching type: %s (id=%d) ===", type_label, type_id)
            count_this_type = 0

            for doc in self._fetch_type(type_id, type_label, max_per_type):
                if total_yielded >= max_total:
                    break
                # Dedup by PDF URL
                pdf_url = doc.get("pdf_url", "")
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                doc["doc_type_label"] = type_label
                yield doc
                total_yielded += 1
                count_this_type += 1

            logger.info("  Type %s: yielded %d documents", type_label, count_this_type)

    def _fetch_type(self, type_id: int, type_label: str, limit: int) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents for a specific type, paginating through results."""
        # First request to get total count
        params = {
            "field_idType": str(type_id),
            "action": "Rechercher",
        }

        try:
            self.rate_limiter.wait()
            resp = SESSION.get(SEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Initial search failed for type %d: %s", type_id, e)
            return

        html = resp.text
        total = _get_total_results(html)
        logger.info("  Type %s: %d total results", type_label, total)

        # Parse first page
        count = 0
        for doc in self._process_page_results(html, type_label):
            if count >= limit:
                return
            yield doc
            count += 1

        if total == 0 or count >= limit:
            return

        # Calculate total pages (20 results per page, 0-indexed after first)
        items_per_page = 20
        total_pages = (total + items_per_page - 1) // items_per_page

        for page_idx in range(1, total_pages):
            if count >= limit:
                return

            page_params = {
                "field_idType": str(type_id),
                "page": str(page_idx),
                "row": str(total),
                "action": "Rechercher",
            }

            try:
                self.rate_limiter.wait()
                resp = SESSION.get(SEARCH_URL, params=page_params, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning("Page %d fetch failed: %s", page_idx, e)
                continue

            for doc in self._process_page_results(resp.text, type_label):
                if count >= limit:
                    return
                yield doc
                count += 1

            if page_idx % 10 == 0:
                logger.info("  Progress: page %d/%d, %d docs so far", page_idx, total_pages, count)

    def _process_page_results(self, html: str, type_label: str) -> Generator[Dict[str, Any], None, None]:
        """Parse page results and download PDFs with text."""
        results = _parse_search_results(html)

        for item in results:
            pdf_url = item.get("pdf_url")
            if not pdf_url:
                # No individual PDF available — skip (JO-only entries
                # contain multiple documents in one PDF, hard to segment)
                continue

            # Ensure full URL
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url

            # Download PDF and extract text
            text = self._download_and_extract(pdf_url)
            if not text or len(text) < 100:
                logger.debug("Skipping %s — insufficient text (%d chars)",
                             item.get("title", "")[:60], len(text) if text else 0)
                continue

            text = _clean_text(text)

            yield {
                "title": item["title"],
                "pdf_url": pdf_url,
                "jo_url": item.get("jo_url", ""),
                "text": text,
            }

    def _download_and_extract(self, pdf_url: str) -> str:
        """Download a PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = SESSION.get(pdf_url, timeout=60)
            resp.raise_for_status()

            if len(resp.content) > 10 * 1024 * 1024:  # Skip >10MB
                logger.warning("PDF too large (%d bytes): %s", len(resp.content), pdf_url)
                return ""

            return _extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning("PDF download failed for %s: %s", pdf_url, e)
            return ""


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="CG/SGG-TextesOfficiels bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "test", "update"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch ~15 sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    scraper = SGGTextesOfficielsScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity to sgg.cg ...")
        try:
            resp = SESSION.get(SEARCH_URL, params={"field_idType": "12", "action": "Rechercher"}, timeout=15)
            resp.raise_for_status()
            total = _get_total_results(resp.text)
            logger.info("Search OK — %d total Loi results", total)
        except Exception as e:
            logger.error("Connectivity test failed: %s", e)
            sys.exit(1)
        return

    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    records = []
    gen = scraper._fetch_documents(sample=args.sample)

    for record in gen:
        normalized = scraper.normalize(record)
        records.append(normalized)
        fname = re.sub(r'[^\w\-.]', '_', normalized["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        logger.info("[%d] %s — %d chars", len(records),
                    normalized.get("reference_number", "?"), len(normalized.get("text", "")))

    logger.info("Done. %d records saved to %s", len(records), sample_dir)

    text_lens = [len(r.get("text", "")) for r in records]
    if text_lens:
        logger.info(
            "Text stats: min=%d, max=%d, avg=%d chars",
            min(text_lens), max(text_lens), sum(text_lens) // len(text_lens),
        )


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
