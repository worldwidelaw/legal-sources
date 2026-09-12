#!/usr/bin/env python3
"""
GN/BCRG-Regulations -- Central Bank of Guinea Regulatory Texts

Fetches regulatory PDFs from www.bcrg-guinee.org. The site hosts banking laws,
insurance codes, microfinance regulations, AML/compliance texts, monetary policy
instructions, and payment systems regulations as PDFs embedded in WordPress pages.

Strategy:
  - Parse sitemap for all page URLs
  - Filter for regulatory/legal content pages by slug keywords
  - For each page, extract PDF URLs from dFlip JSON configs and <a> links
  - Deduplicate PDFs across pages
  - Download and extract text via common/pdf_extract
  - Skip scanned-image PDFs with no extractable text

Usage:
  python bootstrap.py bootstrap          # Fetch all regulatory texts
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from urllib.parse import urljoin, unquote
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GN.BCRG-Regulations")

BASE_URL = "https://www.bcrg-guinee.org"
SITEMAP_URL = f"{BASE_URL}/wp-sitemap-posts-page-1.xml"

# Keywords to identify regulatory/legal content pages
LEGAL_SLUGS = {
    "loi", "reglement", "texte", "code", "instruction", "circulaire",
    "decision", "decret", "arrete", "ordonnance", "statut", "convention",
    "capital", "agrement", "controle", "conformit", "assurance",
    "microfinance", "bancaire", "reglementation", "cadre",
    "systemes-de-paiement", "anti-blanchiment", "monetaire",
    "change", "reserves-obligatoires", "open-market", "taux-directeur",
    "credit-bail", "solvabilit", "liquidit", "fonds-de-garantie",
    "paiement", "devises",
}

# PDFs that appear in site-wide header/footer/sidebar — skip these
GLOBAL_PDFS = {
    "ATTRIBUTIONS-Direction-SMP.pdf",
}


def _slug_matches(url: str) -> bool:
    """Check if a page URL matches regulatory content by slug keywords."""
    parts = url.rstrip("/").split("/")
    slug = parts[-1].lower() if parts else ""
    return any(kw in slug for kw in LEGAL_SLUGS)


def _make_doc_id(pdf_url: str) -> str:
    """Create a stable document ID from the PDF filename."""
    filename = unquote(pdf_url.rsplit("/", 1)[-1])
    filename = re.sub(r"\.pdf$", "", filename, flags=re.I)
    # Normalize: replace spaces/special chars with hyphens
    clean = re.sub(r"[^\w-]", "-", filename)
    clean = re.sub(r"-+", "-", clean).strip("-")
    return f"GN-BCRG-{clean[:80]}"


def _extract_date_from_text(title: str, pdf_name: str, pdf_url: str = "") -> str:
    """Try to extract a date from the title, PDF filename, or upload path."""
    for text in [title, pdf_name]:
        # Pattern: "dd/mm/yyyy"
        m = re.search(r"(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})", text)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    # Pattern: plausible 4-digit year (1900-2099) in title or filename
    for text in [title, pdf_name]:
        m = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text)
        if m:
            return f"{m.group(1)}-01-01"
    # Fallback: year from wp-content/uploads/YYYY/MM path
    if pdf_url:
        m = re.search(r"/uploads/(20\d{2})/(\d{2})/", pdf_url)
        if m:
            return f"{m.group(1)}-{m.group(2)}-01"
    return ""


def _categorize(title: str, pdf_name: str) -> str:
    """Determine regulatory category from title/filename."""
    combined = (title + " " + pdf_name).lower()
    if any(k in combined for k in ["assurance", "reassurance"]):
        return "insurance"
    if any(k in combined for k in ["blanchiment", "lbc", "conformit"]):
        return "aml_compliance"
    if any(k in combined for k in ["microfinance", "ifi", "imf", "inclusif"]):
        return "microfinance"
    if any(k in combined for k in ["monetaire", "taux", "reserve", "open-market", "liquidit"]):
        return "monetary_policy"
    if any(k in combined for k in ["change", "devise", "virement"]):
        return "foreign_exchange"
    if any(k in combined for k in ["paiement", "payment"]):
        return "payment_systems"
    if any(k in combined for k in ["bancaire", "banque", "credit", "capital"]):
        return "banking"
    return "regulation"


class BCRGScraper(BaseScraper):
    """Scraper for GN/BCRG-Regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf",
        })

    def _get_legal_pages(self) -> List[str]:
        """Fetch sitemap and return URLs of regulatory/legal pages."""
        for attempt in range(3):
            try:
                resp = self.session.get(SITEMAP_URL, timeout=30)
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"Sitemap attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    return []

        root = ElementTree.fromstring(resp.content)
        ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        all_urls = [loc.text for loc in root.findall(".//s:loc", ns)]
        legal_urls = [u for u in all_urls if _slug_matches(u)]
        logger.info(f"Sitemap: {len(all_urls)} total pages, {len(legal_urls)} legal pages")
        return legal_urls

    def _extract_pdfs_from_page(self, page_url: str) -> List[Tuple[str, str]]:
        """Extract (pdf_url, title) pairs from a page."""
        try:
            time.sleep(2)
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch {page_url}: {e}")
            return []

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        page_title = ""
        h1 = soup.find("h1")
        if h1:
            page_title = h1.get_text(strip=True)

        results = []
        seen_pdfs = set()

        # Pattern 1: dFlip JSON config with "source" field
        for m in re.finditer(r'\{[^{}]*"source"\s*:\s*"([^"]*\.pdf[^"]*)"[^{}]*\}', html):
            raw_url = m.group(1)
            # Unescape JSON slashes and unicode
            pdf_url = raw_url.replace("\\/", "/")
            try:
                pdf_url = pdf_url.encode().decode("unicode_escape")
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL, pdf_url)
            fname = pdf_url.rsplit("/", 1)[-1]
            if fname not in GLOBAL_PDFS and pdf_url not in seen_pdfs:
                seen_pdfs.add(pdf_url)
                results.append((pdf_url, page_title))

        # Pattern 2: Direct <a> links to PDFs in wp-content/uploads
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower() or "wp-content/uploads" not in href:
                continue
            pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            fname = unquote(pdf_url.rsplit("/", 1)[-1])
            if fname in GLOBAL_PDFS or pdf_url in seen_pdfs:
                continue
            seen_pdfs.add(pdf_url)
            link_text = a.get_text(strip=True)
            title = link_text if link_text and link_text.lower() != "consulter" else page_title
            results.append((pdf_url, title))

        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "GN/BCRG-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["page_url"],
            "pdf_url": raw["pdf_url"],
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all regulatory PDFs from BCRG website."""
        existing = preload_existing_ids("GN/BCRG-Regulations", table="legislation")
        legal_pages = self._get_legal_pages()
        if not legal_pages:
            logger.error("No legal pages found in sitemap")
            return

        # Collect all unique PDFs across all pages
        all_pdfs: Dict[str, Tuple[str, str, str]] = {}  # pdf_url -> (title, page_url, slug)
        for page_url in legal_pages:
            slug = page_url.rstrip("/").split("/")[-1]
            logger.info(f"Scanning page: {slug}")
            pdfs = self._extract_pdfs_from_page(page_url)
            for pdf_url, title in pdfs:
                if pdf_url not in all_pdfs:
                    all_pdfs[pdf_url] = (title, page_url, slug)

        logger.info(f"Found {len(all_pdfs)} unique PDFs across {len(legal_pages)} pages")

        count = 0
        for pdf_url, (title, page_url, slug) in all_pdfs.items():
            doc_id = _make_doc_id(pdf_url)
            if doc_id in existing:
                logger.debug(f"Skipping {doc_id} — already in Neon")
                continue

            pdf_name = unquote(pdf_url.rsplit("/", 1)[-1])
            logger.info(f"Extracting: {pdf_name[:60]}")

            try:
                text = extract_pdf_markdown(
                    source="GN/BCRG-Regulations",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for {pdf_name}: {e}")
                text = None

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {pdf_name}: {len(text) if text else 0} chars")
                continue

            date = _extract_date_from_text(title, pdf_name, pdf_url)
            category = _categorize(title, pdf_name)

            entry = {
                "doc_id": doc_id,
                "title": title or pdf_name.replace(".pdf", "").replace("-", " "),
                "text": text,
                "date": date,
                "pdf_url": pdf_url,
                "page_url": page_url,
                "category": category,
            }
            count += 1
            yield entry

        logger.info(f"Completed: {count} regulatory texts fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates — same as fetch_all since the corpus is small."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GN/BCRG-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BCRGScraper()

    if args.command == "test":
        logger.info("Testing connectivity to BCRG website...")
        try:
            resp = scraper.session.get(BASE_URL, timeout=30)
            resp.raise_for_status()
            logger.info(f"Connection OK — status {resp.status_code}")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            sys.exit(1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
