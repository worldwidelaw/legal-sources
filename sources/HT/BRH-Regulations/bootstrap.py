#!/usr/bin/env python3
"""
HT/BRH-Regulations — Banque de la République d'Haïti

Fetches circulars, banking laws, prudential standards, guidelines, and
lettre-circulaires from the BRH website.

Strategy:
  1. Scrape the normes prudentielles page for all PDF links (most comprehensive)
  2. Also scrape banking laws subsection pages
  3. Download each PDF and extract text with pdfminer
  4. Skip scanned PDFs with no text layer

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HT.BRH-Regulations")

BASE_URL = "https://www.brh.ht"
SOURCE_ID = "HT/BRH-Regulations"

# Pages to scrape for PDF links
PAGES = [
    {
        "url": "/supervision-bancaire/normes-prudentielles/",
        "category": "Normes Prudentielles",
    },
    {
        "url": "/supervision-bancaire/lois-bancaires/banques-commerciales/",
        "category": "Lois Bancaires — Banques Commerciales",
    },
    {
        "url": "/supervision-bancaire/lois-bancaires/banques-depargne-et-de-logement/",
        "category": "Lois Bancaires — Banques d'Épargne",
    },
    {
        "url": "/supervision-bancaire/lois-bancaires/loi-bancaire-2012/",
        "category": "Loi Bancaire 2012",
    },
]


def _extract_text_pdfminer(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        text = pdfminer_extract(io.BytesIO(pdf_bytes))
        # Filter out form-feed-only "text" from scanned PDFs
        clean = text.strip().replace("\x0c", "")
        if len(clean) > 100:
            return clean
    except Exception as e:
        logger.warning("pdfminer extraction failed: %s", e)
    return None


class BRHRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open legal data research)",
                "Accept": "text/html, application/pdf, */*",
            },
            timeout=60,
        )
        self._seen_urls = set()

    def _scrape_pdf_links(self, page_path: str, category: str) -> list[dict]:
        """Scrape a page for PDF download links."""
        url = f"{BASE_URL}{page_path}"
        logger.info("Scraping %s", url)
        try:
            resp = self.http.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning("Failed to fetch %s: HTTP %d", url, resp.status_code)
                return []
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return []

        html = resp.text

        # Try to restrict to article content area
        article_match = re.search(
            r'<article[^>]*>(.*?)</article>', html, re.DOTALL,
        )
        content = article_match.group(1) if article_match else html

        # Also try entry-content or post-content divs (WordPress)
        for div_class in ["entry-content", "post-content", "sppb-addon-content"]:
            div_match = re.search(
                rf'<div[^>]*class="[^"]*{div_class}[^"]*"[^>]*>(.*?)</div>',
                html, re.DOTALL,
            )
            if div_match and len(div_match.group(1)) > len(content):
                content = div_match.group(1)

        pattern = r'href="([^"]*\.pdf(?:\?[^"]*)?)"'
        hrefs = re.findall(pattern, content, re.IGNORECASE)

        results = []
        for href in hrefs:
            if href.startswith("/"):
                pdf_url = f"{BASE_URL}{href}"
            elif href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = urljoin(url, href)

            norm_url = pdf_url.split("?")[0]
            if norm_url in self._seen_urls:
                continue
            self._seen_urls.add(norm_url)

            # Extract title from filename
            filename = unquote(unquote(norm_url.split("/")[-1]))
            title = Path(filename).stem.replace("-", " ").replace("_", " ").strip()

            results.append({
                "pdf_url": pdf_url,
                "norm_url": norm_url,
                "title": title,
                "category": category,
            })

        logger.info("Found %d new PDF links on %s", len(results), page_path)
        return results

    def _classify_doc(self, title: str, category: str) -> tuple[str, str]:
        """Determine _type and document_type from title/category."""
        title_lower = title.lower()
        if any(w in title_lower for w in ["loi", "act", "code"]):
            return "legislation", "loi"
        if "lettre" in title_lower:
            return "doctrine", "lettre_circulaire"
        if "circulaire" in title_lower:
            return "doctrine", "circulaire"
        if "ligne" in title_lower or "guideline" in title_lower:
            return "doctrine", "guideline"
        if "note" in title_lower:
            return "doctrine", "note_additionnelle"
        if "loi" in category.lower():
            return "legislation", "loi"
        return "doctrine", "circulaire"

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            resp = self.http.get(pdf_url, timeout=90)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            return _extract_text_pdfminer(resp.content)
        except Exception as e:
            logger.warning("PDF download failed for %s: %s", pdf_url, e)
        return None

    def _normalize_record(self, pdf_info: dict) -> Optional[dict]:
        """Download PDF, extract text, and build normalized record."""
        pdf_url = pdf_info["pdf_url"]
        norm_url = pdf_info["norm_url"]
        title = pdf_info["title"]
        category = pdf_info["category"]

        filename = Path(unquote(unquote(norm_url.split("/")[-1]))).stem
        doc_id = filename

        text = self._download_and_extract(pdf_url)
        if not text:
            logger.warning("No text extracted (likely scanned): %s", title)
            return None

        _type, doc_type = self._classify_doc(title, category)

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,
            "url": norm_url,
            "document_type": doc_type,
            "category": category,
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BRH regulatory documents."""
        self._seen_urls = set()
        all_pdfs = []
        for page in PAGES:
            pdfs = self._scrape_pdf_links(page["url"], page["category"])
            all_pdfs.extend(pdfs)

        logger.info("Total unique PDFs to process: %d", len(all_pdfs))

        for pdf_info in all_pdfs:
            record = self._normalize_record(pdf_info)
            if record:
                yield record
            time.sleep(1)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No date-based API — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="HT/BRH-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BRHRegulationsScraper()

    if args.command == "test-api":
        for page in PAGES:
            links = scraper._scrape_pdf_links(page["url"], page["category"])
            logger.info("%s: %d PDFs", page["category"], len(links))
            for link in links[:3]:
                logger.info("  %s — %s", link["title"][:50], link["pdf_url"][:70])
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars",
                count,
                record["title"][:60],
                len(record.get("text", "")),
            )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
