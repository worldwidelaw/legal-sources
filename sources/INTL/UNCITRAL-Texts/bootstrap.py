#!/usr/bin/env python3
"""
INTL/UNCITRAL-Texts -- UNCITRAL Model Laws and Legislative Guides

Fetches UNCITRAL instruments (model laws, conventions, legislative guides,
rules, recommendations) from uncitral.un.org with full text from PDFs.

Strategy:
  - Walk the 12 category pages AND every instrument page they link to
  - Emit one record per distinct PDF, not one per page (issue #1581)
  - Download PDFs and extract full text via pdfplumber

There is no pagination anywhere in the /en/texts/ tree, so the ~50 records the
fleet reported were not a page cap: they were one-per-page. A single page
routinely carries several genuinely distinct instruments — the Legislative
Guide on Insolvency Law publishes Parts 1-2, 3 and 4 as separate documents,
/arbitration/contractualtexts/arbitration holds the 1976, 2010, 2013 and 2021
Arbitration Rules — and the old code kept only the longest PDF per page and
discarded the rest. Two category pages (isds, onlinedispute) are themselves
instrument landings that link no sub-pages at all, so the whole ISDS reform
package (Codes of Conduct for Arbitrators and Judges, Model Provisions and
Guidelines on Mediation, the Advisory Centre Statute, the 2025 Prevention
Toolkit) and the ODR Technical Notes were never discovered at all.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
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
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNCITRAL-Texts")

BASE_URL = "https://uncitral.un.org"
TEXTS_URL = f"{BASE_URL}/en/texts"

CATEGORIES = [
    ("arbitration", "International Commercial Arbitration"),
    ("mediation", "International Commercial Mediation"),
    ("isds", "Investor-State Dispute Settlement"),
    ("ecommerce", "Electronic Commerce"),
    ("salegoods", "International Sale of Goods"),
    ("msmes", "Micro, Small and Medium-sized Enterprises"),
    ("insolvency", "Insolvency"),
    ("securityinterests", "Security Interests"),
    ("onlinedispute", "Online Dispute Resolution"),
    ("payments", "Payments and Trade Finance"),
    ("procurement", "Procurement and Public-Private Partnerships"),
    ("transportgoods", "International Transport of Goods"),
]

# pdfplumber's placeholder for a glyph it cannot map to Unicode.
CID_GLYPH_RE = re.compile(r'\(cid:\d+\)')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
}


class UNCITRALTextsScraper(BaseScraper):
    SOURCE_ID = "INTL/UNCITRAL-Texts"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("Failed to fetch %s: %s", url, e)
                    return None
                time.sleep(2 * (attempt + 1))

    def _download_pdf_text(self, pdf_url: str, source_id: str = "") -> Optional[str]:
        """Extract text from a PDF, downloading it ourselves.

        uncitral.un.org's WAF 403s the extractor's own download (A/B'd
        2026-09-07: same URL and second, requests' default User-Agent → 403
        text/html, our `LegalDataHunter/1.0` and a Chrome UA → 200
        application/pdf). Handing `pdf_url` to the extractor therefore failed
        on *every* document and the scraper silently fell back to the page's
        one-paragraph description — which is why records looked complete while
        holding no instrument text. Fetch the bytes on our own session and
        pass `pdf_bytes=` instead (same fix as #1338 US/CFPB).
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("PDF download failed %s: %s", pdf_url, e)
            return ""

        ctype = resp.headers.get("Content-Type", "")
        if "pdf" not in ctype.lower() and not resp.content.startswith(b"%PDF"):
            logger.warning("Not a PDF (%s): %s", ctype, pdf_url)
            return ""

        text = extract_pdf_markdown(
            source="INTL/UNCITRAL-Texts",
            source_id=source_id or pdf_url,
            pdf_bytes=resp.content,
            table="legislation",
        ) or ""

        # The 1985 Model Law scan (06-54671_ebook.pdf) embeds a font with no
        # usable ToUnicode map, so pdfplumber emits "(cid:14)(cid:10)..."
        # placeholders. That is non-empty and would sail past a length check
        # while being unsearchable, so reject it explicitly.
        if text:
            cid_chars = sum(len(m) for m in CID_GLYPH_RE.findall(text))
            if cid_chars > 0.2 * len(text):
                logger.warning(
                    "Unmapped-glyph extraction (%.0f%% cid placeholders), "
                    "discarding: %s", 100 * cid_chars / len(text), pdf_url
                )
                return ""

        return text

    def _discover_pages(self) -> List[Dict]:
        """Every page in the /en/texts/ tree that can carry an instrument.

        That is the 12 category pages themselves plus every instrument page
        they link to. The category pages must be included: `isds` and
        `onlinedispute` link no sub-pages at all and host their instruments
        directly, so a sub-links-only walk misses them entirely (issue #1581).
        """
        pages: List[Dict] = []
        seen_urls: Set[str] = set()

        for cat_slug, cat_name in CATEGORIES:
            cat_url = f"{TEXTS_URL}/{cat_slug}"
            logger.info("Scanning category: %s", cat_name)
            self.rate_limiter.wait()
            html = self._fetch_page(cat_url)
            if not html:
                raise RuntimeError(
                    f"uncitral.un.org category page {cat_url} unreachable — "
                    "refusing to report a short corpus"
                )

            seen_urls.add(cat_url)
            pages.append({
                "url": cat_url,
                "title": cat_name,
                "category": cat_name,
                "category_slug": cat_slug,
                "html": html,
            })

            soup = BeautifulSoup(html, "html.parser")
            for link in soup.find_all("a", href=True):
                full_url = urljoin(BASE_URL, link["href"]).split("?")[0].rstrip("/")
                # Instrument pages live below the category; skip the status,
                # travaux préparatoires and working-group trees.
                if not full_url.startswith(f"{BASE_URL}/en/texts/{cat_slug}/"):
                    continue
                if any(skip in full_url for skip in
                       ("/status", "/travaux", "/working_group", "/clout", "#")):
                    continue
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    continue

                pages.append({
                    "url": full_url,
                    "title": title,
                    "category": cat_name,
                    "category_slug": cat_slug,
                })

        logger.info("Discovered %d pages across %d categories",
                    len(pages), len(CATEGORIES))
        return pages

    def _discover_documents(self) -> List[Dict]:
        """One entry per distinct PDF across the whole tree.

        A page routinely carries several separate instruments (the four
        successive editions of the Arbitration Rules, the four Parts of the
        Legislative Guide on Insolvency Law), so keying records on the page
        rather than the document silently dropped most of the corpus.
        """
        docs: List[Dict] = []
        seen_pdfs: Set[str] = set()
        skipped_external = 0

        for page in self._discover_pages():
            html = page.get("html")
            if html is None:
                self.rate_limiter.wait()
                html = self._fetch_page(page["url"])
            if not html:
                logger.warning("Page unreachable, skipping: %s", page["url"])
                continue

            soup = BeautifulSoup(html, "html.parser")
            description = self._extract_description(html)

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if not href.lower().split("?")[0].endswith(".pdf"):
                    continue
                pdf_url = urljoin(page["url"], href)
                # UNCITRAL pages also link third-party copies (americanbar.org,
                # cfa.com) and at least one malformed href whose "host" is the
                # filename itself (https://recommendations_eng_ebook.pdf).
                # Only its own hosted documents are the authentic corpus.
                if not pdf_url.startswith(f"{BASE_URL}/"):
                    skipped_external += 1
                    logger.debug("Skipping off-site PDF %s (on %s)",
                                 pdf_url, page["url"])
                    continue
                if pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(pdf_url)

                # The anchor text is the instrument's own name and carries its
                # adoption year — "UNCITRAL Code of Conduct for Arbitrators ...
                # (2023)" — which is a far better title and date than anything
                # recoverable from the PDF body.
                doc_title = link.get_text(" ", strip=True)
                if not doc_title or len(doc_title) < 5:
                    doc_title = f"{page['title']} ({pdf_url.rsplit('/', 1)[-1]})"

                docs.append({
                    "pdf_url": pdf_url,
                    "title": doc_title,
                    "page_url": page["url"],
                    "page_title": page["title"],
                    "category": page["category"],
                    "description": description,
                })

        if not docs:
            raise RuntimeError(
                "No PDFs discovered anywhere under uncitral.un.org/en/texts — "
                "layout changed or the request was blocked"
            )
        logger.info("Discovered %d distinct documents (%d off-site links skipped)",
                    len(docs), skipped_external)
        return docs

    def _extract_pdf_urls(self, html: str) -> List[str]:
        """Extract PDF download links from an instrument page."""
        soup = BeautifulSoup(html, "html.parser")
        pdf_urls = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".pdf"):
                full_url = urljoin(BASE_URL, href)
                # Prefer English PDFs
                if full_url not in pdf_urls:
                    pdf_urls.append(full_url)
        return pdf_urls

    def _extract_description(self, html: str) -> str:
        """Extract the description text from the instrument page."""
        soup = BeautifulSoup(html, "html.parser")
        # Main content area
        content = soup.find("div", class_="field--name-body") or soup.find("article")
        if content:
            for tag in content.find_all(["script", "style", "nav"]):
                tag.decompose()
            text = content.get_text(separator="\n")
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()
        return ""

    def _extract_date(self, text: str, title: str) -> Optional[str]:
        """Best available adoption year for an instrument.

        UNCITRAL names its documents with the adoption year in parentheses —
        "UNCITRAL Model Law on International Commercial Arbitration (1985,
        with amendments as adopted in 2006)" — so a trailing parenthesised
        year in the anchor text is authoritative. Prefer the LAST such year so
        an amended instrument dates from its amendment, and only fall back to
        scanning the PDF body when the title carries no year at all.
        """
        title_years = re.findall(r'\((?:[^()]*?)\b(19[4-9]\d|20[0-4]\d)\b[^()]*\)', title)
        if title_years:
            return f"{title_years[-1]}-01-01"

        bare = re.findall(r'\b(19[4-9]\d|20[0-4]\d)\b', title)
        if bare:
            return f"{bare[-1]}-01-01"

        years = re.findall(r'\b(19[4-9]\d|20[0-4]\d)\b', text[:2000])
        if years:
            return f"{years[0]}-01-01"
        return None

    def test_connection(self) -> bool:
        try:
            html = self._fetch_page(TEXTS_URL)
            if html and "UNCITRAL" in html:
                logger.info("Connection OK: UNCITRAL texts index accessible")
                return True
            return False
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        docs = self._discover_documents()
        logger.info("Processing %d documents...", len(docs))

        for i, doc in enumerate(docs):
            logger.info("[%d/%d] %s", i + 1, len(docs), doc["title"][:70])
            full_text = self._download_pdf_text(doc["pdf_url"])

            if not full_text:
                # A scanned or withdrawn PDF still leaves the page's own
                # description, which is a real (if short) account of the
                # instrument; keep it rather than losing the document.
                full_text = doc.get("description", "")
                if not full_text:
                    logger.warning("No text for: %s (%s)",
                                   doc["title"], doc["pdf_url"])
                    continue
                logger.warning("PDF yielded no text, falling back to page "
                               "description: %s", doc["pdf_url"])

            yield {
                "title": doc["title"],
                "url": doc["page_url"],
                "pdf_url": doc["pdf_url"],
                "category": doc["category"],
                "text": full_text,
                "description": doc.get("description", ""),
            }

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        return
        yield

    def normalize(self, raw: dict) -> dict:
        title = raw["title"]
        page_url = raw["url"]
        pdf_url = raw.get("pdf_url", "")

        # Key on the document, not the page: several distinct instruments
        # share one page, and keying on the page collapsed them onto a single
        # _id so all but one were dropped by the upsert (issue #1581).
        ident = pdf_url or page_url
        path = ident.replace(f"{BASE_URL}/", "").strip("/")
        safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', path)[-150:]

        date_str = self._extract_date(raw.get("text", ""), title)

        return {
            "_id": f"UNCITRAL-{safe_id}",
            "_source": "INTL/UNCITRAL-Texts",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date_str,
            "url": pdf_url or page_url,
            "page_url": page_url,
            "pdf_url": pdf_url,
            "category": raw.get("category", ""),
            "description": raw.get("description", ""),
        }

def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/UNCITRAL-Texts Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UNCITRALTextsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        # Use the framework's bootstrap so the full run streams to
        # data/records.jsonl. The old run_bootstrap() wrote every record of a
        # full run into sample/ instead, which is the #798-class mismatch that
        # left the pipeline re-ingesting sample files.
        result = scraper.bootstrap(sample_mode=args.sample)
        print(f"Bootstrap complete: {result}")
    elif args.command == "update":
        logger.info("No update mechanism (instruments rarely change)")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
