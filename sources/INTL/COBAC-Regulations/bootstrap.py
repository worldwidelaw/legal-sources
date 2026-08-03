#!/usr/bin/env python3
"""
INTL/COBAC-Regulations -- Commission Bancaire de l'Afrique Centrale (COBAC)
                          Banking Regulations for the CEMAC zone

Fetches the consolidated banking regulations ("règlements COBAC") published by
the Commission Bancaire de l'Afrique Centrale — the supranational banking
supervisor of the CEMAC monetary union (Cameroon, Central African Republic,
Chad, Republic of the Congo, Equatorial Guinea, Gabon). The texts are hosted
as PDFs on the BEAC (Bank of Central African States) website.

Strategy:
  - Scrape the single listing page
        https://www.beac.int/supervision-bancaire/reglements-de-cobac/
    for every linked PDF under /wp-content/uploads/.
  - Deduplicate near-identical filename variants (e.g. foo.pdf / foo-1.pdf).
  - Download + extract full text from each PDF via common/pdf_extract.py.
  - Many older règlements are scanned images with no text layer — those are
    skipped (text below MIN_TEXT_CHARS). ~37 of the ~79 listed PDFs carry an
    extractable text layer, which is what this source ingests.

Note: the sibling source CG/BEAC-Regulations is blocked because *its* corpus is
all scanned images; the COBAC règlements page is different — a substantial
share of the regulation PDFs have a real text layer, verified before building.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # High-throughput full pull (VPS)
  python bootstrap.py update             # Re-scan listing (idempotent via Neon)
  python bootstrap.py test               # Quick connectivity test
"""

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
logger = logging.getLogger("legal-data-hunter.INTL.COBAC-Regulations")

BASE = "https://www.beac.int"
LISTING_URL = BASE + "/supervision-bancaire/reglements-de-cobac/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
}

MIN_TEXT_CHARS = 500  # below this we treat the PDF as scanned / no text layer


class COBACRegulationsScraper(BaseScraper):
    """
    Scraper for INTL/COBAC-Regulations.
    Country: INTL (CEMAC regional)
    URL: https://www.beac.int/supervision-bancaire/reglements-de-cobac/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── listing ────────────────────────────────────────────────────
    def _list_pdfs(self) -> list[dict]:
        """Scrape the listing page; return deduplicated PDF metadata dicts."""
        try:
            r = self.session.get(LISTING_URL, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Listing page failed: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        items: list[dict] = []
        seen_keys: set[str] = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            if "/wp-content/uploads/" not in href:
                continue
            url = href if href.startswith("http") else BASE + href
            fname = url.rsplit("/", 1)[-1]
            link_text = re.sub(r"\s+", " ", a.get_text(strip=True))

            # Collapse WordPress duplicate variants: foo.pdf / foo-1.pdf -> same
            # key. Only strip a single trailing "-N" marker so genuine reg
            # numbers like "R-2009-03" are not truncated to "R-2009".
            key = re.sub(r"-\d$", "", fname.rsplit(".", 1)[0].lower())
            if key in seen_keys:
                continue
            seen_keys.add(key)

            items.append({
                "pdf_url": url,
                "filename": fname,
                "link_text": link_text,
            })

        logger.info(f"Collected {len(items)} unique regulation PDFs")
        return items

    # ── parsing helpers ────────────────────────────────────────────
    @staticmethod
    def _doc_id(filename: str) -> str:
        stem = filename.rsplit(".", 1)[0]
        return "COBAC-" + re.sub(r"[^0-9A-Za-z]+", "-", stem).strip("-")

    @staticmethod
    def _guess_year(url: str, filename: str) -> Optional[int]:
        """Best-effort year from the filename, else the upload-path /YYYY/MM/."""
        stem = filename.rsplit(".", 1)[0]
        # 4-digit year embedded in the filename (e.g. r_2018, 2009, rapcobac2010)
        for m in re.finditer(r"(19\d{2}|20\d{2})", stem):
            y = int(m.group(1))
            if 1980 <= y <= 2100:
                return y
        # 2-digit year patterns: cbR-93_01, R-09, N-04-18 (take the first 2-digit)
        m = re.search(r"[-_](\d{2})(?:[-_]|$)", stem)
        if m:
            yy = int(m.group(1))
            year = 1900 + yy if yy > 50 else 2000 + yy
            if 1980 <= year <= 2100:
                return year
        # Fall back to the WordPress upload path /YYYY/MM/
        m = re.search(r"/uploads/(\d{4})/(\d{2})/", url)
        if m:
            y = int(m.group(1))
            if 1980 <= y <= 2100:
                return y
        return None

    @staticmethod
    def _guess_date(url: str, filename: str, year: Optional[int]) -> Optional[str]:
        """ISO date — prefer the upload-path month, else YYYY-01-01."""
        m = re.search(r"/uploads/(\d{4})/(\d{2})/", url)
        if m and year and int(m.group(1)) == year:
            return f"{year:04d}-{int(m.group(2)):02d}-01"
        if year:
            return f"{year:04d}-01-01"
        return None

    @staticmethod
    def _title(text: str, link_text: str, filename: str) -> str:
        """Build a human title from the first heading lines of the PDF text."""
        # Look for a "REGLEMENT ..." heading near the top of the document.
        head = text[:1500]
        lines = [re.sub(r"\s+", " ", ln).strip()
                 for ln in head.splitlines() if ln.strip()]
        ref = None
        desc_parts: list[str] = []
        for ln in lines[:20]:
            up = ln.upper()
            if ref is None and re.match(r"(REGLEMENT|RÈGLEMENT|R[ÈE]GLT|DECISION|D[ÉE]CISION|INSTRUCTION|LETTRE|CIRCULAIRE)", up):
                ref = ln
                continue
            if ref is not None:
                desc_parts.append(ln)
                # Stop once we have a reasonable descriptive clause
                if len(" ".join(desc_parts)) > 80:
                    break
        if ref:
            title = (ref + " " + " ".join(desc_parts)).strip()
            title = re.sub(r"\s+", " ", title)
            return title[:300]
        # Fallbacks
        if link_text and len(link_text) > 6 and not link_text.lower().endswith(".pdf"):
            return link_text[:300]
        return filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").strip()[:300]

    # ── schema ─────────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw["pdf_url"]
        filename = raw["filename"]
        doc_id = self._doc_id(filename)

        text = extract_pdf_markdown(
            source="INTL/COBAC-Regulations",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="legislation",
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            # Scanned image / no text layer, or already in Neon — skip.
            return None
        text = text.strip()
        time.sleep(1.0)  # politeness between downloads

        year = self._guess_year(pdf_url, filename)
        date = self._guess_date(pdf_url, filename, year)
        title = self._title(text, raw.get("link_text", ""), filename)

        return {
            "_id": doc_id,
            "_source": "INTL/COBAC-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "year": year,
            "url": pdf_url,
            "source_page": LISTING_URL,
            "issuer": "Commission Bancaire de l'Afrique Centrale (COBAC)",
            "jurisdiction": "CEMAC",
            "language": "fr",
        }

    # ── fetch ──────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW PDF metadata; normalize() downloads + extracts full text."""
        items = self._list_pdfs()
        for it in items:
            yield it

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No per-item modified date; re-scan listing (idempotent via Neon)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/COBAC-Regulations fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of samples")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    bf = sub.add_parser("bootstrap-fast", help="High-throughput full fetch (VPS)")
    bf.add_argument("--full", action="store_true", default=True)

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = COBACRegulationsScraper()

    if args.command == "test":
        items = scraper._list_pdfs()
        logger.info(f"OK: {len(items)} PDFs listed")
        if items:
            rec = scraper.normalize(items[0])
            if rec:
                logger.info(f"First: {rec['title'][:120]!r} ({len(rec['text'])} chars)")
            else:
                logger.info(f"First PDF had no extractable text: {items[0]['filename']}")
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command in ("bootstrap-fast", "bootstrap_fast"):
        stats = scraper.bootstrap_fast()
        logger.info(f"Fast bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
