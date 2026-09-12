#!/usr/bin/env python3
"""
KH/ArbitrationCouncil -- Arbitration Council of Cambodia (labor awards)

Fetches arbitral awards of Cambodia's Arbitration Council, the tripartite
quasi-judicial body that resolves collective labor disputes under the 1997
Labour Law. ~1,940 awards are published openly since 2004 on
arbitrationcouncil.org as PDF files served via the WordPress Download
Manager (wpdm) plugin.

Access pattern:
  1. The listing page ?wpdmc=arbitral-awards enumerates every award as a
     /download/{slug}/ link (single page, no real pagination needed).
  2. Each award's /download/{slug}/ page exposes one or more download
     buttons of the form
         /download/{slug}/?wpdmdl={id}&refresh={token}
     where `refresh` is a per-request token. The PDF is fetched from that
     URL with a Referer header set to the award page.
  3. Full text is extracted from born-digital PDFs (mostly the English
     versions, 2004-2016). Khmer-only scanned awards have no text layer
     and are skipped.

Usage:
  python bootstrap.py bootstrap --sample          # Sample records
  python bootstrap.py bootstrap --sample --count 12
  python bootstrap.py bootstrap                    # Full bootstrap
  python bootstrap.py update                       # Incremental update
  python bootstrap.py test-api                     # Connectivity check
"""

import sys
import re
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KH.ArbitrationCouncil")

BASE = "https://www.arbitrationcouncil.org"
LISTING_URL = f"{BASE}/arbitral-decision/arbitral-award/?wpdmc=arbitral-awards"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# Minimum characters of extracted text for an award to count as full text.
# Scanned/image-only PDFs yield only a handful of stray characters.
MIN_TEXT_CHARS = 400

# Button href -> label, e.g. .../download/slug/?wpdmdl=123&refresh=abc  ->  English
_BTN_RE = re.compile(
    r"href='(" + re.escape(BASE) + r"/download/[^']*?\?wpdmdl=\d+&refresh=[^']+)'[^>]*>([^<]+)</a>"
)
_SLUG_RE = re.compile(r"/download/([a-z0-9\-]+)/")
_CREATE_DATE_RE = re.compile(r"Create Date\s+([A-Z][a-z]+ \d{1,2}, \d{4})")
_CASE_NO_RE = re.compile(r"(\d{1,3})\s*[/\-]\s*(\d{2})")


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF using PyMuPDF, falling back to pdfminer."""
    if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
        return ""
    text = ""
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            text = "\n".join(page.get_text() for page in doc)
        finally:
            doc.close()
    except Exception as e:  # pragma: no cover - backend availability varies
        logger.debug(f"PyMuPDF failed ({e}); trying pdfminer")
    if len(text.strip()) < MIN_TEXT_CHARS:
        try:
            import io
            from pdfminer.high_level import extract_text as _pm_extract

            alt = _pm_extract(io.BytesIO(pdf_bytes)) or ""
            if len(alt.strip()) > len(text.strip()):
                text = alt
        except Exception as e:  # pragma: no cover
            logger.debug(f"pdfminer failed ({e})")
    return _clean_text(text)


def _clean_text(text: str) -> str:
    """Collapse excessive whitespace while preserving paragraph breaks."""
    if not text:
        return ""
    # Normalise line endings and strip page form-feeds
    text = text.replace("\x0c", "\n").replace("\r", "\n")
    # Collapse runs of spaces/tabs
    text = re.sub(r"[ \t]+", " ", text)
    # Collapse 3+ newlines to a double newline
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class ArbitrationCouncilScraper(BaseScraper):
    """Scraper for KH/ArbitrationCouncil — Cambodia labor arbitral awards."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    # -- HTTP helpers -------------------------------------------------------

    def _get_slugs(self) -> list:
        """Return all award slugs, oldest-first (text-rich awards first)."""
        resp = self.session.get(LISTING_URL, timeout=90)
        resp.raise_for_status()
        slugs = list(dict.fromkeys(_SLUG_RE.findall(resp.text)))
        # Listing is newest-first; reverse so born-digital older awards
        # (which carry a text layer) are processed first.
        slugs.reverse()
        logger.info(f"Found {len(slugs)} arbitral awards in listing")
        return slugs

    def _fetch_award(self, slug: str) -> Optional[dict]:
        """Fetch one award page + PDF, returning a raw record dict."""
        page_url = f"{BASE}/download/{slug}/"
        try:
            r = self.session.get(page_url, timeout=60)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"  {slug}: page fetch failed ({e})")
            return None

        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        # Title
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.split(" - Welcome to The Arbitration")[0].strip()
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            title = h1.get_text(strip=True).replace("–", "-").strip()
        if not title:
            title = slug

        # Date (Create Date in the wpdm description block)
        date_str = None
        m = _CREATE_DATE_RE.search(html)
        if m:
            try:
                date_str = datetime.strptime(m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                date_str = None

        # Download buttons (prefer English)
        buttons = _BTN_RE.findall(html)
        if not buttons:
            logger.debug(f"  {slug}: no download button")
            return None
        chosen_url, language = buttons[0][0], buttons[0][1].strip()
        for href, label in buttons:
            if "english" in label.lower():
                chosen_url, language = href, label.strip()
                break

        # Download the PDF
        try:
            pr = self.session.get(chosen_url, timeout=120, headers={"Referer": page_url})
            pr.raise_for_status()
        except Exception as e:
            logger.warning(f"  {slug}: pdf fetch failed ({e})")
            return None

        content = pr.content
        cd = pr.headers.get("content-disposition", "")
        fn_m = re.search(r'filename="?([^";]+)"?', cd)
        pdf_filename = fn_m.group(1).strip() if fn_m else f"{slug}.pdf"

        page_count = None
        text = ""
        if content[:4] == b"%PDF":
            try:
                import fitz

                doc = fitz.open(stream=content, filetype="pdf")
                page_count = doc.page_count
                doc.close()
            except Exception:
                pass
            text = _extract_pdf_text(content)

        case_id = None
        cm = _CASE_NO_RE.search(title) or _CASE_NO_RE.search(slug)
        if cm:
            case_id = f"{cm.group(1)}/{cm.group(2)}"

        return {
            "slug": slug,
            "title": title,
            "date": date_str,
            "language": language,
            "pdf_filename": pdf_filename,
            "page_count": page_count,
            "page_url": page_url,
            "_text": text,
            "case_id": case_id,
        }

    # -- Core methods -------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw award records (with extracted text) for every award."""
        slugs = self._get_slugs()
        for slug in slugs:
            self.rate_limiter.wait()
            rec = self._fetch_award(slug)
            if rec is not None:
                yield rec

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No native since-filter; re-scan (append_only dedup skips existing)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw award into the standard schema, or None to skip."""
        text = raw.get("_text", "") or ""
        if len(text.strip()) < MIN_TEXT_CHARS:
            # Scanned / image-only award with no usable text layer — skip.
            return None

        slug = raw["slug"]
        return {
            "_id": f"KH-AC-{slug}",
            "_source": "KH/ArbitrationCouncil",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or slug,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("page_url"),
            "case_id": raw.get("case_id"),
            "language": raw.get("language"),
            "pdf_filename": raw.get("pdf_filename"),
            "page_count": raw.get("page_count"),
            "court": "Arbitration Council of Cambodia",
            "jurisdiction": "KH",
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="KH/ArbitrationCouncil award fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=12, help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ArbitrationCouncilScraper()

    if args.command == "test-api":
        print("Testing Arbitration Council listing...")
        slugs = scraper._get_slugs()
        print(f"  Awards listed: {len(slugs)}")
        if slugs:
            rec = scraper._fetch_award(slugs[0])
            if rec:
                print(f"  First award: {rec['title']} ({rec['language']}) "
                      f"chars={len(rec['_text'])} pages={rec['page_count']}")
        print("API test PASSED")

    elif args.command == "bootstrap":
        sample_mode = args.sample
        print(f"Starting bootstrap (sample={sample_mode})...")
        stats = scraper.bootstrap(sample_mode=sample_mode,
                                  sample_size=args.count if sample_mode else 10)
        print("\nBootstrap complete:")
        print(f"  Records fetched: {stats.get('records_fetched', 0)}")
        print(f"  Records new: {stats.get('records_new', 0)}")
        print(f"  Errors/skips: {stats.get('errors', 0)}")
        if sample_mode:
            print(f"  Sample records saved to: {scraper.source_dir / 'sample'}")

    elif args.command == "update":
        print("Starting incremental update...")
        stats = scraper.bootstrap(sample_mode=False)
        print("\nUpdate complete:")
        print(f"  Records new: {stats.get('records_new', 0)}")
