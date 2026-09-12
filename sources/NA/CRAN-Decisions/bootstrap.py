#!/usr/bin/env python3
"""
NA/CRAN-Decisions — Communications Regulatory Authority of Namibia (CRAN)

CRAN (https://www.cran.na) is Namibia's independent regulator for
telecommunications, broadcasting, postal services and radio spectrum,
established under the Communications Act (No. 8 of 2009). It files its
regulatory output — regulations, determinations, decisions, withdrawals and
notices — in the Government Gazette, and collects every such issue on its
"Government Gazettes" page (each linked as a PDF under /wp-content/uploads/).

Strategy:
  1. List the gazette PDFs from the /government-gazettes/ page (plus the named
     final-regulation pages).
  2. Download each PDF and extract full text with pdfplumber.
  3. Build a descriptive title from the gazette CONTENTS (the CRAN notice
     headings), falling back to the gazette number + date.
  4. Drop image-only scans with a Latin-text quality filter.

Content is in English. Free access, no authentication.

Usage:
  python bootstrap.py bootstrap --sample   # sample records for validation
  python bootstrap.py bootstrap            # full pull
  python bootstrap.py update               # incremental (re-crawl)
  python bootstrap.py test-api             # connectivity / link-count check
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NA.CRAN-Decisions")

BASE_URL = "https://www.cran.na"
SOURCE_ID = "NA/CRAN-Decisions"

LISTING_PAGES = [
    "/government-gazettes/",
    "/final-regulations/",
    "/adjudication-of-disputes-regulations/",
    "/consumer-complaints-regulations/",
    "/draft-regulations/",
]

MAX_PDF_BYTES = 30_000_000
MAX_PDF_PAGES = 80
MIN_TEXT_CHARS = 500

MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"])}


def _clean_text(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_clean(text: str) -> bool:
    if len(text) < MIN_TEXT_CHARS:
        return False
    tokens = [t for t in text.split() if any(ch.isalpha() for ch in t)]
    if len(tokens) < 80:
        return False
    long_tokens = [t for t in tokens if len(t) >= 4]
    return (len(long_tokens) / len(tokens)) >= 0.35


def _parse_date(text: str) -> Optional[str]:
    head = text[:1500]
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", head)
    if m:
        d = int(m.group(1)); mo = MONTHS.get(m.group(2).lower()); y = int(m.group(3))
        if mo and 1990 < y < 2100:
            try:
                return datetime(y, mo, d).date().isoformat()
            except ValueError:
                pass
    m = re.search(r"\b(19|20)\d{2}\b", head)
    if m:
        return f"{m.group(0)}-01-01"
    return None


def _build_title(text: str, filename: str) -> str:
    """Prefer the first CRAN notice heading from the gazette CONTENTS."""
    # Gazette number, e.g. "No. 8823"
    gz = re.search(r"\bNo\.\s*(\d{3,5})\b", text[:600])
    gz_num = gz.group(1) if gz else None

    # First CRAN notice description in the CONTENTS block.
    m = re.search(
        r"Communications Regulatory Authority of Namibia[:\s]+(.+?)(?:\.{3,}|\n\s*No\.|\n\s*\d+\s*$)",
        text[:3000], re.I | re.S)
    if m:
        desc = re.sub(r"\s+", " ", m.group(1)).strip(" .")
        desc = re.sub(r"\.{2,}.*$", "", desc).strip()
        if 10 <= len(desc) <= 240:
            prefix = f"GG {gz_num}: " if gz_num else ""
            return f"{prefix}CRAN — {desc}"

    if gz_num:
        return f"Government Gazette No. {gz_num} (CRAN)"
    base = unquote(filename).rsplit(".", 1)[0]
    return re.sub(r"[_-]+", " ", base).strip()


class CRANDecisionsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
            respect_robots=False,
        )

    def _list_pdfs(self) -> list[dict]:
        seen: set[str] = set()
        out: list[dict] = []
        for page in LISTING_PAGES:
            url = urljoin(BASE_URL, page)
            try:
                resp = self.http.get(url, timeout=60)
            except Exception as e:
                logger.warning("Listing fetch error %s: %s", page, e)
                continue
            if resp.status_code != 200:
                logger.warning("Listing %s -> HTTP %d", page, resp.status_code)
                continue
            for href in re.findall(r'href="([^"]+\.pdf)"', resp.text, re.I):
                full = urljoin(url, unescape(href))
                if "/wp-content/uploads/" not in full or full in seen:
                    continue
                seen.add(full)
                out.append({"url": full, "page": page})
            logger.info("%s: %d cumulative PDF links", page, len(out))
            time.sleep(1)
        return out

    def _extract_pdf(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url, timeout=90)
        except Exception as e:
            logger.warning("PDF fetch error: %s (%s)", url[:90], e)
            return None
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data or not data[:5].startswith(b"%PDF"):
            return None
        if len(data) > MAX_PDF_BYTES:
            logger.info("Skip oversized PDF (%d bytes): %s", len(data), url[:90])
            return None
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                parts = [(p.extract_text() or "") for p in pdf.pages[:MAX_PDF_PAGES]]
        except Exception as e:
            logger.warning("PDF parse error: %s (%s)", url[:90], e)
            return None
        return _clean_text("\n".join(parts))

    def _build(self, item: dict) -> Optional[dict]:
        text = self._extract_pdf(item["url"])
        if not text or not _is_clean(text):
            return None
        filename = item["url"].rsplit("/", 1)[-1]
        doc_id = "na-cran-" + re.sub(r"[^a-z0-9]+", "-",
                                     unquote(filename).lower()).strip("-")[:90]
        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": _build_title(text, filename),
            "text": text,
            "date": _parse_date(text),
            "url": item["url"],
            "source_page": urljoin(BASE_URL, item["page"]),
            "language": "en",
        }

    # ── BaseScraper interface ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for item in self._list_pdfs():
            rec = self._build(item)
            if rec:
                yield rec
            time.sleep(1.2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ─────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="NA/CRAN-Decisions scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = CRANDecisionsScraper()

    if args.command == "test-api":
        pdfs = scraper._list_pdfs()
        logger.info("Total PDF links: %d", len(pdfs))
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    limit = 15 if args.sample else None
    count = 0
    for record in scraper.fetch_all():
        count += 1
        if args.sample or count <= 15:
            with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("[%d] %s — %d chars (%s)", count,
                    record["title"][:60], len(record["text"]), record.get("date"))
        if limit and count >= limit:
            break
    logger.info("Done: %d records", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
