#!/usr/bin/env python3
"""
NA/LRDC-LegislationReform — Namibia Law Reform & Development Commission reports

The Law Reform and Development Commission (LRDC) of Namibia publishes its
reports, discussion papers, working papers and concept papers as official
publications (each numbered "LRDC N", ISSN 1026-8405). The Legal Assistance
Centre (https://www.lac.org.na) hosts the complete, born-digital PDF set in an
open Apache directory index at /laws/LRDC/. NamibLII (the LRDC's own LII project)
mirrors the same documents, but its document pages sit behind a Cloudflare JS
challenge, so we collect from the LAC directory instead.

Strategy:
  1. List every *.pdf in the /laws/LRDC/ Apache directory index.
  2. Download each PDF and extract full text with pdfplumber.
  3. Derive a clean title from the filename; parse the publication date and the
     "LRDC N" series number from the PDF cover text.
  4. Drop image-only scans with a Latin-text quality filter.

Content is English, free access, no authentication.

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
logger = logging.getLogger("legal-data-hunter.NA.LRDC-LegislationReform")

BASE_URL = "https://www.lac.org.na"
INDEX_PATH = "/laws/LRDC/"
SOURCE_ID = "NA/LRDC-LegislationReform"

MAX_PDF_BYTES = 60_000_000
MAX_PDF_PAGES = 200
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
    """Reject image-only scans: real prose has many long word tokens."""
    if len(text) < MIN_TEXT_CHARS:
        return False
    tokens = [t for t in text.split() if any(ch.isalpha() for ch in t)]
    if len(tokens) < 80:
        return False
    long_tokens = [t for t in tokens if len(t) >= 4]
    return (len(long_tokens) / len(tokens)) >= 0.35


def _title_from_filename(fname: str) -> str:
    """27-LRDC-Locus_Standi_Discussion_Paper.pdf -> 'Locus Standi Discussion Paper'."""
    stem = re.sub(r"\.pdf$", "", fname, flags=re.I)
    # Drop the leading "NN-LRDC-" / "NN-LRD-" prefix.
    stem = re.sub(r"^\d+\s*-\s*LRD[C]?\s*-\s*", "", stem)
    stem = stem.replace("_", " ").replace("-", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    # Drop a trailing publication-year parenthetical, e.g. "Rape(1997)".
    stem = re.sub(r"\s*\((?:19|20)\d{2}\)\s*$", "", stem).strip()
    return stem or fname


def _number_from_filename(fname: str) -> Optional[str]:
    """The authoritative "LRDC N" series number is the filename prefix."""
    m = re.match(r"\s*(\d{1,3})\s*-\s*LRD", fname)
    return m.group(1) if m else None


def _parse_date(text: str) -> Optional[str]:
    """Best-effort ISO date from the cover page, e.g. 'Windhoek, Namibia /
    March 2014' (month + year) or '5 April 2014'. Falls back to a bare year."""
    head = text[:1500]
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", head)
    if m:
        d = int(m.group(1)); mo = MONTHS.get(m.group(2).lower()); y = int(m.group(3))
        if mo and 1900 < y < 2100:
            try:
                return datetime(y, mo, d).date().isoformat()
            except ValueError:
                pass
    m = re.search(r"\b([A-Za-z]+)\s+((?:19|20)\d{2})\b", head)
    if m and m.group(1).lower() in MONTHS and MONTHS[m.group(1).lower()]:
        return f"{m.group(2)}-{MONTHS[m.group(1).lower()]:02d}-01"
    m = re.search(r"\b(19|20)\d{2}\b", head)
    if m:
        return f"{m.group(0)}-01-01"
    return None


class LRDCScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
            respect_robots=False,
        )

    def _list_pdfs(self) -> list[dict]:
        """Parse the Apache directory index at /laws/LRDC/ for *.pdf files."""
        url = urljoin(BASE_URL, INDEX_PATH)
        try:
            resp = self.http.get(url, timeout=60)
        except Exception as e:
            logger.warning("Index fetch error: %s", e)
            return []
        if resp.status_code != 200:
            logger.warning("Index returned %s", resp.status_code)
            return []
        docs: dict[str, dict] = {}
        for href in re.findall(r'href="([^"]+\.pdf)"', resp.text, re.I):
            fname = unquote(href.split("/")[-1])
            if fname in docs:
                continue
            docs[fname] = {
                "fname": fname,
                "url": urljoin(url, href),
                "title": _title_from_filename(fname),
            }
        logger.info("Found %d LRDC PDFs in directory index", len(docs))
        return list(docs.values())

    def _extract_pdf(self, url: str) -> Optional[str]:
        # Cheap pre-check: skip oversized (usually image-only) PDFs without
        # downloading the whole body — the LAC host is slow and flaky.
        try:
            head = self.http.session.head(url, timeout=30, allow_redirects=True)
            clen = int(head.headers.get("Content-Length", 0) or 0)
            if clen > MAX_PDF_BYTES:
                logger.info("Skip oversized PDF (%d bytes, HEAD): %s", clen, url[:90])
                return None
        except Exception:
            pass
        try:
            resp = self.http.get(url, timeout=120)
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

    def _build(self, doc: dict) -> Optional[dict]:
        text = self._extract_pdf(doc["url"])
        if not text or not _is_clean(text):
            return None
        rec = {
            "_id": "na-lrdc-" + re.sub(r"\.pdf$", "", doc["fname"], flags=re.I).lower(),
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": doc["title"],
            "text": text,
            "date": _parse_date(text),
            "url": doc["url"],
            "language": "en",
            "publisher": "Law Reform and Development Commission of Namibia",
        }
        num = _number_from_filename(doc["fname"])
        if num:
            rec["series_number"] = f"LRDC {num}"
        return rec

    # ── BaseScraper interface ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for doc in self._list_pdfs():
            rec = self._build(doc)
            if rec:
                yield rec
            time.sleep(1.2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NA/LRDC-LegislationReform scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = LRDCScraper()

    if args.command == "test-api":
        docs = scraper._list_pdfs()
        logger.info("Total LRDC PDFs discovered: %d", len(docs))
        for d in docs[:15]:
            logger.info("  %s — %s", d["fname"], d["title"][:60])
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
                    record["title"][:55], len(record["text"]), record.get("date"))
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
