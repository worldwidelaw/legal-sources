#!/usr/bin/env python3
"""
US/UT-PSC -- Utah Public Service Commission Orders

Fetches the full text of Orders issued by the Public Service Commission of
Utah (PSC) adjudicating utility dockets (electric, natural gas, water,
telecommunications). Each Order is an administrative adjudication /
disposition of a specific docket by the Commission = case_law. Public
domain (US state government edict).

Strategy (official public PSC site, psc.utah.gov -- a WordPress site whose
document store is the pscdocs.utah.gov S3 bucket):

  1. Enumerate every docket page from the WordPress sitemap
     (psc.utah.gov/wp-sitemap-posts-post-{1,2,3}.xml). Each docket is a
     post at /YYYY/MM/DD/docket-no-{NN-NNN-NN}/ (~4,700 dockets). The
     docket number and post date are parsed from the URL.
  2. GET each docket page. It lists every filing in the docket as an
     anchor whose text is the document description and whose href is a
     born-digital PDF on pscdocs.utah.gov. Keep the anchors whose text
     denotes a Commission Order (matches \\border\\b -- "Order Approving
     Interim Rates", "Scheduling Order and Notice of Hearings",
     "Order Granting Intervention...", etc.).
  3. Download each Order PDF and extract full text (fitz/PyMuPDF;
     Tesseract OCR fallback for the rare image-only scan).

VANTAGE NOTE: the pscdocs.utah.gov S3 bucket serves objects publicly to
residential clients but returns HTTP 403 to cloud/datacenter IP ranges
(verified from this build vantage AND the WebFetch egress). To stay
vantage-independent, _download() tries the live pscdocs URL first and, on
failure, falls back to the Internet Archive Wayback Machine (which has
~20k pscdocs PDFs captured and is reachable from any vantage). From a
residential / proxied vantage the live path retrieves the full corpus.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.UT-PSC")

SITE = "https://psc.utah.gov"
SITEMAPS = [
    SITE + "/wp-sitemap-posts-post-3.xml",  # newest first
    SITE + "/wp-sitemap-posts-post-2.xml",
    SITE + "/wp-sitemap-posts-post-1.xml",
]
WAYBACK_AVAIL = "https://archive.org/wayback/available?url="

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# href -> anchor-text pairs for pscdocs PDFs on a docket page
ANCHOR_RE = re.compile(
    r'<a[^>]*href="(https?://pscdocs\.utah\.gov/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
TAG_RE = re.compile(r"<[^>]+>")
LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
DOCKET_RE = re.compile(r"/docket-no-([0-9a-z-]+)/?", re.I)
POSTDATE_RE = re.compile(r"psc\.utah\.gov/(\d{4})/(\d{2})/(\d{2})/")
FNAME_DATE_RE = re.compile(r"(\d{1,2})-(\d{1,2})-(\d{4})\.pdf$", re.I)
# whole-word "order" (order/orders/ordered), not "recorder"
ORDER_WORD_RE = re.compile(r"\border(?:s|ed)?\b", re.I)
# party filings that merely reference "...Order" in their title but are NOT
# Commission Orders (e.g. "Motion to Amend the Scheduling Order").
NON_ORDER_PREFIX_RE = re.compile(
    r"^\s*(?:motion|petition|comments?|testimony|application|request|response|"
    r"reply|letter|brief|direct|rebuttal|surrebuttal|stipulation|settlement|"
    r"exhibit|transcript|protest|objection|intervention|errata|memorandum|"
    r"prehearing|data\s+request|affidavit|certificate|proposed)\b",
    re.I,
)


def is_order_title(txt: str) -> bool:
    """True if the anchor text denotes a Commission Order, not a party filing."""
    if not ORDER_WORD_RE.search(txt):
        return False
    if NON_ORDER_PREFIX_RE.match(txt):
        return False
    return True
INDUSTRY_RE = re.compile(r"pscdocs\.utah\.gov/([a-z]+)/", re.I)

import html as _html


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def anchor_text(raw: str) -> str:
    return _html.unescape(TAG_RE.sub("", raw)).strip()


def date_from_filename(url: str) -> Optional[str]:
    m = FNAME_DATE_RE.search(url)
    if not m:
        return None
    mo, da, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(yr, mo, da).strftime("%Y-%m-%d")
    except ValueError:
        return None


def date_from_posturl(url: str) -> Optional[str]:
    m = POSTDATE_RE.search(url)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


class UTPSCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- enumeration --------------------------------------------------------

    def _docket_urls(self) -> List[str]:
        """All docket-page URLs from the WordPress sitemaps (newest first)."""
        urls: List[str] = []
        seen = set()
        for sm in SITEMAPS:
            r = None
            for attempt in range(4):
                try:
                    time.sleep(self.delay)
                    r = self.http.get(sm)
                    if r.status_code == 200:
                        break
                    logger.warning(f"sitemap {sm}: HTTP {r.status_code} (attempt {attempt+1})")
                except Exception as e:
                    logger.warning(f"sitemap {sm} error: {e}")
                time.sleep(2.0 * (attempt + 1))
            if r is None or r.status_code != 200:
                continue
            try:
                locs = [u for u in LOC_RE.findall(r.text) if "docket-no" in u]
                # within a sitemap, newest posts tend to be last -> reverse
                for u in reversed(locs):
                    if u not in seen:
                        seen.add(u)
                        urls.append(u)
            except Exception as e:
                logger.warning(f"sitemap {sm} error: {e}")
        logger.info(f"Discovered {len(urls)} docket pages")
        return urls

    def _order_links(self, docket_url: str) -> List[Tuple[str, str]]:
        """Return (pdf_url, title) for each Order-type filing on a docket page."""
        try:
            time.sleep(self.delay)
            r = self.http.get(docket_url)
            if r.status_code != 200:
                logger.debug(f"docket {docket_url}: HTTP {r.status_code}")
                return []
        except Exception as e:
            logger.debug(f"docket {docket_url} error: {e}")
            return []
        out: List[Tuple[str, str]] = []
        seen = set()
        for href, raw in ANCHOR_RE.findall(r.text):
            txt = anchor_text(raw)
            if not txt or not is_order_title(txt):
                continue
            href = _html.unescape(href)
            if href in seen:
                continue
            seen.add(href)
            out.append((href, txt))
        return out

    # ---- download / extract -------------------------------------------------

    def _get_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(self.delay)
            r = self.http.get(url)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                return r.content
        except Exception as e:
            logger.debug(f"GET {url} error: {e}")
        return None

    def _wayback_pdf(self, url: str) -> Optional[bytes]:
        """Fetch the closest Wayback capture of a pscdocs PDF (raw bytes)."""
        try:
            time.sleep(self.delay)
            r = self.http.get(WAYBACK_AVAIL + url)
            if r.status_code != 200:
                return None
            snap = (r.json().get("archived_snapshots") or {}).get("closest")
            if not snap or not snap.get("available"):
                return None
            ts = snap["timestamp"]
            raw = f"https://web.archive.org/web/{ts}id_/{url}"
            time.sleep(self.delay)
            rr = self.http.get(raw)
            if rr.status_code == 200 and rr.content[:4] == b"%PDF":
                return rr.content
        except Exception as e:
            logger.debug(f"wayback {url} error: {e}")
        return None

    def _download(self, url: str) -> Optional[bytes]:
        """Live pscdocs first; Wayback fallback (datacenter-IP-block safe)."""
        return self._get_pdf(url) or self._wayback_pdf(url)

    @staticmethod
    def _extract_text(content: bytes) -> str:
        try:
            doc = fitz.open(stream=content, filetype="pdf")
        except Exception as e:
            logger.warning(f"fitz open failed: {e}")
            return ""
        text = "".join(page.get_text() for page in doc)
        if len(text.strip()) < 100:
            try:
                import pytesseract
                from PIL import Image
                ocr = []
                for page in doc:
                    pix = page.get_pixmap(dpi=200)
                    img = Image.open(io.BytesIO(pix.tobytes("png")))
                    ocr.append(pytesseract.image_to_string(img))
                text = "\n".join(ocr)
            except Exception as e:
                logger.debug(f"OCR unavailable/failed: {e}")
        doc.close()
        return clean_text(text)

    # ---- BaseScraper API ----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        seen_ids = set()
        for docket_url in self._docket_urls():
            docket_no = None
            m = DOCKET_RE.search(docket_url)
            if m:
                docket_no = m.group(1)
            post_date = date_from_posturl(docket_url)
            for pdf_url, title in self._order_links(docket_url):
                doc_id = pdf_url.rsplit("/", 1)[-1][:-4]  # filename stem
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)
                content = self._download(pdf_url)
                if not content:
                    continue
                text = self._extract_text(content)
                if len(text.strip()) < 100:
                    logger.debug(f"Insufficient text for {pdf_url}, skipping")
                    continue
                ind_m = INDUSTRY_RE.search(pdf_url)
                yield {
                    "doc_id": doc_id,
                    "title": title,
                    "docket_number": docket_no,
                    "industry": ind_m.group(1) if ind_m else None,
                    "date": date_from_filename(pdf_url) or post_date,
                    "pdf_url": pdf_url,
                    "docket_url": docket_url,
                    "text": text,
                }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text", "")
        if not text or len(text.strip()) < 100:
            return None
        title = raw.get("title") or "Order"
        docket = raw.get("docket_number")
        if docket:
            title = f"{title} (Docket No. {docket})"
        return {
            "_id": raw["doc_id"],
            "_source": "US/UT-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("pdf_url"),
            "doc_id": raw["doc_id"],
            "docket_number": docket,
            "industry": raw.get("industry"),
            "docket_url": raw.get("docket_url"),
            "jurisdiction": "US-UT",
        }

    # ---- diagnostics --------------------------------------------------------

    def test_api(self) -> bool:
        urls = self._docket_urls()
        if not urls:
            print("FAIL: no docket pages from sitemap")
            return False
        print(f"OK: {len(urls)} docket pages discovered")
        found = 0
        for du in urls[:40]:
            links = self._order_links(du)
            if links:
                print(f"  {du}")
                for pdf_url, title in links[:3]:
                    print(f"    [Order] {title[:60]!r} -> {pdf_url.rsplit('/',1)[-1]}")
                found += len(links)
            if found >= 5:
                break
        print(f"OK: found {found} Order links in first dockets")
        return found > 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/UT-PSC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UTPSCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
