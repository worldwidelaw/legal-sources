#!/usr/bin/env python3
"""
LB/BDL — Banque du Liban (Central Bank of Lebanon)

Fetches Basic Circulars and Intermediate Circulars from the BDL website.

Strategy:
  1. Scrape paginated listing pages for both circular types
  2. Visit each detail page to find PDF download links
  3. Download PDFs and extract full text via pdfplumber
  4. Try English PDF first, fall back to Arabic

Data:
  - ~189 Basic Circulars + ~762 Intermediate Circulars
  - Language: Arabic and English (varies by circular)
  - License: Lebanese government publication

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin

import requests

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.bdl.gov.lb"
SOURCE_ID = "LB/BDL"
SAMPLE_DIR = Path(__file__).parent / "sample"
REQUEST_DELAY = 2.0
PER_PAGE = 15


# ── HTML parsers ──────────────────────────────────────────────────

class _ListingParser(HTMLParser):
    """Parse the BDL circulars listing table to extract metadata."""

    def __init__(self):
        super().__init__()
        self.rows: List[Dict[str, str]] = []
        self._in_table = False
        self._in_tbody = False
        self._in_row = False
        self._in_cell = False
        self._cell_idx = 0
        self._current_row: Dict[str, str] = {}
        self._current_text: List[str] = []
        self._current_href: Optional[str] = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self._in_table = True
        elif tag == "tbody":
            self._in_tbody = True
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._cell_idx = 0
            self._current_row = {}
        elif tag in ("td", "th") and self._in_row:
            self._in_cell = True
            self._current_text = []
            self._current_href = None
        elif tag == "a" and self._in_cell:
            href = attrs_dict.get("href", "")
            if "detail" in href or "download" in href:
                self._current_href = href

    def handle_endtag(self, tag):
        if tag == "table":
            self._in_table = False
            self._in_tbody = False
        elif tag == "tbody":
            self._in_tbody = False
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._in_tbody and self._current_row.get("date"):
                self.rows.append(self._current_row)
        elif tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            text = "".join(self._current_text).strip()
            # Map columns: 0=date, 1=decision#, 2=circular#, 3=title, 4=view link
            if self._cell_idx == 0:
                self._current_row["date"] = text
            elif self._cell_idx == 1:
                self._current_row["decision_number"] = text
            elif self._cell_idx == 2:
                self._current_row["circular_number"] = text
            elif self._cell_idx == 3:
                self._current_row["title"] = text
            elif self._cell_idx == 4 and self._current_href:
                self._current_row["detail_href"] = self._current_href
            self._cell_idx += 1

    def handle_data(self, data):
        if self._in_cell:
            self._current_text.append(data)


class _DetailParser(HTMLParser):
    """Parse a BDL circular detail page to extract PDF download links."""

    def __init__(self):
        super().__init__()
        self.pdf_links: List[Tuple[str, str]] = []  # (language, url)
        self._in_link = False
        self._current_href = ""
        self._current_text: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            href = attrs_dict.get("href", "")
            if ".pdf" in href.lower() or "download" in href.lower():
                self._in_link = True
                self._current_href = href
                self._current_text = []

    def handle_endtag(self, tag):
        if tag == "a" and self._in_link:
            self._in_link = False
            text = "".join(self._current_text).strip().upper()
            lang = "AR"
            if "EN" in text or "ENGLISH" in text:
                lang = "EN"
            elif "FR" in text or "FRENCH" in text or "FRANÇAIS" in text:
                lang = "FR"
            if self._current_href:
                self.pdf_links.append((lang, self._current_href))

    def handle_data(self, data):
        if self._in_link:
            self._current_text.append(data)


# ── PDF extraction ────────────────────────────────────────────────

def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    if not HAS_PDF:
        return ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


# ── Fetching ──────────────────────────────────────────────────────

class _LegacyTLSAdapter(requests.adapters.HTTPAdapter):
    """Permit the legacy TLS/ciphers that www.bdl.gov.lb negotiates.

    Modern OpenSSL 3.x (on the fleet VPS) rejects the server's old
    cipher/renegotiation with ``SSLV3_ALERT_HANDSHAKE_FAILURE``. Lowering the
    security level to 1 and allowing legacy server connect lets the handshake
    complete (macOS LibreSSL already accepts it). Issue #1160.
    """

    def _ctx(self):
        import ssl
        from urllib3.util.ssl_ import create_urllib3_context
        ctx = create_urllib3_context()
        # Lower the OpenSSL 3.x security level so the VPS accepts the server's
        # legacy cipher/key. LibreSSL (local) has no @SECLEVEL concept and
        # raises — its default already accepts the server, so ignore.
        try:
            ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        except ssl.SSLError:
            pass
        ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
        return ctx

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ctx()
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ctx()
        return super().proxy_manager_for(*args, **kwargs)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    s.mount("https://", _LegacyTLSAdapter())
    return s


def fetch_listing_page(session: requests.Session, circ_type: str, page: int) -> Optional[str]:
    """Fetch a paginated listing page. circ_type is 'basic' or 'intermediate'."""
    if circ_type == "basic":
        url = f"{BASE_URL}/basiccirculars.php?page={page}&langid=EN"
    else:
        url = f"{BASE_URL}/intermediatecirculars.php?page={page}&langid=EN"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.warning("Listing page %s p%d returned %d", circ_type, page, resp.status_code)
    except Exception as e:
        logger.warning("Failed to fetch listing %s p%d: %s", circ_type, page, e)
    return None


def parse_listing(html: str) -> List[Dict[str, str]]:
    """Parse a listing page HTML into circular metadata."""
    parser = _ListingParser()
    parser.feed(html)
    return parser.rows


def get_total_pages(html: str) -> int:
    """Extract total page count from pagination HTML."""
    # Look for patterns like "of 189 results" or page links
    m = re.search(r"of\s+(\d+)\s+results?", html, re.IGNORECASE)
    if m:
        total = int(m.group(1))
        return (total + PER_PAGE - 1) // PER_PAGE
    # Count page links
    pages = re.findall(r'page=(\d+)', html)
    if pages:
        return max(int(p) for p in pages)
    return 1


def fetch_all_listings(session: requests.Session, circ_type: str,
                       max_items: int = 0) -> List[Dict[str, str]]:
    """Fetch all paginated listings for a circular type."""
    all_circulars = []
    page = 1

    html = fetch_listing_page(session, circ_type, page)
    if not html:
        return []

    total_pages = get_total_pages(html)
    circulars = parse_listing(html)
    for c in circulars:
        c["circular_type"] = circ_type
    all_circulars.extend(circulars)
    logger.info("%s circulars: page 1/%d, got %d items", circ_type, total_pages, len(circulars))

    if max_items and len(all_circulars) >= max_items:
        return all_circulars[:max_items]

    for page in range(2, total_pages + 1):
        time.sleep(REQUEST_DELAY)
        html = fetch_listing_page(session, circ_type, page)
        if not html:
            break
        circulars = parse_listing(html)
        if not circulars:
            break
        for c in circulars:
            c["circular_type"] = circ_type
        all_circulars.extend(circulars)
        logger.info("%s circulars: page %d/%d, got %d items (total %d)",
                    circ_type, page, total_pages, len(circulars), len(all_circulars))
        if max_items and len(all_circulars) >= max_items:
            return all_circulars[:max_items]

    return all_circulars


def fetch_detail_page(session: requests.Session, circ: Dict[str, str]) -> List[Tuple[str, str]]:
    """Fetch the detail page and extract PDF download links."""
    href = circ.get("detail_href", "")
    if not href:
        return []
    url = urljoin(BASE_URL + "/", href)
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return []
        parser = _DetailParser()
        parser.feed(resp.text)
        return parser.pdf_links
    except Exception as e:
        logger.warning("Detail page fetch failed for %s: %s", href, e)
        return []


def try_constructed_pdf(session: requests.Session, circ: Dict[str, str]) -> Optional[Tuple[str, bytes]]:
    """Try to construct PDF URL from known pattern and download it."""
    decision = circ.get("decision_number", "")
    detail_href = circ.get("detail_href", "")
    circ_type = circ.get("circular_type", "basic")

    # Extract detail ID from href like basiccircular_detail.php?id=4990&langid=EN
    detail_id = ""
    m = re.search(r'id=(\d+)', detail_href)
    if m:
        detail_id = m.group(1)

    if not decision or not detail_id:
        return None

    type_dir = "Basic Circulars" if circ_type == "basic" else "Intermediate Circulars"

    # Try English first, then Arabic
    for lang_code, lang_num in [("EN", "3"), ("AR", "1"), ("FR", "2")]:
        pdf_url = (f"{BASE_URL}/CB%20Com/Laws%20And%20Regulations/{type_dir}/"
                   f"Decision_{decision}_{lang_code}%C2%A7{detail_id}_{lang_num}.pdf")
        try:
            resp = session.get(pdf_url, timeout=60, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 500:
                content_type = resp.headers.get("content-type", "")
                if "pdf" in content_type.lower() or resp.content[:5] == b"%PDF-":
                    return (lang_code, resp.content)
        except Exception:
            pass

    return None


def download_pdf(session: requests.Session, circ: Dict[str, str]) -> Optional[Tuple[str, bytes]]:
    """Download the PDF for a circular. Returns (language, bytes) or None."""
    # Strategy 1: Try constructed URL pattern (faster, avoids detail page fetch)
    result = try_constructed_pdf(session, circ)
    if result:
        return result

    # Strategy 2: Fetch detail page and find PDF links
    time.sleep(REQUEST_DELAY)
    pdf_links = fetch_detail_page(session, circ)
    if not pdf_links:
        return None

    # Prefer English, then French, then Arabic
    pdf_links_sorted = sorted(pdf_links, key=lambda x: {"EN": 0, "FR": 1, "AR": 2}.get(x[0], 3))

    for lang, href in pdf_links_sorted:
        pdf_url = urljoin(BASE_URL + "/", href)
        try:
            resp = session.get(pdf_url, timeout=60)
            if resp.status_code == 200 and len(resp.content) > 500:
                if resp.content[:5] == b"%PDF-" or "pdf" in resp.headers.get("content-type", "").lower():
                    return (lang, resp.content)
        except Exception as e:
            logger.warning("PDF download failed %s: %s", pdf_url, e)
        time.sleep(REQUEST_DELAY)

    return None


# ── Normalize ─────────────────────────────────────────────────────

def normalize(circ: Dict[str, str], pdf_text: str, lang: str) -> Dict[str, Any]:
    """Normalize a circular record into standard schema."""
    circ_type = circ.get("circular_type", "basic")
    circ_num = circ.get("circular_number", "unknown")
    decision_num = circ.get("decision_number", "")
    date_str = circ.get("date", "")
    title = circ.get("title", "")

    # Parse date: DD/MM/YYYY → ISO 8601
    iso_date = None
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            iso_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    prefix = "BC" if circ_type == "basic" else "IC"
    doc_id = f"BDL-{prefix}-{circ_num}"
    type_label = "Basic" if circ_type == "basic" else "Intermediate"

    detail_href = circ.get("detail_href", "")
    url = urljoin(BASE_URL + "/", detail_href) if detail_href else BASE_URL

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"BDL {type_label} Circular No. {circ_num}: {title}",
        "text": pdf_text,
        "date": iso_date,
        "url": url,
        "circular_number": circ_num,
        "decision_number": decision_num,
        "circular_type": circ_type,
        "language": lang,
    }


# ── Bootstrap ─────────────────────────────────────────────────────

def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Fetch all BDL circulars with full text."""
    if not HAS_PDF:
        logger.error("pdfplumber not available — cannot extract PDF text")
        sys.exit(1)

    session = _session()
    all_circulars = []

    if sample:
        # In sample mode, fetch first page of each type
        for circ_type in ["basic", "intermediate"]:
            circs = fetch_all_listings(session, circ_type, max_items=10)
            all_circulars.extend(circs)
            time.sleep(REQUEST_DELAY)
        all_circulars = all_circulars[:20]
    else:
        for circ_type in ["basic", "intermediate"]:
            circs = fetch_all_listings(session, circ_type)
            all_circulars.extend(circs)
            time.sleep(REQUEST_DELAY)

    logger.info("Total circulars to process: %d", len(all_circulars))

    success = 0
    errors = 0
    limit = 20 if sample else len(all_circulars)

    for i, circ in enumerate(all_circulars[:limit]):
        circ_num = circ.get("circular_number", "?")
        circ_type = circ.get("circular_type", "?")
        logger.info("[%d/%d] Fetching %s circular %s: %s",
                    i + 1, limit, circ_type, circ_num,
                    circ.get("title", "")[:60])

        result = download_pdf(session, circ)
        if not result:
            logger.warning("No PDF found for %s circular %s", circ_type, circ_num)
            errors += 1
            time.sleep(REQUEST_DELAY)
            continue

        lang, pdf_bytes = result
        pdf_text = extract_pdf_text(pdf_bytes)

        if not pdf_text or len(pdf_text) < 50:
            logger.warning("Insufficient text from %s circular %s (%d chars)",
                           circ_type, circ_num, len(pdf_text) if pdf_text else 0)
            errors += 1
            time.sleep(REQUEST_DELAY)
            continue

        record = normalize(circ, pdf_text, lang)
        success += 1
        yield record
        time.sleep(REQUEST_DELAY)

    logger.info("Done: %d success, %d errors out of %d attempted",
                success, errors, min(limit, len(all_circulars)))


def main():
    parser = argparse.ArgumentParser(description="LB/BDL bootstrap")
    sub = parser.add_subparsers(dest="command")
    # bootstrap-fast is the VPS pipeline alias for a full streaming run
    boot = sub.add_parser("bootstrap", help="Fetch circulars")
    boot.add_argument("--sample", action="store_true", help="Fetch ~20 sample records")
    boot.add_argument("--full", action="store_true", help="Full run (default; accepted for pipeline compat)")
    fast = sub.add_parser("bootstrap-fast", help="Fetch circulars (VPS pipeline alias)")
    fast.add_argument("--sample", action="store_true", help="Fetch ~20 sample records")
    fast.add_argument("--full", action="store_true", help="Full run (default; accepted for pipeline compat)")
    args = parser.parse_args()

    if args.command not in ("bootstrap", "bootstrap-fast"):
        parser.print_help()
        sys.exit(1)

    if args.sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=True):
            out = SAMPLE_DIR / f"{record['_id']}.json"
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            logger.info("Saved %s (%d chars text)", record["_id"], len(record.get("text", "")))
        logger.info("Total sample records saved: %d", count)
        if count == 0:
            logger.error("No records fetched — check connectivity and PDF access")
            sys.exit(1)
        return

    # Full run: stream every record to data/records.jsonl for the VPS ingest pipeline
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = data_dir / "records.jsonl"
    count = 0
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for record in fetch_all(sample=False):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 25 == 0:
                logger.info("Progress: %d records written", count)
    logger.info("Full bootstrap complete: %d records -> %s", count, jsonl_path)
    if count == 0:
        logger.error("No records fetched — check connectivity and PDF access")
        sys.exit(1)


if __name__ == "__main__":
    main()
