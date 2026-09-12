#!/usr/bin/env python3
"""
Bangladesh Securities & Exchange Commission (BSEC) Data Fetcher

Fetches securities laws, rules, regulations, directives, orders, and circulars
from sec.gov.bd. Documents are available as PDFs at /slaws/*.pdf.

The site provides a search endpoint at /home/laws that lists all documents.
We scrape the listing page to find PDF links, then download and extract text.
"""

import hashlib
import html as html_mod
import io
import json
import logging
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "BD/BSEC"
BASE_URL = "https://sec.gov.bd"
LAWS_URL = f"{BASE_URL}/home/laws"
DELAY = 1.5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
SAMPLE_DIR = Path(__file__).parent / "sample"


def http_get(url: str, timeout: int = 30) -> Optional[bytes]:
    """Fetch URL and return raw bytes."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        logger.warning(f"HTTP GET failed for {url[:100]}: {e}")
        return None


def http_get_text(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL and return text."""
    raw = http_get(url, timeout)
    if raw is None:
        return None
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def strip_html(text: str) -> str:
    """Remove HTML tags and clean whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|tr|li|h[1-6])>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using pdfminer if available, else basic extraction."""
    try:
        from pdfminer.high_level import extract_text
        return extract_text(io.BytesIO(pdf_bytes)).strip()
    except ImportError:
        pass
    # Fallback: basic text extraction from PDF streams
    text_parts = []
    content = pdf_bytes.decode("latin-1", errors="replace")
    for match in re.finditer(r"\(([^)]{2,})\)", content):
        candidate = match.group(1)
        if len(candidate) > 5 and any(c.isalpha() for c in candidate):
            text_parts.append(candidate)
    return "\n".join(text_parts).strip()


def parse_listing_page(html_text: str) -> List[Dict[str, str]]:
    """Parse the laws listing page to extract document entries."""
    entries = []
    # Find PDF links with their context
    # Pattern: <a href="...pdf">...</a> with surrounding date/title context
    pdf_pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL
    )
    for match in pdf_pattern.finditer(html_text):
        url = match.group(1).strip()
        link_text = strip_html(match.group(2)).strip()
        if not url.startswith("http"):
            url = BASE_URL + ("" if url.startswith("/") else "/") + url

        # Try to extract date from surrounding context
        context_start = max(0, match.start() - 500)
        context = html_text[context_start:match.end() + 200]
        date = extract_date(context)

        # Determine category from URL or link text
        category = classify_document(url, link_text)

        if link_text and len(link_text) > 3:
            entries.append({
                "title": link_text,
                "url": url,
                "date": date,
                "category": category,
            })

    # Deduplicate by URL
    seen = set()
    unique = []
    for e in entries:
        if e["url"] not in seen:
            seen.add(e["url"])
            unique.append(e)
    return unique


def extract_date(context: str) -> Optional[str]:
    """Extract a date from surrounding HTML context."""
    # Try various date patterns
    patterns = [
        r"(\d{1,2})[./\-](\d{1,2})[./\-](20\d{2})",  # DD/MM/YYYY
        r"(20\d{2})[./\-](\d{1,2})[./\-](\d{1,2})",   # YYYY-MM-DD
        r"(\w+)\s+(\d{1,2}),?\s+(20\d{2})",             # Month DD, YYYY
        r"(\d{1,2})\s+(\w+)\s+(20\d{2})",               # DD Month YYYY
    ]
    text = strip_html(context)
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            groups = m.groups()
            try:
                if re.match(r"20\d{2}", groups[0]):
                    return f"{groups[0]}-{int(groups[1]):02d}-{int(groups[2]):02d}"
                elif groups[0].isdigit() and groups[1].isdigit():
                    day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                    if 1 <= month <= 12 and 1 <= day <= 31:
                        return f"{year}-{month:02d}-{day:02d}"
                else:
                    # Month name
                    month_names = {
                        "january": 1, "february": 2, "march": 3, "april": 4,
                        "may": 5, "june": 6, "july": 7, "august": 8,
                        "september": 9, "october": 10, "november": 11, "december": 12,
                        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
                        "jun": 6, "jul": 7, "aug": 8, "sep": 9,
                        "oct": 10, "nov": 11, "dec": 12,
                    }
                    for g in groups:
                        if g.lower() in month_names:
                            month = month_names[g.lower()]
                            day_g = [x for x in groups if x.isdigit() and int(x) <= 31]
                            year_g = [x for x in groups if re.match(r"20\d{2}$", x)]
                            if day_g and year_g:
                                return f"{year_g[0]}-{month:02d}-{int(day_g[0]):02d}"
            except (ValueError, IndexError):
                continue
    # Try extracting from filename
    return None


def classify_document(url: str, title: str) -> str:
    """Classify document type from URL and title."""
    combined = (url + " " + title).lower()
    if "rule" in combined:
        return "Rules"
    elif "regulation" in combined or "regulat" in combined:
        return "Regulations"
    elif "directive" in combined:
        return "Directive"
    elif "circular" in combined:
        return "Circular"
    elif "notification" in combined:
        return "Notification"
    elif "order" in combined:
        return "Order"
    elif "guideline" in combined or "code" in combined:
        return "Guidelines"
    elif "amendment" in combined:
        return "Amendment"
    return "Other"


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a raw document into standard schema."""
    doc_id = hashlib.sha256(raw["url"].encode()).hexdigest()[:16]
    return {
        "_id": f"bsec-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "category": raw.get("category", ""),
        "country": "BD",
        "issuing_body": "Bangladesh Securities and Exchange Commission",
    }


def fetch_all() -> Iterator[Dict[str, Any]]:
    """Fetch all BSEC documents."""
    logger.info("Fetching BSEC laws listing page...")
    html = http_get_text(LAWS_URL)
    if not html:
        logger.error("Failed to fetch listing page")
        return

    entries = parse_listing_page(html)
    logger.info(f"Found {len(entries)} document entries")

    for i, entry in enumerate(entries):
        logger.info(f"[{i+1}/{len(entries)}] Fetching: {entry['title'][:60]}...")
        time.sleep(DELAY)

        pdf_bytes = http_get(entry["url"], timeout=60)
        if not pdf_bytes:
            logger.warning(f"  Failed to download: {entry['url']}")
            continue

        text = extract_pdf_text(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"  Insufficient text extracted ({len(text) if text else 0} chars)")
            continue

        entry["text"] = text
        yield normalize(entry)


def fetch_updates(since: str) -> Iterator[Dict[str, Any]]:
    """Fetch documents updated since a date."""
    for doc in fetch_all():
        if doc.get("date") and doc["date"] >= since:
            yield doc


def bootstrap_sample(max_records: int = 15) -> None:
    """Download sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetch_all():
        if count >= max_records:
            break
        text = record.get("text", "")
        if len(text) < 100:
            continue
        outpath = SAMPLE_DIR / f"{record['_id']}.json"
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"  Saved {outpath.name} ({len(text)} chars)")
        count += 1
    logger.info(f"Saved {count} sample records to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_flag = "--sample" in sys.argv
        if sample_flag:
            bootstrap_sample()
        else:
            for doc in fetch_all():
                print(json.dumps(doc, ensure_ascii=False))
    else:
        print(f"Usage: {sys.argv[0]} bootstrap [--sample]")
