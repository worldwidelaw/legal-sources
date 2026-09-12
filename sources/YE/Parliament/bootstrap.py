#!/usr/bin/env python3
"""
YE/Parliament — Yemen House of Representatives Legislative Encyclopedia

Fetches legislation from the Yemeni Parliament website (yemenparliament.gov.ye).

Strategy:
  - PWS.asmx/CategoryPosts (JSON POST, paginated) → HTML fragments with post links
  - Details?Post={id} (GET) → detail page with title, date, PDF attachment URL
  - Download PDF → extract full text via PyMuPDF

Data:
  - ~292 laws spanning 1990–2019 across 5 legislative periods
  - Full text in Arabic (PDF with selectable text)
  - License: Public Domain (Government Works)

Usage:
  python3 bootstrap.py bootstrap          # Full initial pull
  python3 bootstrap.py bootstrap --sample # Fetch 15 sample records
"""

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
from typing import Any, Dict, Iterator, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://yemenparliament.gov.ye"
AJAX_URL = BASE_URL + "/PWS.asmx/CategoryPosts"
DELAY = 1.5
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}
AJAX_HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Content-Type": "application/json; charset=utf-8",
    "X-Requested-With": "XMLHttpRequest",
}

# Legislative period categories
CATEGORIES = {
    21: "قوانين 1990-1993",
    42: "قوانين 1993-1997",
    43: "قوانين 1997-2003",
    44: "قوانين 2004-2009",
    168: "قوانين 2010-2019",
}

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
    logger.warning("PyMuPDF (fitz) not available — trying pypdf fallback")
    try:
        from pypdf import PdfReader
    except ImportError:
        PdfReader = None
        logger.warning("pypdf not available either — PDF extraction disabled")


def _get(url: str, timeout: int = 30) -> Optional[str]:
    """GET a URL and return response text."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"GET {url} failed: {e}")
        return None


def _ajax_post(cat_id: int, page: int) -> Optional[str]:
    """POST to the AJAX endpoint and return the HTML fragment."""
    data = json.dumps({"id": cat_id, "page": page}).encode("utf-8")
    req = urllib.request.Request(AJAX_URL, data=data, headers=AJAX_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("d", "")
    except Exception as e:
        logger.warning(f"AJAX CategoryPosts(id={cat_id}, page={page}) failed: {e}")
        return None


def _download_pdf(url: str) -> Optional[bytes]:
    """Download a PDF file and return its bytes."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except Exception as e:
        logger.warning(f"PDF download failed {url}: {e}")
        return None


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyMuPDF or pypdf."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            return text.strip()
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
            return ""
    elif PdfReader is not None:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            text = ""
            for page in reader.pages:
                text += (page.extract_text() or "")
            return text.strip()
        except Exception as e:
            logger.warning(f"pypdf extraction failed: {e}")
            return ""
    return ""


def list_posts_in_category(cat_id: int) -> List[str]:
    """Get all post IDs for a category by paginating through AJAX results."""
    all_posts = []
    page = 1
    while True:
        html = _ajax_post(cat_id, page)
        if not html:
            break
        posts = re.findall(r"Details\?Post=(\d+)", html)
        unique = list(dict.fromkeys(posts))
        if not unique:
            break
        all_posts.extend(unique)
        # Check for next page
        page_nums = re.findall(r"categoryPost\(\d+,(\d+)\)", html)
        max_page = max(int(p) for p in page_nums) if page_nums else page
        if page >= max_page:
            break
        page += 1
        time.sleep(DELAY)
    return all_posts


def list_all_posts() -> List[Tuple[str, str]]:
    """Get all (post_id, category_name) pairs across all categories."""
    all_items = []
    for cat_id, cat_name in CATEGORIES.items():
        logger.info(f"Listing category {cat_id}: {cat_name}")
        posts = list_posts_in_category(cat_id)
        for pid in posts:
            all_items.append((pid, cat_name))
        logger.info(f"  Found {len(posts)} laws")
        time.sleep(DELAY)
    return all_items


def fetch_detail(post_id: str) -> Optional[Dict[str, Any]]:
    """Fetch detail page for a post and extract metadata + PDF URL."""
    url = f"{BASE_URL}/Details?Post={post_id}"
    html = _get(url)
    if not html:
        return None

    # Extract title from first <h4>
    title_match = re.search(r"<h4[^>]*>([^<]+)</h4>", html)
    title = title_match.group(1).strip() if title_match else ""

    # Extract Gregorian date
    date_match = re.search(r"الموافق\s*(\d{4}/\d{1,2}/\d{1,2})", html)
    date_str = date_match.group(1) if date_match else None

    # Extract PDF URL
    pdf_match = re.search(r"(uploads/posts/documents/[^\"'>\s]+\.pdf)", html, re.IGNORECASE)
    pdf_path = pdf_match.group(1) if pdf_match else None
    pdf_url = f"{BASE_URL}/{pdf_path}" if pdf_path else None

    return {
        "post_id": post_id,
        "title": title,
        "date_str": date_str,
        "pdf_url": pdf_url,
        "detail_url": url,
    }


def normalize(raw: Dict[str, Any], text: str, category: str) -> Dict[str, Any]:
    """Normalize a raw record into the standard schema."""
    date_iso = None
    if raw.get("date_str"):
        try:
            dt = datetime.strptime(raw["date_str"], "%Y/%m/%d")
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return {
        "_id": f"YE-Parliament-{raw['post_id']}",
        "_source": "YE/Parliament",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": text,
        "date": date_iso,
        "url": raw.get("detail_url", ""),
        "pdf_url": raw.get("pdf_url", ""),
        "category": category,
        "post_id": raw["post_id"],
    }


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield all normalized records with full text."""
    items = list_all_posts()
    logger.info(f"Total posts to fetch: {len(items)}")

    if sample:
        items = items[:15]
        logger.info("Sample mode: limiting to 15 records")

    for i, (post_id, category) in enumerate(items):
        logger.info(f"[{i+1}/{len(items)}] Fetching Post={post_id}")
        detail = fetch_detail(post_id)
        if not detail:
            logger.warning(f"  Could not fetch detail for Post={post_id}")
            continue

        text = ""
        if detail.get("pdf_url"):
            pdf_bytes = _download_pdf(detail["pdf_url"])
            if pdf_bytes:
                text = _extract_text_from_pdf(pdf_bytes)
                logger.info(f"  Extracted {len(text)} chars from PDF")
            else:
                logger.warning(f"  PDF download failed for Post={post_id}")
        else:
            logger.warning(f"  No PDF URL found for Post={post_id}")

        if not text:
            logger.warning(f"  No text extracted for Post={post_id}, skipping")
            continue

        record = normalize(detail, text, category)
        yield record
        time.sleep(DELAY)


def bootstrap(sample: bool = False):
    """Run bootstrap: fetch records and save to sample/."""
    src_dir = Path(__file__).parent
    sample_dir = src_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    all_records = []
    for record in fetch_all(sample=sample):
        count += 1
        fname = sample_dir / f"record_{count:04d}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        all_records.append(record)
        logger.info(f"  Saved {fname.name}: {record['title'][:60]}")

    # Save combined file
    if all_records:
        combined = sample_dir / "all_samples.json"
        with open(combined, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    # Validation summary
    texts = [r.get("text", "") for r in all_records]
    non_empty = sum(1 for t in texts if len(t) > 100)
    logger.info(f"Validation: {non_empty}/{count} records have substantial text (>100 chars)")

    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    args = sys.argv[1:]
    if not args or args[0] == "bootstrap":
        sample = "--sample" in args
        bootstrap(sample=sample)
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")
        sys.exit(1)
