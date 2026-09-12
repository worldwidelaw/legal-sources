#!/usr/bin/env python3
"""
CM/PRC — Cameroon Official Acts (Presidency)

Fetches laws, decrees, ordinances, orders, decisions, and circulars from
the Presidency of the Republic of Cameroon at prc.cm.

Strategy:
  - Scrape paginated listing pages for each act category
  - For each act, follow detail page → document page → embedded PDF
  - Extract text from PDF using PyMuPDF (fitz) or pdfplumber fallback
  - Clean watermark artifacts

Source: https://www.prc.cm/en/news/the-acts
Rate limit: 2 sec between requests

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from common.pdf_extract import extract_pdf_markdown
except ImportError:
    extract_pdf_markdown = None

SOURCE_ID = "CM/PRC"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CM.PRC")

BASE_URL = "https://www.prc.cm"
LANG = "en"

CATEGORIES = [
    {"slug": "laws", "label": "Law", "data_type": "legislation"},
    {"slug": "ordinances", "label": "Ordinance", "data_type": "legislation"},
    {"slug": "decrees", "label": "Decree", "data_type": "legislation"},
    {"slug": "orders", "label": "Order", "data_type": "legislation"},
    {"slug": "decisions", "label": "Decision", "data_type": "legislation"},
    {"slug": "circulars", "label": "Circular", "data_type": "legislation"},
]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

RATE_LIMIT = 2.0
_last_request = 0.0


def _throttle():
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < RATE_LIMIT:
        time.sleep(RATE_LIMIT - elapsed)
    _last_request = time.time()


def _get(url: str, timeout: int = 30) -> Optional[requests.Response]:
    _throttle()
    try:
        resp = SESSION.get(url, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.warning(f"Request failed {url}: {e}")
        return None


def scrape_listing_page(category_slug: str, start: int = 0) -> list:
    """Scrape a single listing page, return list of (article_id, title, url) tuples."""
    url = f"{BASE_URL}/{LANG}/news/the-acts/{category_slug}"
    if start > 0:
        url += f"?start={start}"

    resp = _get(url)
    if not resp:
        return []

    html = resp.text
    pattern = re.compile(
        r'href="(/(?:en|fr)/news/the-acts/'
        + re.escape(category_slug)
        + r'/(\d+)-([^"]+))"[^>]*>\s*(.*?)\s*</a>',
        re.DOTALL | re.IGNORECASE,
    )

    seen = set()
    results = []
    for m in pattern.finditer(html):
        rel_url = m.group(1)
        article_id = m.group(2)
        title_html = m.group(4)
        title = re.sub(r"<[^>]+>", "", title_html).strip()

        if article_id in seen or not title or len(title) < 5:
            continue
        seen.add(article_id)

        full_url = urljoin(BASE_URL, rel_url)
        results.append((article_id, title, full_url))

    return results


def list_all_articles(category_slug: str, max_pages: int = 200) -> list:
    """Paginate through all listing pages for a category."""
    all_items = []
    seen_ids = set()

    for page_num in range(max_pages):
        start = page_num * 8
        items = scrape_listing_page(category_slug, start)
        if not items:
            break

        new_count = 0
        for article_id, title, url in items:
            if article_id not in seen_ids:
                seen_ids.add(article_id)
                all_items.append((article_id, title, url))
                new_count += 1

        if new_count == 0:
            break

        logger.info(f"  {category_slug} page {page_num + 1}: {new_count} new items (total {len(all_items)})")

    return all_items


def get_pdf_url_from_article(article_url: str) -> Optional[str]:
    """From an article detail page, find the document page, then extract the embedded PDF URL."""
    resp = _get(article_url)
    if not resp:
        return None

    html = resp.text
    # Find link to document page: /en/multimedia/documents/{id}-{slug}
    doc_match = re.search(
        r'href="(/(?:en|fr)/multimedia/documents/(\d+)-[^"]+)"',
        html,
    )
    if not doc_match:
        # Try to find a direct PDF embed on the article page itself
        pdf_match = re.search(r'src="(/files/[^"]+\.pdf)"', html)
        if pdf_match:
            return urljoin(BASE_URL, pdf_match.group(1))
        return None

    doc_url = urljoin(BASE_URL, doc_match.group(1))

    # Fetch document page to find embedded PDF
    resp2 = _get(doc_url)
    if not resp2:
        return None

    pdf_match = re.search(r'src="(/files/[^"]+\.pdf)"', resp2.text)
    if pdf_match:
        return urljoin(BASE_URL, pdf_match.group(1))

    return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes. Uses PyMuPDF first, then pdfplumber fallback."""
    text = ""

    # Try PyMuPDF (fitz) first — best quality
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            t = page.get_text()
            if t:
                pages.append(t)
        doc.close()
        text = "\n\n".join(pages)
    except Exception:
        pass

    # Fallback to pdfplumber
    if not text.strip():
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pages.append(t)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                text = "\n\n".join(pages)
        except Exception:
            pass

    # Clean watermark (appears as "www.prc.cm" or split per-character "w\nw\nw\n.\np\nr\nc\n.\nc\nm")
    text = re.sub(r'\bwww\.prc\.cm\b', '', text)
    text = re.sub(r'(?:m\n?c\n?\.\n?c\n?r\n?p\n?\.\n?w\n?w\n?w)', '', text)
    text = re.sub(r'(?:w\n?w\n?w\n?\.\n?p\n?r\n?c\n?\.\n?c\n?m)', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text).strip()

    return text


def download_and_extract_pdf(pdf_url: str) -> str:
    """Download PDF and extract text."""
    if extract_pdf_markdown:
        try:
            md = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id="",
                pdf_url=pdf_url,
                table="legislation",
            )
            if md:
                return md
        except Exception as e:
            logger.debug(f"common.pdf_extract failed: {e}")

    _throttle()
    try:
        resp = SESSION.get(pdf_url, timeout=120)
        resp.raise_for_status()
        return extract_pdf_text(resp.content)
    except Exception as e:
        logger.warning(f"Failed to download PDF {pdf_url}: {e}")
        return ""


def parse_date_from_title(title: str) -> Optional[str]:
    """Extract date from title like 'Law No.2026/003 of 14 April 2026 ...'"""
    m = re.search(r'of\s+(\d{1,2})\s+(\w+)\s+(\d{4})', title, re.IGNORECASE)
    if m:
        day, month_str, year = m.group(1), m.group(2), m.group(3)
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    m = re.search(r'(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})', title)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        try:
            return f"{y}-{int(mo):02d}-{int(d):02d}"
        except ValueError:
            pass

    return None


def normalize(raw: dict) -> dict:
    """Normalize a raw record into standard schema."""
    return {
        "_id": f"CM-PRC-{raw['article_id']}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw["article_url"],
        "pdf_url": raw.get("pdf_url", ""),
        "act_type": raw.get("act_type", ""),
        "category": raw.get("category", ""),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents from all categories."""
    count = 0
    sample_limit = 15 if sample else 999999

    for cat in CATEGORIES:
        if count >= sample_limit:
            break

        slug = cat["slug"]
        label = cat["label"]
        logger.info(f"Scanning category: {slug}")

        articles = list_all_articles(slug, max_pages=3 if sample else 200)
        logger.info(f"  Found {len(articles)} articles in {slug}")

        for article_id, title, article_url in articles:
            if count >= sample_limit:
                break

            logger.info(f"  [{count + 1}] Processing {article_id}: {title[:60]}...")

            pdf_url = get_pdf_url_from_article(article_url)
            text = ""
            if pdf_url:
                text = download_and_extract_pdf(pdf_url)
                logger.info(f"    PDF text: {len(text)} chars")
            else:
                logger.warning(f"    No PDF found for {article_id}")

            date = parse_date_from_title(title)

            raw = {
                "article_id": article_id,
                "title": title,
                "text": text,
                "date": date,
                "article_url": article_url,
                "pdf_url": pdf_url or "",
                "act_type": label,
                "category": slug,
            }

            yield normalize(raw)
            count += 1


def test_api():
    """Test connectivity and structure."""
    print(f"Testing {SOURCE_ID}...")
    print(f"Base URL: {BASE_URL}")

    # Test listing page
    resp = _get(f"{BASE_URL}/{LANG}/news/the-acts/laws")
    if resp:
        print(f"[OK] Laws listing: HTTP {resp.status_code}, {len(resp.text)} bytes")
    else:
        print("[FAIL] Cannot reach laws listing page")
        return False

    # Test first article
    articles = scrape_listing_page("laws", 0)
    print(f"[OK] Found {len(articles)} articles on first page")

    if articles:
        aid, title, url = articles[0]
        print(f"  First: [{aid}] {title[:80]}")

        pdf_url = get_pdf_url_from_article(url)
        if pdf_url:
            print(f"[OK] PDF URL: {pdf_url}")

            _throttle()
            resp = SESSION.get(pdf_url, timeout=60)
            if resp.status_code == 200:
                text = extract_pdf_text(resp.content)
                clean = text.replace("www.prc.cm", "").strip()
                print(f"[OK] PDF downloaded: {len(resp.content)} bytes, text: {len(clean)} chars")
                if clean:
                    print(f"  Preview: {clean[:200]}...")
            else:
                print(f"[WARN] PDF download failed: HTTP {resp.status_code}")
        else:
            print("[WARN] No PDF URL found on article page")

    # Count categories
    for cat in CATEGORIES:
        items = scrape_listing_page(cat["slug"], 0)
        print(f"  {cat['slug']}: {len(items)} items on first page")
        time.sleep(1)

    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    for record in fetch_all(sample=sample):
        records.append(record)
        # Save each record as individual JSON
        fname = f"{record['_id'].replace('/', '_')}.json"
        fpath = SAMPLE_DIR / fname
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Summary
    total = len(records)
    with_text = sum(1 for r in records if r.get("text") and len(r["text"]) > 50)
    with_date = sum(1 for r in records if r.get("date"))

    print(f"\n{'='*60}")
    print(f"CM/PRC Bootstrap {'(SAMPLE)' if sample else ''} Complete")
    print(f"  Total records: {total}")
    print(f"  With text (>50 chars): {with_text}")
    print(f"  With date: {with_date}")
    print(f"  Saved to: {SAMPLE_DIR}")
    print(f"{'='*60}")

    return total, with_text


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description=f"Bootstrap {SOURCE_ID}")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Only fetch ~15 sample records")
    args = parser.parse_args()

    if args.command == "test-api":
        ok = test_api()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        total, with_text = bootstrap(sample=args.sample)
        if total == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        if with_text < min(10, total):
            logger.warning(f"Only {with_text}/{total} records have text content")
