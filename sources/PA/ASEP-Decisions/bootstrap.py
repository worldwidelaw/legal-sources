#!/usr/bin/env python3
"""
PA/ASEP-Decisions -- Panama ASEP (Autoridad Nacional de los Servicios Públicos)

Fetches regulatory resolutions issued by Panama's national public-services
regulator (telecommunications, electricity, water/sewage, radio & TV) from the
official WordPress site at https://asep.gob.pa via its public WP REST API
(/wp-json/wp/v2/). Each resolution is a WordPress post whose body embeds the
signed resolution PDF through a pdf-viewer iframe; the full text of the
resolution lives in that PDF.

The scraper:
  1. Paginates the WP REST API per resolution category.
  2. Extracts the embedded resolution PDF URL from each post body.
  3. Downloads the PDF and extracts its text layer with pdfplumber.
  4. Emits a normalized record only when real full text is recovered
     (some recent PDFs are signed scans with no text layer; those are skipped).

The post body itself carries the official "sumilla" (one-line summary, starting
"Por la cual ..."), preserved as the `summary` field.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import html
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

SOURCE_ID = "PA/ASEP-Decisions"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PA.ASEP-Decisions")

BASE_URL = "https://asep.gob.pa/wp-json/wp/v2"

# Resolution categories (WP category id -> human label).
# comision_sustanciadora and the sector "resoluciones" categories hold the
# regulator's binding resolutions. CS resolutions are reliably born-digital
# (real text layer); some recent sector PDFs are signed scans and get skipped.
CATEGORIES = {
    261: "Comisión Sustanciadora",
    278: "Resoluciones Electricidad",
    108: "Resoluciones Telecomunicaciones",
    111: "Resoluciones Radio y Televisión",
    110: "Resoluciones Agua y Alcantarillado",
    335: "Resoluciones Atención al Usuario",
}

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research project)",
    "Accept": "application/json",
}

PAGE_SIZE = 50
REQUEST_DELAY = 1.0  # seconds between requests
MIN_TEXT_CHARS = 400  # below this we treat the PDF as a scan with no text layer

PDF_IFRAME_RE = re.compile(r"viewer\.html\?file=([^\"'&]+\.pdf)", re.IGNORECASE)
HREF_PDF_RE = re.compile(r'href="([^"]+\.pdf)"', re.IGNORECASE)


def strip_html(raw: str) -> str:
    """Strip HTML tags and decode entities into clean plain text."""
    if not raw:
        return ""
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", raw)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def find_pdf_url(content_html: str) -> Optional[str]:
    """Locate the embedded resolution PDF URL inside a post body."""
    m = PDF_IFRAME_RE.search(content_html)
    if m:
        return html.unescape(m.group(1))
    # Fallback: first /resoluciones/ PDF linked directly.
    for href in HREF_PDF_RE.findall(content_html):
        if "/resoluciones/" in href.lower():
            return html.unescape(href)
    return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract the text layer from a resolution PDF."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            parts = []
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    parts.append(t.strip())
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(parts).strip()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"PDF parse failed: {e}")
        return ""


def get_posts(category_id: int, limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Paginate through all posts in a WP category."""
    page = 1
    count = 0
    while True:
        url = f"{BASE_URL}/posts"
        params = {
            "categories": category_id,
            "per_page": PAGE_SIZE,
            "page": page,
            "_fields": "id,date,link,title,content,categories",
        }
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
            if resp.status_code == 400:
                # Past the last page (WP returns 400 when page > total pages).
                break
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch posts (cat {category_id}, page {page}): {e}")
            break

        posts = resp.json()
        if not posts:
            break

        for post in posts:
            yield post
            count += 1
            if limit and count >= limit:
                return

        if len(posts) < PAGE_SIZE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)


def normalize(post: dict, category_name: str, text: str, pdf_url: str) -> dict:
    """Transform a WP post + extracted PDF text into the standard schema."""
    title = strip_html(post.get("title", {}).get("rendered", "")) or post.get("link", "")
    summary = strip_html(post.get("content", {}).get("rendered", ""))
    # Trim the summary to the descriptive lead (drop the "Resolución / Anexos" tail).
    for marker in ("\nResolución", "\nAnexos", "\nDescargar"):
        idx = summary.find(marker)
        if idx > 0:
            summary = summary[:idx].strip()

    date_iso = None
    raw_date = post.get("date")
    if raw_date:
        try:
            date_iso = datetime.fromisoformat(raw_date).date().isoformat()
        except ValueError:
            date_iso = raw_date[:10]

    return {
        "_id": f"PA-ASEP-{post.get('id')}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "summary": summary,
        "date": date_iso,
        "url": post.get("link", ""),
        "pdf_url": pdf_url,
        "category": category_name,
        "language": "spa",
        "publisher": "Autoridad Nacional de los Servicios Públicos (ASEP)",
        "country": "PA",
    }


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch resolutions across all categories, yielding only full-text records."""
    total = 0
    for cat_id, cat_name in CATEGORIES.items():
        logger.info(f"Fetching category: {cat_name} ({cat_id})")
        cat_count = 0
        for post in get_posts(cat_id):
            content_html = post.get("content", {}).get("rendered", "")
            pdf_url = find_pdf_url(content_html)
            if not pdf_url:
                continue
            try:
                resp = requests.get(pdf_url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=90)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download failed {pdf_url}: {e}")
                time.sleep(REQUEST_DELAY)
                continue

            text = extract_pdf_text(resp.content)
            time.sleep(REQUEST_DELAY)
            if len(text) < MIN_TEXT_CHARS:
                logger.debug(f"Skipping scan (no text layer): {pdf_url}")
                continue

            yield normalize(post, cat_name, text, pdf_url)
            cat_count += 1
            total += 1
            if limit and total >= limit:
                logger.info(f"Reached limit of {limit} records")
                return

        logger.info(f"  {cat_name}: {cat_count} full-text records")


def fetch_updates(since: str, limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch resolutions published after `since` (ISO date) across all categories."""
    total = 0
    for cat_id, cat_name in CATEGORIES.items():
        page = 1
        while True:
            params = {
                "categories": cat_id,
                "per_page": PAGE_SIZE,
                "page": page,
                "after": f"{since}T00:00:00",
                "_fields": "id,date,link,title,content,categories",
            }
            try:
                resp = requests.get(f"{BASE_URL}/posts", params=params, headers=HEADERS, timeout=60)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"fetch_updates failed (cat {cat_id}): {e}")
                break
            posts = resp.json()
            if not posts:
                break
            for post in posts:
                pdf_url = find_pdf_url(post.get("content", {}).get("rendered", ""))
                if not pdf_url:
                    continue
                try:
                    r = requests.get(pdf_url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=90)
                    r.raise_for_status()
                except requests.exceptions.RequestException:
                    continue
                text = extract_pdf_text(r.content)
                time.sleep(REQUEST_DELAY)
                if len(text) < MIN_TEXT_CHARS:
                    continue
                yield normalize(post, cat_name, text, pdf_url)
                total += 1
                if limit and total >= limit:
                    return
            if len(posts) < PAGE_SIZE:
                break
            page += 1
            time.sleep(REQUEST_DELAY)


def test_api():
    """Test API connectivity and report per-category post counts."""
    logger.info("Testing ASEP WordPress REST API...")
    try:
        for cat_id, cat_name in CATEGORIES.items():
            resp = requests.get(
                f"{BASE_URL}/posts",
                params={"categories": cat_id, "per_page": 1, "_fields": "id"},
                headers=HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"  {cat_name} ({cat_id}): {total} posts")

        # Sample one full-text resolution.
        logger.info("\nFetching one sample full-text resolution from Comisión Sustanciadora...")
        for rec in fetch_all(limit=1):
            logger.info(f"  {rec['title']}  ({len(rec['text']):,} chars)")
            logger.info(f"  PDF: {rec['pdf_url']}")
            logger.info(f"  Preview: {rec['text'][:200]}...")
    except Exception as e:  # noqa: BLE001
        logger.error(f"API test failed: {e}")
        sys.exit(1)


def bootstrap(sample: bool = False, full: bool = False):
    """Run bootstrap: fetch and save records."""
    limit = 15 if sample else None
    out_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    text_lengths = []

    for record in fetch_all(limit=limit):
        records.append(record)
        text_lengths.append(len(record.get("text", "")))
        safe_id = record["_id"].replace("/", "_").replace(" ", "_")
        with open(out_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {record['_id']} ({len(record.get('text', '')):,} chars)")

    if records:
        avg_text = sum(text_lengths) / len(text_lengths)
        logger.info(f"\n{'='*60}")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"Avg text length: {avg_text:,.0f} chars")
        logger.info(f"Min text length: {min(text_lengths):,} chars")
        logger.info(f"Max text length: {max(text_lengths):,} chars")
        logger.info(f"Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
        logger.info(f"Output directory: {out_dir}")
    else:
        logger.warning("No records fetched!")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="PA/ASEP-Decisions data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", help="For updates: ISO date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
