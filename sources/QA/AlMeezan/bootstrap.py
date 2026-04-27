#!/usr/bin/env python3
"""
QA/AlMeezan — Qatar Al Meezan Legislation Fetcher

Fetches Qatari legislation from the official Al Meezan legal portal
(almeezan.qa), operated by the Ministry of Justice.

Strategy:
  - Enumerate law IDs from 2284 to ~6500
  - Fetch metadata from LawPage.aspx?id=X&language=ar
  - Fetch section tree to discover article groups
  - Fetch full article text from LawArticles.aspx?LawTreeSectionID=X
  - Also fetch English translation if available

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "QA/AlMeezan"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.QA.AlMeezan")

BASE_URL = "https://www.almeezan.qa"
MIN_ID = 2284
MAX_ID = 6500

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ar,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.verify = False  # SSL cert issues with almeezan.qa

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_meta(html: str, field_id: str) -> str:
    """Extract text from a span by its ASP.NET ID."""
    pattern = rf'id="{re.escape(field_id)}"[^>]*>(.*?)</(?:span|h3|div)'
    m = re.search(pattern, html, re.DOTALL)
    if m:
        return clean_html(m.group(1)).strip()
    return ""


def fetch_law_metadata(law_id: int, language: str = "ar") -> Optional[dict]:
    """Fetch law metadata from the LawPage."""
    url = f"{BASE_URL}/LawPage.aspx?id={law_id}&language={language}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch metadata for ID {law_id}: {e}")
        return None

    html = resp.text
    prefix = "ContentPlaceHolder1_"

    # Check if this is a valid law page (title contains "Legislations" or Arabic equiv)
    title_match = re.search(r'<title>[^|]*\|[^|]*\|\s*([^<]+)', html)
    if not title_match:
        return None

    title = unescape(title_match.group(1).strip())
    if not title or len(title) < 3:
        return None

    law_type = extract_meta(html, f"{prefix}lblcardtype")
    number = extract_meta(html, f"{prefix}lblNumber")
    date_str = extract_meta(html, f"{prefix}lbldate")
    article_count = extract_meta(html, f"{prefix}lblArticlesNumber")
    status = extract_meta(html, f"{prefix}lblstatus")

    # Clean label prefixes
    for label in ["Type:", "النوع:", "Type :"]:
        law_type = law_type.replace(label, "").strip()
    for label in ["Number:", "الرقم:", "Number :"]:
        number = number.replace(label, "").strip()
    for label in ["Date:", "التاريخ:", "Date :"]:
        date_str = date_str.replace(label, "").strip()
    for label in ["Number of Articles:", "عدد المواد:", "Number of Articles :"]:
        article_count = article_count.replace(label, "").strip()
    for label in ["Status:", "الحالة:", "Status :"]:
        status = status.replace(label, "").strip()

    # Parse date - format is typically "DD/MM/YYYY Corresponding to ..."
    iso_date = None
    if date_str:
        date_part = date_str.split("Corresponding")[0].split("الموافق")[0].strip()
        for fmt in ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"]:
            try:
                iso_date = datetime.strptime(date_part, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Extract section tree IDs
    sections = re.findall(
        r"LawArticles\.aspx\?LawTreeSectionID=(\d+)&(?:amp;)?lawId=(\d+)",
        html,
    )
    section_ids = list(dict.fromkeys(s[0] for s in sections))  # deduplicate, preserve order

    return {
        "law_id": law_id,
        "title": title,
        "law_type": law_type,
        "number": number,
        "date": iso_date or date_str,
        "article_count": article_count,
        "status": status,
        "section_ids": section_ids,
        "language": language,
    }


def fetch_section_text(law_id: int, section_id: str, language: str = "ar") -> list:
    """Fetch all articles from a section page. Returns list of (article_id, text)."""
    url = f"{BASE_URL}/LawArticles.aspx?LawTreeSectionID={section_id}&lawId={law_id}&language={language}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch section {section_id} for law {law_id}: {e}")
        return []

    html = resp.text
    # Articles appear as: <h4>...<a href='...LawArticleID=X...'>N Article</a></h4>\n<p>TEXT</p>
    articles = re.findall(
        r'LawArticleID=(\d+).*?</h4>\s*<p>(.*?)</p>',
        html,
        re.DOTALL,
    )

    results = []
    for aid, raw_text in articles:
        text = clean_html(raw_text)
        if text:
            results.append((aid, text))
    return results


def fetch_law_text(law_id: int, section_ids: list, language: str = "ar") -> str:
    """Fetch full text of a law by iterating through all its sections."""
    all_articles = []
    seen_ids = set()

    for section_id in section_ids:
        articles = fetch_section_text(law_id, section_id, language)
        for aid, text in articles:
            if aid not in seen_ids:
                seen_ids.add(aid)
                all_articles.append(text)
        time.sleep(0.5)

    return "\n\n".join(all_articles)


def fetch_law(law_id: int) -> Optional[dict]:
    """Fetch a complete law with metadata and full text."""
    # Try Arabic first (more complete coverage)
    meta = fetch_law_metadata(law_id, "ar")
    if not meta:
        return None

    section_ids = meta.pop("section_ids", [])

    text_ar = ""
    if section_ids:
        text_ar = fetch_law_text(law_id, section_ids, "ar")
    time.sleep(0.5)

    # Try English translation
    meta_en = fetch_law_metadata(law_id, "en")
    text_en = ""
    title_en = ""
    if meta_en:
        section_ids_en = meta_en.pop("section_ids", [])
        title_en = meta_en["title"]
        if section_ids_en:
            text_en = fetch_law_text(law_id, section_ids_en, "en")

    # Combine texts
    text_parts = []
    if text_en:
        text_parts.append(text_en)
    if text_ar:
        text_parts.append(text_ar)
    full_text = "\n\n---\n\n".join(text_parts) if len(text_parts) > 1 else (text_parts[0] if text_parts else "")

    return normalize(meta, full_text, title_en)


def normalize(meta: dict, full_text: str, title_en: str = "") -> dict:
    """Normalize a law record to standard schema."""
    law_id = meta["law_id"]
    title = meta["title"]
    if title_en and title_en != title:
        title = f"{title_en}\n{title}"

    return {
        "_id": f"QA_ALMEEZAN_{law_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": meta.get("date", ""),
        "url": f"{BASE_URL}/LawPage.aspx?id={law_id}&language=en",
        "law_type": meta.get("law_type", ""),
        "number": meta.get("number", ""),
        "status": meta.get("status", ""),
        "article_count": meta.get("article_count", ""),
        "language": "ar",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all legislation records."""
    count = 0
    limit = 15 if sample else None
    errors = 0

    for law_id in range(MIN_ID, MAX_ID + 1):
        if limit and count >= limit:
            break

        try:
            record = fetch_law(law_id)
        except Exception as e:
            logger.error(f"Error fetching law {law_id}: {e}")
            errors += 1
            if errors > 20:
                logger.error("Too many errors, stopping")
                break
            continue

        if record is None:
            continue  # No law at this ID

        if not record.get("text"):
            logger.warning(f"Law {law_id} has no full text, skipping")
            continue

        count += 1
        logger.info(f"[{count}] Law {law_id}: {record['title'][:60]}... ({len(record['text'])} chars)")
        yield record
        time.sleep(1.0)

    logger.info(f"Done. Fetched {count} laws.")


def save_sample(record: dict) -> None:
    """Save a record to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{record['_id']}.json"
    path = SAMPLE_DIR / fname
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def cmd_test_api():
    """Test API connectivity."""
    print("Testing Al Meezan connectivity...")
    meta = fetch_law_metadata(2284, "en")
    if meta:
        print(f"OK: {meta['title'][:80]}")
        print(f"Type: {meta['law_type']}, Status: {meta['status']}")
        print(f"Sections: {len(meta.get('section_ids', []))}")
    else:
        print("FAIL: Could not fetch constitution (ID 2284)")
        sys.exit(1)


def cmd_bootstrap(sample: bool = False):
    """Run the bootstrap."""
    count = 0
    for record in fetch_all(sample=sample):
        if sample:
            save_sample(record)
        count += 1

    if sample:
        print(f"\nSaved {count} sample records to {SAMPLE_DIR}/")
    else:
        print(f"\nFetched {count} legislation records.")

    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QA/AlMeezan Legislation Fetcher")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test API connectivity")
    boot = sub.add_parser("bootstrap", help="Fetch legislation")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command == "bootstrap":
        cmd_bootstrap(sample=args.sample)
    else:
        parser.print_help()
        sys.exit(1)
