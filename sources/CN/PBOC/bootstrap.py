#!/usr/bin/env python3
"""
CN/PBOC -- People's Bank of China Regulations (中国人民银行法规)

Fetches regulations, rules, and normative documents from pbc.gov.cn.

Strategy:
  - List: Scrape paginated HTML list pages from 4 sections:
      144951 (National Laws), 144953 (State Council Regulations),
      144957 (PBC Rules/Orders), 3581332 (Announcements/Normative Docs)
  - Detail: Fetch individual pages, extract full text from <td class="content">
  - Pagination: page 1 = index.html, page N = {hash}-{N}.html
  - ~570 total regulations with full text in Chinese

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test-api
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

try:
    from common.pdf_extract import extract_pdf_markdown
    HAS_PDF_EXTRACT = True
except ImportError:
    HAS_PDF_EXTRACT = False

SOURCE_ID = "CN/PBOC"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.PBOC")

BASE_URL = "https://www.pbc.gov.cn"

# Section ID → (label, data_type, pagination_hash, total_pages)
SECTIONS = {
    144951: ("national_law", "legislation", "21885", 2),
    144953: ("state_council_regulation", "legislation", None, 1),
    144957: ("pbc_rule", "legislation", "21892", 6),
    3581332: ("announcement", "doctrine", "3b3662a6", 22),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

MIN_TEXT_LENGTH = 100


def _get(url, retries=3, backoff=5, **kwargs):
    kwargs.setdefault("timeout", 30)
    kwargs.setdefault("headers", HEADERS)
    for attempt in range(retries):
        try:
            resp = requests.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries} for {url[:80]}... waiting {wait}s")
                time.sleep(wait)
            else:
                raise


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '\n', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&ensp;', ' ', text)
    text = re.sub(r'&emsp;', ' ', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&[a-z]+;', ' ', text)
    text = re.sub(r'&#\d+;', '', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines).strip()


def _list_page_url(section_id: int, page: int) -> str:
    """Build URL for a list page."""
    base = f"{BASE_URL}/tiaofasi/144941/{section_id}"
    if page == 1:
        return f"{base}/index.html"
    pag_hash = SECTIONS[section_id][2]
    if pag_hash:
        return f"{base}/{pag_hash}-{page}.html"
    return f"{base}/index.html"


def parse_list_page(html: str, section_id: int) -> list:
    """Extract (url, title) pairs from a list page."""
    pattern = rf'/tiaofasi/144941/{section_id}/(\d+)/index\.html'
    results = []
    seen = set()
    for m in re.finditer(
        rf'<a[^>]+href="(/tiaofasi/144941/{section_id}/[^"]+)"[^>]*>(.*?)</a>',
        html, re.DOTALL
    ):
        url_path = m.group(1)
        title = strip_html(m.group(2)).strip()
        if url_path in seen:
            continue
        seen.add(url_path)
        # Skip pagination links
        if re.search(r'-\d+\.html$', url_path):
            continue
        # Skip .docx/.doc links
        if re.search(r'\.(docx?|pdf|xlsx?)$', url_path, re.I):
            continue
        results.append((url_path, title))
    return results


def fetch_detail_page(url_path: str) -> dict:
    """Fetch a regulation detail page and extract metadata + full text."""
    url = f"{BASE_URL}{url_path}"
    try:
        resp = _get(url)
    except Exception as e:
        logger.error(f"Failed to fetch {url_path}: {e}")
        return {}

    html = resp.text

    # Extract date — prefer <meta name="PubDate">, fallback to hui12 class
    date_str = ""
    pub_m = re.search(r'<meta[^>]*name="PubDate"[^>]*content="([^"]*)"', html, re.I)
    if pub_m:
        raw = pub_m.group(1).strip().split(" ")[0]
        if re.match(r'\d{4}-\d{2}-\d{2}', raw):
            date_str = raw
    if not date_str:
        date_m = re.search(r'class="hui12"[^>]*>\s*(\d{4}-\d{2}-\d{2})', html)
        if date_m:
            date_str = date_m.group(1)
    if not date_str:
        date_m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', html)
        if date_m:
            date_str = f"{date_m.group(1)}-{int(date_m.group(2)):02d}-{int(date_m.group(3)):02d}"

    # Extract content from <td class="content">
    content_m = re.search(r'<td[^>]*class="content"[^>]*>(.*?)</td>', html, re.DOTALL)
    text = ""
    if content_m:
        text = strip_html(content_m.group(1))

    # If content is too short, try broader extraction
    if len(text) < MIN_TEXT_LENGTH:
        # Try zoom class
        zoom_m = re.search(r'<div[^>]*class="zoom"[^>]*>(.*?)</div>', html, re.DOTALL)
        if zoom_m:
            alt_text = strip_html(zoom_m.group(1))
            if len(alt_text) > len(text):
                text = alt_text

    # Extract title from <title> tag as fallback
    title_from_page = ""
    title_m = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
    if title_m:
        title_from_page = strip_html(title_m.group(1)).strip()
        # Remove site suffix
        title_from_page = re.sub(r'\s*[-_|].*$', '', title_from_page)

    return {
        "text": text,
        "date": date_str,
        "title_from_page": title_from_page,
    }


def _extract_doc_id(url_path: str) -> str:
    """Extract numeric doc ID from URL path."""
    m = re.search(r'/(\d+)/index\.html$', url_path)
    return m.group(1) if m else url_path.replace("/", "_")


def normalize(url_path: str, list_title: str, detail: dict,
              section_label: str, data_type: str) -> dict:
    doc_id = _extract_doc_id(url_path)
    title = list_title or detail.get("title_from_page", "")
    text = detail.get("text", "")
    date_str = detail.get("date", "")

    return {
        "_id": f"PBOC-{doc_id}",
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": f"{BASE_URL}{url_path}",
        "category": section_label,
        "language": "zh-CN",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    sample_per_section = 4 if sample else None
    total_yielded = 0

    for section_id, (label, data_type, pag_hash, max_pages) in SECTIONS.items():
        logger.info(f"--- Section: {label} (id={section_id}) ---")
        section_count = 0

        for page in range(1, max_pages + 1):
            url = _list_page_url(section_id, page)
            logger.info(f"  Fetching list page {page}: {url}")

            try:
                resp = _get(url)
            except Exception as e:
                logger.error(f"  Failed to fetch list page: {e}")
                break

            entries = parse_list_page(resp.text, section_id)
            if not entries:
                logger.info(f"  No entries on page {page}, stopping.")
                break

            logger.info(f"  Found {len(entries)} entries on page {page}")

            for url_path, list_title in entries:
                time.sleep(1.5)  # rate limit

                try:
                    detail = fetch_detail_page(url_path)
                except Exception as e:
                    logger.error(f"  Failed to fetch detail {url_path}: {e}")
                    continue

                if not detail or not detail.get("text"):
                    logger.warning(f"  No text for {url_path}: {list_title[:60]}")
                    continue

                record = normalize(url_path, list_title, detail, label, data_type)
                if len(record["text"]) < MIN_TEXT_LENGTH:
                    logger.warning(f"  Short text ({len(record['text'])} chars) for {list_title[:60]}")
                    continue

                yield record
                section_count += 1
                total_yielded += 1

                if sample_per_section and section_count >= sample_per_section:
                    break

            if sample_per_section and section_count >= sample_per_section:
                logger.info(f"  Sample limit reached for {label}: {section_count} docs")
                break

            time.sleep(1)

        logger.info(f"  Section {label}: {section_count} documents fetched")

    logger.info(f"Total documents fetched: {total_yielded}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    try:
        since_dt = datetime.strptime(since, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {since}")
        return

    for section_id, (label, data_type, pag_hash, max_pages) in SECTIONS.items():
        for page in range(1, max_pages + 1):
            url = _list_page_url(section_id, page)
            try:
                resp = _get(url)
            except Exception:
                break

            entries = parse_list_page(resp.text, section_id)
            if not entries:
                break

            stop_paging = False
            for url_path, list_title in entries:
                time.sleep(1.5)
                try:
                    detail = fetch_detail_page(url_path)
                except Exception:
                    continue

                if not detail or not detail.get("date"):
                    continue

                try:
                    row_dt = datetime.strptime(detail["date"], "%Y-%m-%d")
                except ValueError:
                    continue

                if row_dt < since_dt:
                    stop_paging = True
                    break

                if not detail.get("text"):
                    continue

                record = normalize(url_path, list_title, detail, label, data_type)
                if record["text"]:
                    yield record

            if stop_paging:
                break
            time.sleep(1)


def save_samples(records: list, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        fname = f"{rec['_id']}.json"
        with open(out_dir / fname, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} samples to {out_dir}")


def test_api():
    """Quick connectivity and data check."""
    print("=== Testing CN/PBOC ===\n")

    for section_id, (label, data_type, pag_hash, max_pages) in SECTIONS.items():
        url = _list_page_url(section_id, 1)
        try:
            resp = _get(url)
            entries = parse_list_page(resp.text, section_id)
            print(f"Section '{label}' (id={section_id}): {len(entries)} entries on page 1, max_pages={max_pages}")
            if entries:
                url_path, title = entries[0]
                detail = fetch_detail_page(url_path)
                text = detail.get("text", "")
                print(f"  First: {title[:80]}")
                print(f"  Date: {detail.get('date', 'N/A')}")
                print(f"  Text: {len(text)} chars")
                print(f"  Preview: {text[:150]}...")
        except Exception as e:
            print(f"Section '{label}' (id={section_id}): ERROR - {e}")
        print()


def main():
    parser = argparse.ArgumentParser(description="CN/PBOC Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api", "updates"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (4 per section)")
    parser.add_argument("--since", help="For updates: fetch docs since YYYY-MM-DD")
    parser.add_argument("--full", action="store_true", help="Fetch all documents (no sample limit)")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        is_sample = args.sample or not args.full
        records = list(fetch_all(sample=is_sample))
        if records:
            out_dir = SAMPLE_DIR if is_sample else SOURCE_DIR / "data"
            save_samples(records, out_dir)

            texts = [len(r["text"]) for r in records]
            print(f"\n=== CN/PBOC Bootstrap Results ===")
            print(f"Records: {len(records)}")
            print(f"Text lengths: min={min(texts)}, max={max(texts)}, avg={sum(texts)//len(texts)}")
            print(f"With text: {sum(1 for t in texts if t > 0)}/{len(records)}")
            cats = {}
            for r in records:
                cats[r["category"]] = cats.get(r["category"], 0) + 1
            print(f"Categories: {cats}")
        else:
            print("ERROR: No records fetched!")
            sys.exit(1)
    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since YYYY-MM-DD required for updates")
            sys.exit(1)
        records = list(fetch_updates(args.since))
        if records:
            save_samples(records, SOURCE_DIR / "updates")
        print(f"Updates since {args.since}: {len(records)} records")


if __name__ == "__main__":
    main()
