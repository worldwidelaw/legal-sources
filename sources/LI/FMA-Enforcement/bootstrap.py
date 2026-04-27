#!/usr/bin/env python3
"""
LI/FMA-Enforcement - Liechtenstein Financial Market Authority Enforcement

Fetches enforcement-related content from FMA Liechtenstein:
1. Enforcement news (license withdrawals, sanctions, prohibitions)
2. Warning notices (unauthorized firms, clone firms, abuse)
3. FMA-Praxis annual brochures (anonymised enforcement decisions as PDF)

Data source: https://www.fma-li.li
API: Dynamic Search JSON endpoints at /dynamic-search/default/
License: Open Government Data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.pdf_extract import extract_pdf_markdown

BASE_URL = "https://www.fma-li.li"
SEARCH_BASE = f"{BASE_URL}/dynamic-search/default"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "LI/FMA-Enforcement"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "application/json, text/html",
}
REQUEST_DELAY = 1.5

# Enforcement-related keywords for filtering news items
ENFORCEMENT_KEYWORDS = [
    "enforcement", "sanction", "fine", "penalty", "withdraw", "revok",
    "prohibit", "suspend", "coercive", "violation", "breach", "unlicensed",
    "unauthorized", "warning", "disciplin", "cease", "desist",
    "Sanktion", "Busse", "Entzug", "Widerruf", "Verbot", "Suspendierung",
    "Zwangsmassnahme", "Verletzung", "Verstoss", "unbewilligt",
]


def fetch_json(endpoint: str, params: dict) -> Optional[dict]:
    """Fetch JSON from the FMA dynamic search API."""
    url = f"{SEARCH_BASE}/{endpoint}"
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Error fetching {endpoint}: {e}")
        return None


def fetch_html_text(url: str) -> str:
    """Fetch an HTML page and extract clean text from paragraphs."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove nav, header, footer, script, style
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        # Try main content area first
        main = soup.find("main") or soup.find("div", class_="content") or soup.body
        if not main:
            return ""

        text = main.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()
    except Exception as e:
        print(f"  Error fetching HTML {url}: {e}")
        return ""


def is_enforcement_related(item: dict) -> bool:
    """Check if a news item is enforcement-related by keywords."""
    cat = (item.get("category_label") or "").lower()
    if "enforcement" in cat:
        return True

    searchable = " ".join([
        item.get("title") or "",
        item.get("teaser") or "",
        item.get("lead") or "",
        item.get("content") or "",
    ]).lower()

    return any(kw.lower() in searchable for kw in ENFORCEMENT_KEYWORDS)


def _iterate_pages(endpoint: str, locale: str, per_page: int = 100):
    """Yield items from a paginated FMA dynamic-search endpoint.

    The API returns empty `result` once a page is past the end, so we stop
    then. `total` is not reliable (often 0), so don't rely on it.
    """
    page = 1
    while True:
        data = fetch_json(endpoint, {
            "locale": locale,
            "perPage": per_page,
            "page": page,
        })
        if not data or "result" not in data:
            return
        items = data["result"]
        if not items:
            return
        for item in items:
            yield item
        page += 1


def fetch_news(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch enforcement-related news items with full text (all pages, both locales)."""
    print("Fetching enforcement news...")

    # Iterate both locales fully; each locale's element_id space is distinct,
    # so we get DE + EN coverage. Deduplication is handled by the caller.
    for locale in ["en", "de"]:
        count = 0
        total_seen = 0
        for item in _iterate_pages("j-news-search", locale):
            total_seen += 1
            if max_records and count >= max_records:
                break

            if not is_enforcement_related(item):
                continue

            detail_url = item.get("detail_url", "")
            if not detail_url:
                continue
            if not detail_url.startswith("http"):
                detail_url = BASE_URL + detail_url

            # Get full text from detail page
            time.sleep(REQUEST_DELAY)
            full_text = fetch_html_text(detail_url)
            if not full_text or len(full_text) < 100:
                # Fall back to API content fields
                full_text = "\n\n".join(filter(None, [
                    item.get("lead", ""),
                    item.get("content", ""),
                ]))

            if not full_text or len(full_text) < 50:
                continue

            # Parse date from unix timestamp
            date_str = None
            ts = item.get("date")
            if ts:
                try:
                    date_str = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
                except (ValueError, TypeError, OSError):
                    pass

            doc_id = f"news-{locale}-{item.get('element_id', '')}"
            title = item.get("title", "FMA Enforcement Notice")

            yield {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.utcnow().isoformat() + "Z",
                "title": title,
                "text": full_text,
                "date": date_str,
                "url": detail_url,
                "language": "deu" if locale == "de" else "eng",
                "category": item.get("category_label", ""),
                "country": "LI",
                "institution": "Financial Market Authority (FMA)",
            }
            count += 1
            print(f"    [{count}] {title[:60]}... ({len(full_text):,} chars)")

        print(f"  {locale.upper()}: {total_seen} news scanned, {count} enforcement-relevant yielded")


def fetch_warnings(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch warning notices — full text available directly in API response.

    Iterates all pages in both locales. DE warnings have larger coverage
    so are fetched first; EN warnings are then yielded with distinct IDs.
    """
    print("Fetching warning notices...")

    for locale in ["de", "en"]:
        count = 0
        for item in _iterate_pages("j-warning-search", locale):
            if max_records and count >= max_records:
                break

            text = item.get("text", "")
            title = item.get("title", "FMA Warning")
            if not text or len(text) < 30:
                continue

            date_str = None
            ts = item.get("release_date")
            if ts:
                try:
                    date_str = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
                except (ValueError, TypeError, OSError):
                    pass

            detail_url = item.get("detail_url", "")
            if detail_url and not detail_url.startswith("http"):
                detail_url = BASE_URL + detail_url

            doc_id = f"warning-{locale}-{item.get('element_id', '')}"
            cat = item.get("category_label", "Warning")

            yield {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.utcnow().isoformat() + "Z",
                "title": f"[{cat}] {title}",
                "text": text,
                "date": date_str,
                "url": detail_url or f"{BASE_URL}/en/media-public/client-protection/warning-notices-and-indications/warnings",
                "language": "deu" if locale == "de" else "eng",
                "category": cat,
                "country": "LI",
                "institution": "Financial Market Authority (FMA)",
            }
            count += 1

        print(f"  Yielded {count} warnings ({locale.upper()})")


def fetch_praxis_pdfs(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch FMA-Praxis annual enforcement brochures (PDF → text)."""
    print("Fetching FMA-Praxis publications...")

    # Paginate across all publications so we don't miss old FMA-Praxis editions.
    praxis_items = []
    for item in _iterate_pages("j-publication-search", "de"):
        title = (item.get("title") or "").lower()
        if "praxis" in title:
            praxis_items.append(item)

    print(f"  Found {len(praxis_items)} FMA-Praxis publications")

    count = 0
    for item in sorted(praxis_items, key=lambda x: x.get("publication_year", 0), reverse=True):
        if max_records and count >= max_records:
            return

        download = item.get("download", "")
        if not download:
            continue

        pdf_url = download if download.startswith("http") else BASE_URL + download
        title = item.get("title", "FMA-Praxis")
        # Extract the content year from the title (e.g. "FMA-Praxis 2024")
        year_match = re.search(r"(\d{4})", title)
        year = year_match.group(1) if year_match else str(item.get("publication_year", ""))
        doc_id = f"praxis-{year}" if year else f"praxis-{item.get('element_id', count)}"

        print(f"    Extracting PDF: {title} ...")
        time.sleep(REQUEST_DELAY)

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
        )

        if not text or len(text) < 200:
            # Fallback: try direct download and basic extraction
            print(f"    PDF extraction returned insufficient text, trying fallback...")
            try:
                resp = requests.get(pdf_url, headers=HEADERS, timeout=60)
                resp.raise_for_status()
                # Try pdfplumber directly
                import pdfplumber
                import io
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    pages = []
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            pages.append(page_text)
                    text = "\n\n".join(pages)
            except Exception as e:
                print(f"    Fallback PDF extraction failed: {e}")
                continue

        if not text or len(text) < 200:
            print(f"    Skipping {title}: insufficient text")
            continue

        detail_url = item.get("detail_url", "")
        if detail_url and not detail_url.startswith("http"):
            detail_url = BASE_URL + detail_url

        yield {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "title": title,
            "text": text,
            "date": f"{year}-01-01" if year else None,
            "url": detail_url or pdf_url,
            "language": "deu",
            "category": "FMA-Praxis",
            "country": "LI",
            "institution": "Financial Market Authority (FMA)",
        }
        count += 1
        print(f"    [{count}] {title} ({len(text):,} chars)")


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all enforcement records from all streams."""
    seen_ids = set()

    # Stream 1: Enforcement news
    for record in fetch_news(max_records):
        if record["_id"] not in seen_ids:
            seen_ids.add(record["_id"])
            yield record

    # Stream 2: Warnings
    for record in fetch_warnings(max_records):
        if record["_id"] not in seen_ids:
            seen_ids.add(record["_id"])
            yield record

    # Stream 3: FMA-Praxis PDFs
    for record in fetch_praxis_pdfs(max_records):
        if record["_id"] not in seen_ids:
            seen_ids.add(record["_id"])
            yield record

    print(f"\nTotal unique records: {len(seen_ids)}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    for record in fetch_all():
        if record.get("date"):
            try:
                doc_date = datetime.fromisoformat(record["date"])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def normalize(raw: dict) -> dict:
    """Validate and normalize the record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "date", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get("text") or len(raw["text"]) < 30:
        raise ValueError("Document has insufficient text content")

    return raw


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    for i, record in enumerate(fetch_all(max_records=sample_count)):
        try:
            normalized = normalize(record)
            records.append(normalized)

            filename = SAMPLE_DIR / f"record_{i+1:03d}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get("text", ""))
            print(f"  [{i+1:02d}] {normalized['_id']}: {normalized['title'][:60]} ({text_len:,} chars)")

        except ValueError as e:
            print(f"  Skipping record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="LI/FMA-Enforcement fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "info"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true",
                       help="Fetch all records (no limit)")

    args = parser.parse_args()

    if args.command == "info":
        print(f"LI/FMA-Enforcement - Liechtenstein FMA Enforcement")
        print(f"Source URL: {BASE_URL}")
        print(f"Streams: enforcement news, warnings, FMA-Praxis PDFs")

    elif args.command == "bootstrap":
        if args.full:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))
        else:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
