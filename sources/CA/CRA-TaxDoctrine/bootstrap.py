#!/usr/bin/env python3
"""
Canada Revenue Agency Tax Doctrine Data Fetcher

Fetches Canadian tax doctrine from canada.ca:
- Income Tax Folios (current official interpretations)
- Interpretation Bulletins (archived but still referenced)
- Income Tax Technical News (ITTNs)
- Information Circulars (ICs)

Data source: https://www.canada.ca/en/revenue-agency/services/tax/technical-information/income-tax.html
License: Crown Copyright (Government of Canada Open Data)
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

BASE = "https://www.canada.ca"
RATE_LIMIT_DELAY = 2.0  # seconds between requests
CURL_TIMEOUT = 180  # canada.ca is extremely slow from some locations

# Index pages for each document collection
FOLIO_INDEX = "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index.html"
IT_INDEX_PAGES = [
    f"/en/revenue-agency/services/forms-publications/current-income-tax-interpretation-bulletins-{n}.html"
    for n in range(1, 11)
]
ITTN_INDEX = "/en/revenue-agency/services/tax/technical-information/income-tax/current-income-tax-technical-news-ittn.html"
IC_INDEX_PAGES = [
    f"/en/revenue-agency/services/forms-publications/current-income-tax-information-circulars-{n}.html"
    for n in range(1, 7)
]


def fetch_page(url: str, retries: int = 2) -> Optional[str]:
    """Fetch a page using curl (Python requests has SSL issues with canada.ca on older Python)."""
    full_url = url if url.startswith("http") else BASE + url
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-w", "\n%{http_code}", full_url],
                capture_output=True, text=True, timeout=CURL_TIMEOUT + 10
            )
            parts = result.stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status_code = parts[0], parts[1].strip()
            else:
                body, status_code = result.stdout, "000"

            if status_code == "404":
                return None
            if not status_code.startswith("2"):
                if attempt == retries:
                    print(f"HTTP {status_code} for {full_url}", file=sys.stderr)
                    return None
                time.sleep(3)
                continue
            if body:
                return body
            if attempt == retries:
                return None
            time.sleep(3)
        except Exception as e:
            if attempt == retries:
                print(f"Failed to fetch {full_url}: {e}", file=sys.stderr)
                return None
            time.sleep(3)
    return None


def extract_text(html: str) -> tuple:
    """Extract clean text and title from HTML page."""
    soup = BeautifulSoup(html, "html.parser")

    # Title
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Main content
    main = soup.find("main")
    if not main:
        main = soup.find("article") or soup.find("div", class_="mwsbodytext")
    if not main:
        return title, ""

    # Remove nav/aside/scripts
    for e in main.find_all(["nav", "aside", "script", "style", "header", "footer"]):
        e.decompose()

    text = main.get_text(separator="\n")
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text).strip()

    # Extract date from "Page details" at bottom
    date = None
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})\s*$", text)
    if date_match:
        date = date_match.group(1)

    return title, text, date


def discover_folio_chapters() -> list:
    """Discover all Income Tax Folio chapter URLs from the index."""
    print("Discovering Income Tax Folios...", file=sys.stderr)
    html = fetch_page(FOLIO_INDEX)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    if not main:
        return []

    # Get folio sub-index pages (one per folio topic)
    folio_urls = []
    for link in main.find_all("a", href=True):
        href = link["href"]
        if "/income-tax-folios-index/" in href and "folio" in href.lower():
            folio_urls.append(href)

    # Now visit each folio sub-index to find chapter pages
    chapters = []
    seen = set()
    for folio_url in folio_urls:
        time.sleep(RATE_LIMIT_DELAY)
        html = fetch_page(folio_url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main")
        if not main:
            continue
        for link in main.find_all("a", href=True):
            href = link["href"]
            if "income-tax-folio-s" in href and href not in seen:
                seen.add(href)
                # Extract folio code from URL (e.g., s1-f1-c1)
                code_match = re.search(r"income-tax-folio-(s\d+-f\d+-c\d+)", href)
                code = code_match.group(1).upper() if code_match else ""
                chapters.append({
                    "url": href,
                    "doc_type": "folio",
                    "code": code,
                    "title_hint": link.get_text(strip=True),
                })

    print(f"  Found {len(chapters)} folio chapters", file=sys.stderr)
    return chapters


def discover_interpretation_bulletins() -> list:
    """Discover Interpretation Bulletin URLs."""
    print("Discovering Interpretation Bulletins...", file=sys.stderr)
    bulletins = []
    seen = set()

    for index_url in IT_INDEX_PAGES:
        time.sleep(RATE_LIMIT_DELAY)
        html = fetch_page(index_url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main")
        if not main:
            continue
        for link in main.find_all("a", href=True):
            href = link["href"]
            # IT bulletin links: /publications/it{NNN} or /publications/it{NNN}r{N}
            if re.search(r"/publications/it\d+", href) and href not in seen:
                seen.add(href)
                code_match = re.search(r"(it\d+r?\d*)", href)
                code = code_match.group(1).upper() if code_match else ""
                bulletins.append({
                    "url": href,
                    "doc_type": "interpretation_bulletin",
                    "code": code,
                    "title_hint": link.get_text(strip=True),
                })

    print(f"  Found {len(bulletins)} interpretation bulletins", file=sys.stderr)
    return bulletins


def discover_technical_news() -> list:
    """Discover Income Tax Technical News URLs."""
    print("Discovering Technical News...", file=sys.stderr)
    html = fetch_page(ITTN_INDEX)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    if not main:
        return []

    items = []
    seen = set()
    for link in main.find_all("a", href=True):
        href = link["href"]
        if re.search(r"/publications/itnews-?\d+", href) and href not in seen:
            seen.add(href)
            code_match = re.search(r"itnews-?(\d+)", href)
            code = f"ITTN-{code_match.group(1)}" if code_match else ""
            items.append({
                "url": href,
                "doc_type": "technical_news",
                "code": code,
                "title_hint": link.get_text(strip=True),
            })

    print(f"  Found {len(items)} technical news items", file=sys.stderr)
    return items


def discover_information_circulars() -> list:
    """Discover Information Circular URLs."""
    print("Discovering Information Circulars...", file=sys.stderr)
    items = []
    seen = set()

    for index_url in IC_INDEX_PAGES:
        time.sleep(RATE_LIMIT_DELAY)
        html = fetch_page(index_url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main")
        if not main:
            continue
        for link in main.find_all("a", href=True):
            href = link["href"]
            if re.search(r"/publications/ic\d+", href) and href not in seen:
                seen.add(href)
                code_match = re.search(r"(ic\d+-\d+[a-z]?\d*)", href)
                code = code_match.group(1).upper() if code_match else ""
                items.append({
                    "url": href,
                    "doc_type": "information_circular",
                    "code": code,
                    "title_hint": link.get_text(strip=True),
                })

    print(f"  Found {len(items)} information circulars", file=sys.stderr)
    return items


def fetch_document(doc_info: dict) -> Optional[dict]:
    """Fetch full text of a single document."""
    url = doc_info["url"]
    html = fetch_page(url)
    if not html:
        return None

    result = extract_text(html)
    if len(result) == 3:
        title, text, date = result
    else:
        title, text = result
        date = None

    if not text or len(text) < 200:
        # Some ITs have a landing page that links to full text
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main")
        if main:
            # Look for "full text" or "archived" subpage link
            for link in main.find_all("a", href=True):
                href = link["href"]
                link_text = link.get_text(strip=True).lower()
                if any(w in link_text for w in ["archived", "full text", "complete"]) or \
                   any(w in href for w in ["/archived-", "/complete-"]):
                    time.sleep(RATE_LIMIT_DELAY)
                    sub_html = fetch_page(href)
                    if sub_html:
                        sub_result = extract_text(sub_html)
                        if len(sub_result) == 3:
                            title2, text2, date2 = sub_result
                        else:
                            title2, text2 = sub_result
                            date2 = None
                        if text2 and len(text2) > len(text):
                            text = text2
                            if not title:
                                title = title2
                            if not date and date2:
                                date = date2
                            url = href  # Update URL to full text page
                    break

    if not text or len(text) < 200:
        return None

    full_url = url if url.startswith("http") else BASE + url

    return {
        "url": full_url,
        "title": title or doc_info.get("title_hint", ""),
        "text": text,
        "date": date,
        "doc_type": doc_info["doc_type"],
        "code": doc_info.get("code", ""),
    }


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all CRA tax doctrine documents."""
    all_docs = []
    all_docs.extend(discover_folio_chapters())
    all_docs.extend(discover_interpretation_bulletins())
    all_docs.extend(discover_technical_news())
    all_docs.extend(discover_information_circulars())

    print(f"\nTotal documents to fetch: {len(all_docs)}", file=sys.stderr)
    count = 0

    for doc_info in all_docs:
        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_document(doc_info)
        if raw:
            yield raw
            count += 1
            if count % 10 == 0:
                print(f"Fetched {count}/{len(all_docs)} documents...", file=sys.stderr)
            if max_docs and count >= max_docs:
                return


def normalize(raw: dict) -> dict:
    """Transform raw document into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    code = raw.get("code", "")
    doc_type = raw["doc_type"]

    # Build a stable ID
    if code:
        doc_id = f"CRA-{code}"
    else:
        # Fallback: hash the URL
        import hashlib
        url_hash = hashlib.md5(raw["url"].encode()).hexdigest()[:8]
        doc_id = f"CRA-{doc_type}-{url_hash}"

    return {
        "_id": doc_id,
        "_source": "CA/CRA-TaxDoctrine",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": raw["title"],
        "text": raw["text"],
        "date": raw.get("date"),
        "url": raw["url"],
        "doc_type": doc_type,
        "code": code,
        "language": "en",
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count + 5):
        record = normalize(raw)

        if not record["text"] or len(record["text"]) < 500:
            print(f"Skipping {record['_id']}: insufficient text ({len(record.get('text', ''))} chars)", file=sys.stderr)
            continue

        samples.append(record)

        filename = f"{record['_id']}.json"
        # Sanitize filename
        filename = re.sub(r'[^\w\-.]', '_', filename)
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CRA Tax Doctrine fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Generate sample data only")
    parser.add_argument("--count", type=int, default=15,
                       help="Number of samples to generate")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            for raw in fetch_all():
                record = normalize(raw)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for raw in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
