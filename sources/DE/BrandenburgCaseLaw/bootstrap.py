#!/usr/bin/env python3
"""
DE/BrandenburgCaseLaw - Brandenburg State Court Decisions

Fetches court decisions from the official Brandenburg Entscheidungsdatenbank
at gerichtsentscheidungen.brandenburg.de.

Coverage:
- ~25K court decisions from 30+ Brandenburg courts
- OLG, OVG, LG, VG, AG, ArbG, SG, FG, LArbG, LSG, VerfG, etc.
- Decisions from early 2000s onward

Data source: https://gerichtsentscheidungen.brandenburg.de
Access: HTML scraping with paginated search + individual decision detail pages

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import argparse
import json
import re
import socket
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
BASE_URL = "https://gerichtsentscheidungen.brandenburg.de"
SEARCH_URL = f"{BASE_URL}/suche"
RATE_LIMIT_DELAY = 2.0
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/BrandenburgCaseLaw"
RESULTS_PER_PAGE = 20


def get_session() -> requests.Session:
    """Create a configured requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })
    return session


def search_page(session: requests.Session, page: int = 1) -> Optional[List[Dict]]:
    """Fetch a search results page and extract decision IDs + basic metadata."""
    params = {
        "input_fulltext": "",
        "input_title_abr": "",
        "input_aktenzeichen": "",
        "input_ecli": "",
        "input_date_promulgation_from": "",
        "input_date_promulgation_to": "",
        "select_source": "0",
        "page": str(page),
    }
    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = session.get(SEARCH_URL, params=params, timeout=60)
        if resp.status_code != 200:
            print(f"Search page {page} error: {resp.status_code}")
            return None
    except Exception as e:
        print(f"Search page {page} error: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    table = soup.find("table", id="resultlist") or soup.find("table")
    if not table:
        return results

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        link = cells[3].find("a", href=True) if len(cells) > 3 else None
        if not link:
            link = row.find("a", href=re.compile(r"/gerichtsentscheidung/\d+"))
        if not link:
            continue
        href = link["href"]
        m = re.search(r"/gerichtsentscheidung/(\d+)", href)
        if not m:
            continue
        doc_id = m.group(1)
        decision_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        date_str = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        title_text = link.get_text(strip=True)
        court = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        results.append({
            "doc_id": doc_id,
            "decision_type": decision_type,
            "date_str": date_str,
            "title_preview": title_text,
            "court_preview": court,
        })

    return results


def get_total_pages(session: requests.Session) -> int:
    """Get total number of search result pages."""
    params = {
        "input_fulltext": "",
        "input_title_abr": "",
        "input_aktenzeichen": "",
        "input_ecli": "",
        "input_date_promulgation_from": "",
        "input_date_promulgation_to": "",
        "select_source": "0",
        "page": "1",
    }
    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = session.get(SEARCH_URL, params=params, timeout=60)
        if resp.status_code != 200:
            return 0
    except Exception:
        return 0

    m = re.search(r'Seite\s+\d+/(\d+)', resp.text)
    if m:
        return int(m.group(1))
    return 0


def fetch_decision(session: requests.Session, doc_id: str, retries: int = 3) -> Optional[Dict]:
    """Fetch a single decision detail page and extract metadata + full text."""
    url = f"{BASE_URL}/gerichtsentscheidung/{doc_id}"
    for attempt in range(retries):
        time.sleep(RATE_LIMIT_DELAY)
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code == 200:
                return parse_decision_page(resp.text, doc_id)
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"Rate limited on {doc_id}, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code >= 500:
                print(f"Server error {resp.status_code} for {doc_id}, retrying...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"HTTP {resp.status_code} for decision {doc_id}")
                return None
        except requests.exceptions.Timeout:
            print(f"Timeout for {doc_id}, attempt {attempt + 1}/{retries}")
            time.sleep(5)
        except Exception as e:
            print(f"Error fetching {doc_id}: {e}")
            time.sleep(5)
    return None


def parse_decision_page(html: str, doc_id: str) -> Dict:
    """Parse a decision detail page into structured data."""
    soup = BeautifulSoup(html, "html.parser")

    # Extract header (case number)
    header = soup.find("h1", id="header")
    header_text = header.get_text(strip=True) if header else ""
    case_number = header_text.replace("Entscheidung", "").strip() if header_text else ""

    # Extract metadata from table
    metadata = {}
    meta_div = soup.find("div", id="metadata")
    if meta_div:
        for row in meta_div.find_all("tr"):
            ths = row.find_all("th")
            tds = row.find_all("td")
            for i, th in enumerate(ths):
                key = th.get_text(strip=True)
                if i < len(tds):
                    val = tds[i].get_text(strip=True)
                    if key and val and val != "-":
                        metadata[key] = val

    court = metadata.get("Gericht", "")
    date_str = metadata.get("Entscheidungsdatum", "")
    aktenzeichen = metadata.get("Aktenzeichen", case_number)
    ecli = metadata.get("ECLI", "")
    decision_type = metadata.get("Dokumententyp", "")
    normen = metadata.get("Normen", "")

    # Extract full text from the decision detail div
    detail_div = soup.find("div", id="gerichtsentscheidung-detail")
    if detail_div:
        # Remove script/style tags
        for tag in detail_div.find_all(["script", "style"]):
            tag.decompose()
        text = detail_div.get_text(separator="\n")
    else:
        # Fallback: try docLayoutText
        doc_text_divs = soup.find_all("div", class_="docLayoutText")
        parts = []
        for div in doc_text_divs:
            for tag in div.find_all(["script", "style"]):
                tag.decompose()
            parts.append(div.get_text(separator="\n"))
        text = "\n\n".join(parts)

    # Clean text
    text = unescape(text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
    text = text.strip()

    # Parse date
    date = parse_german_date(date_str)

    # Build title
    title_parts = []
    if court:
        title_parts.append(court)
    if aktenzeichen:
        title_parts.append(aktenzeichen)
    if decision_type:
        title_parts.append(decision_type)
    if date_str:
        title_parts.append(date_str)
    title = " - ".join(title_parts) if title_parts else f"Entscheidung {doc_id}"

    return {
        "doc_id": doc_id,
        "title": title,
        "text": text,
        "court": court,
        "case_number": aktenzeichen,
        "ecli": ecli,
        "decision_type": decision_type,
        "date": date,
        "date_raw": date_str,
        "normen": normen,
        "url": f"{BASE_URL}/gerichtsentscheidung/{doc_id}",
    }


def parse_german_date(date_str: str) -> Optional[str]:
    """Parse DD.MM.YYYY to ISO 8601."""
    if not date_str:
        return None
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        # Sanity check the year
        yr = int(year)
        if yr < 1900 or yr > 2100:
            return None
        return f"{year}-{month}-{day}"
    return None


def normalize(raw: Dict) -> Dict:
    """Normalize a raw decision record into standard schema."""
    return {
        "_id": f"BB-{raw['doc_id']}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "doc_id": raw.get("doc_id", ""),
        "court": raw.get("court", ""),
        "case_number": raw.get("case_number", ""),
        "decision_type": raw.get("decision_type", ""),
        "ecli": raw.get("ecli", ""),
        "normen": raw.get("normen", ""),
        "jurisdiction": "Brandenburg",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Yield all decisions."""
    session = get_session()
    total_pages = get_total_pages(session)
    print(f"Total search pages: {total_pages} (~{total_pages * RESULTS_PER_PAGE:,} decisions)")

    if total_pages == 0:
        print("Could not determine total pages")
        return

    seen_ids = set()
    count = 0
    errors = 0

    for page in range(1, total_pages + 1):
        if page % 50 == 0:
            print(f"  Page {page}/{total_pages}, fetched {count} decisions, {errors} errors")

        results = search_page(session, page)
        if not results:
            continue

        for item in results:
            doc_id = item["doc_id"]
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            raw = fetch_decision(session, doc_id)
            if not raw:
                errors += 1
                continue

            record = normalize(raw)
            if record.get("text") and len(record["text"]) >= 100:
                yield record
                count += 1
            else:
                errors += 1

            if limit and count >= limit:
                print(f"Reached limit of {limit} decisions")
                return

    print(f"Fetched {count} decisions with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records from various pages."""
    session = get_session()
    total_pages = get_total_pages(session)
    print(f"Total search pages: {total_pages}")

    samples = []
    # Sample from different pages to get variety
    pages_to_try = [1, 50, 200, 500, 800, 1000, 1200]
    pages_to_try = [p for p in pages_to_try if p <= total_pages]

    for page in pages_to_try:
        if len(samples) >= count:
            break
        print(f"Scanning search page {page}...")
        results = search_page(session, page)
        if not results:
            continue

        for item in results[:3]:  # Take up to 3 per page
            if len(samples) >= count:
                break
            doc_id = item["doc_id"]
            print(f"  Fetching decision {doc_id}...")
            raw = fetch_decision(session, doc_id)
            if not raw:
                continue
            record = normalize(raw)
            if record.get("text") and len(record["text"]) >= 100:
                samples.append(record)
                text_len = len(record["text"])
                print(f"  Sample {len(samples)}: {text_len:,} chars - {record.get('court', 'N/A')} {record.get('case_number', '')}")

    return samples


def save_samples(samples: List[Dict]) -> None:
    """Save sample records to disk."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(samples):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(samples)} samples to {SAMPLE_DIR}")


def validate_samples(samples: List[Dict]) -> bool:
    """Validate sample records meet quality requirements."""
    print("\n=== Sample Validation ===")
    issues = []
    if len(samples) < 10:
        issues.append(f"Only {len(samples)} samples, need at least 10")
    text_lengths = []
    for i, record in enumerate(samples):
        text = record.get("text", "")
        if not text:
            issues.append(f"Record {i}: missing 'text' field")
        elif len(text) < 200:
            issues.append(f"Record {i}: text too short ({len(text)} chars)")
        else:
            text_lengths.append(len(text))
        if not record.get("_id"):
            issues.append(f"Record {i}: missing '_id'")
        if not record.get("title"):
            issues.append(f"Record {i}: missing 'title'")
        if "<div" in text or "<span" in text or "<table" in text:
            issues.append(f"Record {i}: raw HTML tags found in text")
    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Records with text: {len(text_lengths)}/{len(samples)}")
        print(f"Average text length: {avg_len:,.0f} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")
    courts = set(r.get("court") for r in samples if r.get("court"))
    print(f"Unique courts: {len(courts)}")
    for court in sorted(courts)[:10]:
        print(f"  - {court}")
    eclis = [r.get("ecli") for r in samples if r.get("ecli")]
    print(f"Records with ECLI: {len(eclis)}/{len(samples)}")
    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False
    print("\nAll validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description=f"{SOURCE_ID} data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "status"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--count", type=int, default=15)
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = fetch_sample(args.count)
            save_samples(samples)
            if validate_samples(samples):
                print("\nBootstrap sample complete")
                return 0
            else:
                print("\nValidation failed")
                return 1
        else:
            print("Full bootstrap - fetching all decisions...")
            count = 0
            for record in fetch_all():
                count += 1
                if count % 10 == 0:
                    print(f"Fetched {count} records...")
            print(f"Total: {count} records")
    elif args.command == "update":
        print("Fetching recent updates...")
        count = 0
        for record in fetch_all(limit=50):
            count += 1
        print(f"Fetched {count} updated decisions")
    elif args.command == "status":
        session = get_session()
        total_pages = get_total_pages(session)
        est_decisions = total_pages * RESULTS_PER_PAGE
        print(f"\n{SOURCE_ID} Status:")
        print(f"  Total pages: {total_pages}")
        print(f"  Estimated decisions: ~{est_decisions:,}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
