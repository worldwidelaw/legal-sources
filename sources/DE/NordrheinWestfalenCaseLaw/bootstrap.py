#!/usr/bin/env python3
"""
DE/NordrheinWestfalenCaseLaw - Nordrhein-Westfalen State Court Decisions

Fetches court decisions from the NRWE database at nrwesuche.justiz.nrw.de.

Coverage:
- ~208K+ court decisions from all NRW courts
- OLG, OVG, LG, VG, AG, ArbG, SG, FG, LAG, LSG, VerfGH, etc.
- Decisions from early 2000s onward

Data source: https://nrwe.de/ (redirects to nrwesuche.justiz.nrw.de)
Access: HTML scraping — POST search form + individual decision pages on nrwe.justiz.nrw.de

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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
SEARCH_URL = "https://nrwesuche.justiz.nrw.de/index.php"
DECISION_BASE = "https://nrwe.justiz.nrw.de"
RATE_LIMIT_DELAY = 2.0
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/NordrheinWestfalenCaseLaw"
RESULTS_PER_PAGE = 100


def get_session() -> requests.Session:
    """Create a configured requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })
    return session


def search_decisions(session: requests.Session, page: int = 1,
                     date_from: str = "", date_to: str = "",
                     pub_from: str = "", pub_to: str = "",
                     q: str = "") -> Optional[List[Dict]]:
    """Search NRWE and return decision links + metadata from a results page.

    Dates in DD.MM.YYYY format.
    """
    data = {
        "gerichtstyp": "",
        "gerichtsbarkeit": "",
        "gerichtsort": "",
        "entscheidungsart": "",
        "date": "",
        "von": date_from,
        "bis": date_to,
        "von2": pub_from,
        "bis2": pub_to,
        "aktenzeichen": "",
        "schlagwoerter": "",
        "q": q,
        "method": "stem",
        "qSize": str(RESULTS_PER_PAGE),
        "sortieren_nach": "datum_absteigend",
        "absenden": "Suchen",
        "advanced_search": "true",
    }
    if page > 1:
        data[f"page{page}"] = str(page)

    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = session.post(SEARCH_URL, data=data, timeout=60)
        if resp.status_code != 200:
            print(f"Search page {page} error: {resp.status_code}")
            return None
    except Exception as e:
        print(f"Search page {page} error: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    for div in soup.find_all("div", class_="einErgebnis"):
        link = div.find("a", href=re.compile(r'nrwe\.justiz\.nrw\.de'))
        if not link:
            continue
        href = link["href"]
        title_preview = link.get_text(strip=True)

        # Extract metadata from the result div text
        div_text = div.get_text(separator="\n")
        ecli = ""
        ecli_m = re.search(r'ECLI:\S+', div_text)
        if ecli_m:
            ecli = ecli_m.group(0)

        decision_type = ""
        dt_m = re.search(r'Entscheidungsart:\s*(.+)', div_text)
        if dt_m:
            decision_type = dt_m.group(1).strip()

        case_number = ""
        az_m = re.search(r'Aktenzeichen:\s*(.+)', div_text)
        if az_m:
            case_number = az_m.group(1).strip()

        date_str = ""
        date_m = re.search(r'Entscheidungsdatum:\s*(\d{2}\.\d{2}\.\d{4})', div_text)
        if date_m:
            date_str = date_m.group(1)

        results.append({
            "url": href,
            "title_preview": title_preview,
            "ecli": ecli,
            "decision_type": decision_type,
            "case_number": case_number,
            "date_str": date_str,
        })

    return results


def get_total_results(session: requests.Session,
                      pub_from: str = "", pub_to: str = "") -> int:
    """Get total number of results for a search."""
    data = {
        "gerichtstyp": "",
        "gerichtsbarkeit": "",
        "gerichtsort": "",
        "entscheidungsart": "",
        "date": "",
        "von": "",
        "bis": "",
        "von2": pub_from,
        "bis2": pub_to,
        "aktenzeichen": "",
        "schlagwoerter": "",
        "q": "",
        "method": "stem",
        "qSize": "10",
        "sortieren_nach": "datum_absteigend",
        "absenden": "Suchen",
        "advanced_search": "true",
    }
    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = session.post(SEARCH_URL, data=data, timeout=60)
        if resp.status_code != 200:
            return 0
    except Exception:
        return 0

    # Look for result count patterns like "208.543 Treffer" or "von 208543"
    m = re.search(r'([\d.]+)\s*Treffer', resp.text)
    if m:
        return int(m.group(1).replace(".", ""))
    m = re.search(r'von\s+([\d.]+)', resp.text)
    if m:
        return int(m.group(1).replace(".", ""))
    return 0


def fetch_decision(session: requests.Session, url: str, retries: int = 3) -> Optional[Dict]:
    """Fetch a single decision page and extract metadata + full text."""
    for attempt in range(retries):
        time.sleep(RATE_LIMIT_DELAY)
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code == 200:
                return parse_decision_page(resp.text, url)
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"Rate limited on {url}, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code >= 500:
                print(f"Server error {resp.status_code} for {url}, retrying...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"HTTP {resp.status_code} for {url}")
                return None
        except requests.exceptions.Timeout:
            print(f"Timeout for {url}, attempt {attempt + 1}/{retries}")
            time.sleep(5)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            time.sleep(5)
    return None


def parse_decision_page(html: str, url: str) -> Dict:
    """Parse a decision page from nrwe.justiz.nrw.de into structured data."""
    soup = BeautifulSoup(html, "html.parser")

    # Extract metadata from feldbezeichnung/feldinhalt pairs
    metadata = {}
    labels = soup.find_all("div", class_="feldbezeichnung")
    for label in labels:
        key = label.get_text(strip=True).rstrip(":")
        value_div = label.find_next_sibling("div", class_="feldinhalt")
        if value_div:
            val = value_div.get_text(strip=True)
            if key and val:
                metadata[key] = val

    court = metadata.get("Gericht", "")
    date_str = metadata.get("Datum", metadata.get("Entscheidungsdatum", ""))
    case_number = metadata.get("Aktenzeichen", "")
    ecli = metadata.get("ECLI", "")
    decision_type = metadata.get("Entscheidungsart", "")
    spruchkoerper = metadata.get("Spruchkörper", "")

    # Extract structured text sections
    text_parts = []

    # Leitsätze
    leitsaetze = soup.find("div", class_=re.compile(r"leitsaetze"))
    if leitsaetze:
        for tag in leitsaetze.find_all(["script", "style"]):
            tag.decompose()
        ls_text = leitsaetze.get_text(separator="\n").strip()
        if ls_text:
            text_parts.append("Leitsätze:\n" + ls_text)

    # Tenor
    tenor = soup.find("div", class_=re.compile(r"tenor"))
    if tenor:
        for tag in tenor.find_all(["script", "style"]):
            tag.decompose()
        t_text = tenor.get_text(separator="\n").strip()
        if t_text:
            text_parts.append("Tenor:\n" + t_text)

    # Main body paragraphs (Tatbestand + Entscheidungsgründe)
    body_paras = soup.find_all("p", class_="absatzLinks")
    if body_paras:
        para_texts = []
        for p in body_paras:
            for tag in p.find_all(["script", "style"]):
                tag.decompose()
            # Remove paragraph number spans
            for span in p.find_all("span", class_="absatzRechts"):
                span.decompose()
            pt = p.get_text(strip=True)
            if pt:
                para_texts.append(pt)
        if para_texts:
            text_parts.append("\n\n".join(para_texts))

    # Fallback: if no structured sections found, try the whole body
    if not text_parts:
        body = soup.find("body")
        if body:
            for tag in body.find_all(["script", "style", "nav", "footer", "header", "meta"]):
                tag.decompose()
            # Remove metadata divs we already parsed
            for div in body.find_all("div", class_=["feldbezeichnung", "feldinhalt"]):
                div.decompose()
            fallback = body.get_text(separator="\n")
            text_parts.append(fallback)

    text = "\n\n".join(text_parts)

    # Clean text
    text = unescape(text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
    text = text.strip()

    # Parse date
    date = parse_german_date(date_str)

    # Build doc_id from URL path
    doc_id = url.rstrip("/").split("/")[-1].replace(".html", "")

    # Build title
    title_parts = []
    if court:
        title_parts.append(court)
    if case_number:
        title_parts.append(case_number)
    if decision_type:
        title_parts.append(decision_type)
    if date_str:
        title_parts.append(date_str)
    title = " - ".join(title_parts) if title_parts else f"NRWE {doc_id}"

    return {
        "doc_id": doc_id,
        "title": title,
        "text": text,
        "court": court,
        "case_number": case_number,
        "ecli": ecli,
        "decision_type": decision_type,
        "spruchkoerper": spruchkoerper,
        "date": date,
        "date_raw": date_str,
        "url": url,
    }


def parse_german_date(date_str: str) -> Optional[str]:
    """Parse DD.MM.YYYY to ISO 8601."""
    if not date_str:
        return None
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        yr = int(year)
        if yr < 1900 or yr > 2100:
            return None
        return f"{year}-{month}-{day}"
    return None


def normalize(raw: Dict) -> Dict:
    """Normalize a raw decision record into standard schema."""
    return {
        "_id": f"NW-{raw['doc_id']}",
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
        "spruchkoerper": raw.get("spruchkoerper", ""),
        "jurisdiction": "Nordrhein-Westfalen",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Yield all decisions using publication date ranges."""
    session = get_session()
    total = get_total_results(session)
    print(f"Estimated total decisions: ~{total:,}")

    seen_urls = set()
    count = 0
    errors = 0

    # Iterate year by year using publication date
    current_year = datetime.now().year
    for year in range(current_year, 1999, -1):
        pub_from = f"01.01.{year}"
        pub_to = f"31.12.{year}"
        page = 1

        while True:
            results = search_decisions(session, page=page,
                                       pub_from=pub_from, pub_to=pub_to)
            if not results:
                break

            for item in results:
                url = item["url"]
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                raw = fetch_decision(session, url)
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

            if len(results) < RESULTS_PER_PAGE:
                break
            page += 1

        if count % 100 == 0:
            print(f"  Year {year}: fetched {count} decisions so far ({errors} errors)")

    print(f"Fetched {count} decisions with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records from different time periods."""
    session = get_session()
    total = get_total_results(session)
    print(f"Estimated total decisions: ~{total:,}")

    samples = []
    # Sample from different years for variety
    sample_ranges = [
        ("01.01.2025", "30.04.2025"),
        ("01.01.2024", "30.06.2024"),
        ("01.01.2020", "30.06.2020"),
        ("01.01.2015", "30.06.2015"),
        ("01.01.2010", "30.06.2010"),
    ]

    for pub_from, pub_to in sample_ranges:
        if len(samples) >= count:
            break
        print(f"\nSearching publication date range {pub_from} - {pub_to}...")
        results = search_decisions(session, page=1,
                                   pub_from=pub_from, pub_to=pub_to)
        if not results:
            print("  No results")
            continue
        print(f"  Found {len(results)} results on page 1")

        for item in results[:4]:
            if len(samples) >= count:
                break
            url = item["url"]
            print(f"  Fetching {url}...")
            raw = fetch_decision(session, url)
            if not raw:
                continue
            record = normalize(raw)
            if record.get("text") and len(record["text"]) >= 100:
                samples.append(record)
                text_len = len(record["text"])
                print(f"  Sample {len(samples)}: {text_len:,} chars - "
                      f"{record.get('court', 'N/A')} {record.get('case_number', '')}")

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
        total = get_total_results(session)
        print(f"\n{SOURCE_ID} Status:")
        print(f"  Estimated decisions: ~{total:,}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
