#!/usr/bin/env python3
"""
DE/BremenCaseLaw - Bremen State Court Decisions

Fetches court decisions from four Bremen courts via their SixCMS-based
decision overview pages:
  - Oberlandesgericht Bremen (OLG) ~596 decisions
  - Oberverwaltungsgericht Bremen (OVG) ~682 decisions
  - Verwaltungsgericht Bremen (VG) ~696 decisions
  - Landesarbeitsgericht Bremen (LArbG) ~59 decisions

Each court publishes decisions as PDFs. The scraper:
  1. Paginates through the overview pages (100 per page)
  2. Extracts metadata (date, case number, norms, legal area, type)
  3. Downloads PDFs and extracts full text via pdfplumber

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import argparse
import io
import json
import re
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, Dict, Any, List

import pdfplumber
import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
RATE_LIMIT_DELAY = 2.0
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/BremenCaseLaw"

# Court definitions: (court_code, court_name, base_url, overview_path, cms_prefix)
COURTS = [
    (
        "OLG",
        "Hanseatisches Oberlandesgericht Bremen",
        "https://www.oberlandesgericht.bremen.de",
        "/entscheidungen/entscheidungsuebersicht-2335",
        "bremen88",
    ),
    (
        "OVG",
        "Oberverwaltungsgericht Bremen",
        "https://www.oberverwaltungsgericht.bremen.de",
        "/entscheidungen/entscheidungsuebersicht-11265",
        "bremen72",
    ),
    (
        "VG",
        "Verwaltungsgericht Bremen",
        "https://www.verwaltungsgericht.bremen.de",
        "/entscheidungen/entscheidungsuebersicht-13039",
        "bremen73",
    ),
    (
        "LArbG",
        "Landesarbeitsgericht Bremen",
        "https://www.landesarbeitsgericht.bremen.de",
        "/entscheidungen/entscheidungsuebersicht-11508",
        "bremen105",
    ),
]


def get_session() -> requests.Session:
    """Create a configured requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })
    return session


def get_total_entries(session: requests.Session, base_url: str, overview_path: str) -> int:
    """Get total number of entries for a court's overview page."""
    url = f"{base_url}{overview_path}?skip=0&max=10"
    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = session.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"  Error fetching {url}: {resp.status_code}")
            return 0
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return 0
    m = re.search(r'Anzahl der Eintr[äa]ge:\s*(\d+)', resp.text)
    if m:
        return int(m.group(1))
    return 0


def parse_overview_page(html: str, base_url: str, court_code: str, court_name: str) -> List[Dict]:
    """Parse a court overview page and extract decision metadata + PDF URLs."""
    soup = BeautifulSoup(html, "html.parser")
    entries = []

    for row in soup.find_all("tr", class_="search-result"):
        tds = row.find_all("td", recursive=False)
        if len(tds) < 2:
            continue

        # Parse metadata td (first td)
        meta_td = tds[0]
        date_em = meta_td.find("em")
        date_str = date_em.get_text(strip=True) if date_em else ""

        # Extract text content split by <br> tags
        meta_text = meta_td.get_text(separator="|", strip=True)
        meta_parts = [p.strip() for p in meta_text.split("|") if p.strip()]

        # First part is date, then case number, norms, legal area, decision type
        case_number = ""
        normen = ""
        legal_area = ""
        decision_type = ""
        if len(meta_parts) >= 2:
            case_number = meta_parts[1]
        if len(meta_parts) >= 3:
            normen = meta_parts[2]
        if len(meta_parts) >= 4:
            legal_area = meta_parts[3]
        if len(meta_parts) >= 5:
            decision_type = meta_parts[4]

        # Sometimes norms span multiple lines merged into one part
        # The last part is always decision type (Beschluss/Urteil)
        # and second-to-last is legal area
        if len(meta_parts) > 5:
            decision_type = meta_parts[-1]
            legal_area = meta_parts[-2]
            normen = " ".join(meta_parts[2:-2])

        # Parse content td (second td)
        content_td = tds[1]

        # Find PDF link
        pdf_link = content_td.find("a", href=re.compile(r"/sixcms/media\.php"))
        pdf_url = ""
        title = ""
        if pdf_link:
            pdf_url = pdf_link["href"]
            if not pdf_url.startswith("http"):
                pdf_url = base_url + pdf_url
            title = pdf_link.get_text(strip=True)
            # Remove the file size info from title
            title = re.sub(r'\s*\(pdf,\s*[\d.,]+\s*[KMG]B\)\s*$', '', title)

        # Find detail page link
        detail_link = content_td.find("a", href=re.compile(r"detail\.php\?gsid="))
        detail_url = ""
        gsid = ""
        if detail_link:
            detail_url = detail_link["href"]
            m = re.search(r'gsid=([^&]+)', detail_url)
            if m:
                gsid = m.group(1)
            if not detail_url.startswith("http"):
                detail_url = base_url + "/entscheidungen/" + detail_url

        # Extract summary (text after PDF link, before MEHR link)
        summary = ""
        for text_node in content_td.stripped_strings:
            if text_node == "MEHR":
                continue
            summary = text_node
        # The summary is usually the last text before MEHR
        all_texts = list(content_td.stripped_strings)
        if len(all_texts) >= 2:
            # Skip the title (first) and MEHR (last)
            summary_parts = [t for t in all_texts[1:] if t != "MEHR"]
            summary = " ".join(summary_parts)

        # Use data-date attribute if available
        data_date = row.get("data-date", "")

        # Generate a unique doc_id from gsid or case number
        doc_id = gsid if gsid else f"{court_code}-{case_number}".replace(" ", "_").replace("/", "-")

        entries.append({
            "doc_id": doc_id,
            "court_code": court_code,
            "court": court_name,
            "case_number": case_number,
            "date_str": date_str,
            "data_date": data_date,
            "decision_type": decision_type,
            "legal_area": legal_area,
            "normen": normen,
            "title": title,
            "summary": summary,
            "pdf_url": pdf_url,
            "detail_url": detail_url,
        })

    return entries


def fetch_overview_pages(session: requests.Session, base_url: str, overview_path: str,
                         court_code: str, court_name: str, limit: int = None) -> Iterator[Dict]:
    """Paginate through a court's overview and yield entry metadata."""
    total = get_total_entries(session, base_url, overview_path)
    print(f"  {court_code}: {total} entries")
    if total == 0:
        return

    skip = 0
    page_size = 100
    count = 0

    while skip < total:
        url = f"{base_url}{overview_path}?skip={skip}&max={page_size}"
        time.sleep(RATE_LIMIT_DELAY)
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code != 200:
                print(f"  Error on page skip={skip}: {resp.status_code}")
                skip += page_size
                continue
        except Exception as e:
            print(f"  Error on page skip={skip}: {e}")
            skip += page_size
            continue

        entries = parse_overview_page(resp.text, base_url, court_code, court_name)
        if not entries:
            break

        for entry in entries:
            yield entry
            count += 1
            if limit and count >= limit:
                return

        skip += page_size
        if skip % 500 == 0:
            print(f"    {court_code}: processed {count} entries so far...")


def extract_pdf_text(session: requests.Session, pdf_url: str, retries: int = 3) -> Optional[str]:
    """Download a PDF and extract text using pdfplumber."""
    for attempt in range(retries):
        time.sleep(RATE_LIMIT_DELAY)
        try:
            resp = session.get(pdf_url, timeout=60)
            if resp.status_code == 200:
                pdf = pdfplumber.open(io.BytesIO(resp.content))
                text_parts = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        text_parts.append(t)
                pdf.close()
                text = "\n\n".join(text_parts)
                # Clean up text
                text = re.sub(r' +', ' ', text)
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = text.strip()
                return text
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code >= 500:
                print(f"  Server error {resp.status_code} for PDF, retrying...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"  HTTP {resp.status_code} for PDF: {pdf_url}")
                return None
        except requests.exceptions.Timeout:
            print(f"  Timeout downloading PDF, attempt {attempt + 1}/{retries}")
            time.sleep(5)
        except Exception as e:
            print(f"  Error extracting PDF: {e}")
            time.sleep(5)
    return None


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
    date = parse_german_date(raw.get("date_str", ""))
    if not date and raw.get("data_date"):
        date = raw["data_date"]

    title = raw.get("title", "")
    if not title:
        parts = []
        if raw.get("court"):
            parts.append(raw["court"])
        if raw.get("case_number"):
            parts.append(raw["case_number"])
        if raw.get("decision_type"):
            parts.append(raw["decision_type"])
        if raw.get("date_str"):
            parts.append(raw["date_str"])
        title = " - ".join(parts) if parts else f"Entscheidung {raw.get('doc_id', 'unknown')}"

    return {
        "_id": f"HB-{raw.get('court_code', 'XX')}-{raw.get('doc_id', '')}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("detail_url") or raw.get("pdf_url", ""),
        "doc_id": raw.get("doc_id", ""),
        "court": raw.get("court", ""),
        "court_code": raw.get("court_code", ""),
        "case_number": raw.get("case_number", ""),
        "decision_type": raw.get("decision_type", ""),
        "legal_area": raw.get("legal_area", ""),
        "normen": raw.get("normen", ""),
        "summary": raw.get("summary", ""),
        "jurisdiction": "Bremen",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Yield all decisions from all Bremen courts."""
    session = get_session()
    count = 0
    errors = 0

    for court_code, court_name, base_url, overview_path, cms_prefix in COURTS:
        print(f"\nProcessing {court_name}...")
        court_count = 0

        for entry in fetch_overview_pages(session, base_url, overview_path,
                                          court_code, court_name, limit=limit):
            if not entry.get("pdf_url"):
                errors += 1
                continue

            text = extract_pdf_text(session, entry["pdf_url"])
            if not text or len(text) < 100:
                errors += 1
                continue

            entry["text"] = text
            record = normalize(entry)
            yield record
            count += 1
            court_count += 1

            if limit and count >= limit:
                print(f"Reached limit of {limit}")
                return

        print(f"  {court_code}: fetched {court_count} decisions")

    print(f"\nTotal: {count} decisions with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records spread across all courts."""
    session = get_session()
    samples = []
    # Distribute samples across courts
    per_court = max(3, count // len(COURTS))

    for court_code, court_name, base_url, overview_path, cms_prefix in COURTS:
        print(f"\nSampling {court_name}...")
        court_samples = 0

        # Get entries from the first page
        entries = list(fetch_overview_pages(session, base_url, overview_path,
                                           court_code, court_name, limit=per_court + 5))
        if not entries:
            print(f"  No entries found for {court_code}")
            continue

        for entry in entries:
            if court_samples >= per_court or len(samples) >= count:
                break
            if not entry.get("pdf_url"):
                continue

            print(f"  Fetching PDF for {entry.get('case_number', 'unknown')}...")
            text = extract_pdf_text(session, entry["pdf_url"])
            if not text or len(text) < 100:
                print(f"    Skipping: text too short ({len(text) if text else 0} chars)")
                continue

            entry["text"] = text
            record = normalize(entry)
            samples.append(record)
            court_samples += 1
            print(f"    Sample {len(samples)}: {len(text):,} chars - {court_code} {entry.get('case_number', '')}")

        if len(samples) >= count:
            break

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
    print(f"\nSaved {len(samples)} samples to {SAMPLE_DIR}")


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
    for court in sorted(courts):
        print(f"  - {court}")
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
            print("Fetching sample records from Bremen courts...")
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
        print(f"\n{SOURCE_ID} Status:")
        total = 0
        for court_code, court_name, base_url, overview_path, _ in COURTS:
            n = get_total_entries(session, base_url, overview_path)
            print(f"  {court_code} ({court_name}): {n} entries")
            total += n
        print(f"  Total: ~{total:,} decisions")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
