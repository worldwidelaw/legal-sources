#!/usr/bin/env python3
"""
INTL/CISGOnline - CISG-online Case Law Fetcher

Fetches decisions applying the UN Convention on Contracts for the
International Sale of Goods (CISG) from cisg-online.org.
~7,600+ cases from courts and arbitral tribunals worldwide.

Data source: https://cisg-online.org/
Method: CFC search endpoint for case listing + HTML detail page scraping + PDF text extraction
License: Free access (pro bono project, no registration required)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


BASE_URL = "https://cisg-online.org"
SEARCH_URL = f"{BASE_URL}/cfc/SearchCase.cfc"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/CISGOnline"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

RATE_LIMIT_DELAY = 2.0


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="INTL/CISGOnline",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def get_all_case_ids(session: requests.Session, limit: int = 0) -> list:
    """Fetch all case IDs from the CFC search endpoint.

    Returns list of dicts with keys: case_id, cisg_number, title
    """
    # Use a large number to get all cases; the endpoint returns HTML with case links
    n = limit if limit > 0 else 10000
    url = f"{SEARCH_URL}?method=searchForCase&searchLastFive={n}&isAdmin=0"
    print(f"Fetching case listing (up to {n} cases)...")

    resp = session.get(url, headers=HEADERS, timeout=120)
    resp.raise_for_status()

    data = resp.json()
    if not data.get("SUCCESS"):
        raise RuntimeError("Search endpoint returned failure")

    html = data.get("RESULT", "")
    soup = BeautifulSoup(html, "html.parser")

    cases = []
    # Links are like: href="/search-for-cases?caseId=15745"
    for link in soup.find_all("a", href=re.compile(r"caseId=\d+")):
        href = link.get("href", "")
        m = re.search(r"caseId=(\d+)", href)
        if m:
            case_id = int(m.group(1))
            # The link text typically contains the case title/description
            title = link.get_text(strip=True)
            # Try to extract CISG-online number from nearby text
            cisg_num = ""
            parent = link.find_parent("tr") or link.find_parent("div")
            if parent:
                parent_text = parent.get_text()
                num_match = re.search(r"CISG-online\s*(?:No\.?\s*)?(\d+)", parent_text)
                if num_match:
                    cisg_num = num_match.group(1)

            cases.append({
                "case_id": case_id,
                "cisg_number": cisg_num,
                "title": title,
            })

    # Deduplicate by case_id
    seen = set()
    unique = []
    for c in cases:
        if c["case_id"] not in seen:
            seen.add(c["case_id"])
            unique.append(c)

    print(f"Found {len(unique)} unique cases")
    return unique


def _extract_field(section, label: str) -> str:
    """Extract a field value from a section by its label text."""
    if not section:
        return ""
    # Find the label element, then get the next sibling's text
    for el in section.find_all(string=re.compile(re.escape(label), re.I)):
        parent = el.find_parent("div")
        if parent:
            # The value is typically in a sibling div
            next_div = parent.find_next_sibling("div")
            if next_div:
                return next_div.get_text(strip=True)
            # Or the label and value are in the same parent's children
            grandparent = parent.find_parent("div")
            if grandparent:
                children = grandparent.find_all("div", recursive=False)
                for i, child in enumerate(children):
                    if label.lower() in child.get_text(strip=True).lower():
                        if i + 1 < len(children):
                            return children[i + 1].get_text(strip=True)
    return ""


def scrape_case_detail(session: requests.Session, case_id: int) -> dict:
    """Scrape a single case detail page for metadata and PDF links."""
    url = f"{BASE_URL}/search-for-cases?caseId={case_id}"
    resp = session.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    info = {
        "case_id": case_id,
        "url": url,
        "cisg_online_number": "",
        "title": "",
        "jurisdiction": "",
        "court": "",
        "chamber": "",
        "date": "",
        "case_number": "",
        "ecli": "",
        "seller_country": "",
        "buyer_country": "",
        "goods": "",
        "cisg_articles": [],
        "clout_number": "",
        "full_text_url": "",
        "abstract_url": "",
        "abstract_text": "",
    }

    # Extract from general-information-rows section
    gen_info = soup.find("div", class_="general-information-rows")
    if gen_info:
        # Parse the structured rows - each row is a pair of divs (label, value)
        # The text runs as: "CISG-online number7797Case nameHotel..."
        # We need to extract by finding specific label patterns
        gen_text = gen_info.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in gen_text.split("\n") if l.strip()]

        field_map = {}
        i = 0
        while i < len(lines):
            key = lines[i].lower()
            if i + 1 < len(lines):
                val = lines[i + 1]
                # Match known labels
                if "cisg-online number" in key:
                    field_map["cisg_number"] = val
                    i += 2
                    continue
                elif key == "case name":
                    field_map["case_name"] = val
                    i += 2
                    continue
                elif key == "jurisdiction":
                    field_map["jurisdiction"] = val
                    i += 2
                    continue
                elif key == "court":
                    field_map["court"] = val
                    i += 2
                    continue
                elif key == "chamber":
                    field_map["chamber"] = val
                    i += 2
                    continue
                elif "date of decision" in key:
                    field_map["date"] = val
                    i += 2
                    continue
                elif "case nr" in key or "docket" in key:
                    field_map["case_number"] = val
                    i += 2
                    continue
                elif key == "ecli":
                    field_map["ecli"] = val
                    i += 2
                    continue
                elif "clout" in key:
                    field_map["clout"] = val
                    i += 2
                    continue
            i += 1

        info["cisg_online_number"] = field_map.get("cisg_number", "")
        info["title"] = field_map.get("case_name", "")
        info["jurisdiction"] = field_map.get("jurisdiction", "")
        info["court"] = field_map.get("court", "")
        info["chamber"] = field_map.get("chamber", "")
        info["date"] = _parse_date(field_map.get("date", ""))
        info["case_number"] = field_map.get("case_number", "")
        info["ecli"] = field_map.get("ecli", "")
        clout = field_map.get("clout", "")
        m = re.search(r"(\d+)", clout)
        if m:
            info["clout_number"] = m.group(1)

    # Extract from contract-information-rows
    contract_info = soup.find("div", class_="contract-information-rows")
    if contract_info:
        contract_text = contract_info.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in contract_text.split("\n") if l.strip()]
        # Extract seller/buyer countries and goods
        for i, line in enumerate(lines):
            lower = line.lower()
            if "seller" in lower and "place of business" not in lower:
                # Next "Place of business" value is seller country
                for j in range(i + 1, min(i + 5, len(lines))):
                    if "place of business" in lines[j].lower() and j + 1 < len(lines):
                        info["seller_country"] = lines[j + 1]
                        break
            elif "buyer" in lower and "place of business" not in lower:
                for j in range(i + 1, min(i + 5, len(lines))):
                    if "place of business" in lines[j].lower() and j + 1 < len(lines):
                        info["buyer_country"] = lines[j + 1]
                        break
            elif "goods as per contract" in lower and i + 1 < len(lines):
                info["goods"] = lines[i + 1]
            elif "category of goods" in lower and i + 1 < len(lines) and not info["goods"]:
                info["goods"] = lines[i + 1]

    # Extract CISG articles from decision-information-rows
    decision_info = soup.find("div", class_="decision-information-rows")
    if decision_info:
        decision_text = decision_info.get_text()
        articles = re.findall(r"Art\.?\s*(\d+)", decision_text)
        info["cisg_articles"] = sorted(set(articles), key=int)

    # Find PDF links
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "/fullTextFile/" in href or "/fulltextfile/" in href.lower():
            full_url = href if href.startswith("http") else BASE_URL + href
            # Clean up relative paths like /../files/...
            full_url = full_url.replace("/../", "/")
            info["full_text_url"] = full_url
        elif "/abstractsFile/" in href or "/abstractsfile/" in href.lower():
            if not info["abstract_url"]:
                full_url = href if href.startswith("http") else BASE_URL + href
                full_url = full_url.replace("/../", "/")
                info["abstract_url"] = full_url

    # Extract inline abstract text from comment-rows
    comment_section = soup.find("div", class_="comment-rows")
    if comment_section:
        # Look for abstract text (not just PDF link text)
        abstract_div = comment_section.find(string=re.compile(r"Abstract", re.I))
        if abstract_div:
            parent = abstract_div.find_parent("div")
            if parent:
                next_div = parent.find_next_sibling("div")
                if next_div:
                    text = next_div.get_text(strip=True)
                    if len(text) > 50 and not text.startswith("Original language"):
                        info["abstract_text"] = text

    return info


def _parse_date(date_str: str) -> str:
    """Parse various date formats to ISO 8601."""
    date_str = date_str.strip()

    # Try common formats
    for fmt in [
        "%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d",
        "%d %B %Y", "%d %b %Y", "%B %d, %Y",
        "%m/%d/%Y",
    ]:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try to extract date with regex
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return m.group(0)

    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", date_str)
    if m:
        day, month, year = m.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    return date_str


def download_pdf_text(session: requests.Session, url: str) -> str:
    """Download a PDF and extract its text content."""
    if not url:
        return ""

    try:
        resp = session.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()

        if resp.headers.get("Content-Type", "").startswith("application/pdf") or url.endswith(".pdf"):
            return extract_text_from_pdf(resp.content)
        else:
            # Maybe it's HTML
            return ""
    except Exception as e:
        print(f"    PDF download failed for {url}: {e}")
        return ""


def normalize(raw: dict, text: str) -> dict:
    """Normalize a case record to the standard schema."""
    cisg_num = raw.get("cisg_online_number", "")

    title_parts = []
    if raw.get("jurisdiction"):
        title_parts.append(raw["jurisdiction"])
    if raw.get("court"):
        title_parts.append(raw["court"])
    if raw.get("date"):
        title_parts.append(raw["date"])
    if cisg_num:
        title_parts.append(f"CISG-online {cisg_num}")

    title = raw.get("title", "") or " - ".join(title_parts)

    return {
        "_id": f"cisg-online-{cisg_num}" if cisg_num else f"cisg-online-id-{raw['case_id']}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": raw.get("date", ""),
        "url": raw.get("url", ""),
        "cisg_online_number": cisg_num,
        "jurisdiction": raw.get("jurisdiction", ""),
        "court": raw.get("court", ""),
        "chamber": raw.get("chamber", ""),
        "case_number": raw.get("case_number", ""),
        "ecli": raw.get("ecli", ""),
        "claimant": raw.get("claimant", ""),
        "respondent": raw.get("respondent", ""),
        "seller_country": raw.get("seller_country", ""),
        "buyer_country": raw.get("buyer_country", ""),
        "goods": raw.get("goods", ""),
        "cisg_articles": raw.get("cisg_articles", []),
        "clout_number": raw.get("clout_number", ""),
    }


def fetch_cases(session: requests.Session, case_list: list, sample: bool = False) -> Generator:
    """Fetch and yield normalized case records with full text."""
    limit = 15 if sample else len(case_list)
    count = 0
    text_count = 0

    for i, case_info in enumerate(case_list[:limit]):
        case_id = case_info["case_id"]
        print(f"\n[{i+1}/{limit}] Fetching case {case_id} (CISG-online {case_info.get('cisg_number', '?')})...")

        try:
            detail = scrape_case_detail(session, case_id)
            time.sleep(RATE_LIMIT_DELAY)

            # Try to get full text from PDF
            text = ""
            if detail.get("full_text_url"):
                print(f"  Downloading full text PDF...")
                text = download_pdf_text(session, detail["full_text_url"])
                time.sleep(RATE_LIMIT_DELAY)

            # Fall back to abstract if no full text
            if not text and detail.get("abstract_url"):
                print(f"  No full text PDF, trying abstract PDF...")
                text = download_pdf_text(session, detail["abstract_url"])
                time.sleep(RATE_LIMIT_DELAY)

            # Fall back to inline abstract text
            if not text and detail.get("abstract_text"):
                text = detail["abstract_text"]

            if text:
                text_count += 1
                print(f"  Got text ({len(text)} chars)")
            else:
                print(f"  WARNING: No text found for this case")

            record = normalize(detail, text)
            count += 1
            yield record

        except Exception as e:
            print(f"  ERROR scraping case {case_id}: {e}")
            continue

    print(f"\nFetched {count} cases, {text_count} with text ({text_count*100//max(count,1)}%)")


def test_connectivity():
    """Test basic connectivity to CISG-online."""
    session = requests.Session()

    print("Testing CISG-online connectivity...")

    # Test main page
    resp = session.get(BASE_URL, headers=HEADERS, timeout=30)
    print(f"  Main page: {resp.status_code}")

    # Test search endpoint with small number
    url = f"{SEARCH_URL}?method=searchForCase&searchLastFive=5&isAdmin=0"
    resp = session.get(url, headers=HEADERS, timeout=30)
    print(f"  Search endpoint: {resp.status_code}")
    data = resp.json()
    print(f"  Search success: {data.get('SUCCESS')}")

    # Parse a few case IDs
    html = data.get("RESULT", "")
    ids = re.findall(r"caseId=(\d+)", html)
    print(f"  Found {len(ids)} case IDs in search result")

    if ids:
        # Test a case detail page
        test_id = ids[0]
        resp = session.get(f"{BASE_URL}/search-for-cases?caseId={test_id}", headers=HEADERS, timeout=30)
        print(f"  Case detail page ({test_id}): {resp.status_code}")

    print("Connectivity test PASSED")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    session = requests.Session()
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"

    # Get case listing
    case_list = get_all_case_ids(session, limit=20 if sample else 0)

    if not case_list:
        print("ERROR: No cases found")
        sys.exit(1)

    if sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        saved = 0
        for record in fetch_cases(session, case_list, sample=True):
            doc_id = record["_id"]
            safe_name = re.sub(r'[^\w\-]', '_', doc_id)
            out_path = SAMPLE_DIR / f"{safe_name}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            saved += 1
            print(f"  Saved: {out_path.name}")
        print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")
    else:
        data_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = data_dir / "records.jsonl"
        saved = 0
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for record in fetch_cases(session, case_list, sample=False):
                line = json.dumps(record, ensure_ascii=False, default=str)
                f.write(line + "\n")
                saved += 1
                if saved % 100 == 0:
                    print(f"  Saved {saved} records...")
                    f.flush()
        print(f"\nBootstrap complete: {saved} records saved to {jsonl_path}")


def main():
    parser = argparse.ArgumentParser(description="CISG-online Case Law Fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample and not args.full)
    elif args.command == "bootstrap-fast":
        bootstrap(sample=False)


if __name__ == "__main__":
    main()
