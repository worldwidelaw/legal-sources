#!/usr/bin/env python3
"""
UN/OHCHR-Juris - OHCHR Treaty Body Jurisprudence Database Fetcher

Fetches decisions of 8 UN treaty bodies on individual human rights complaints.
Full text is downloaded from docstore.ohchr.org as Word documents with text extraction.

Data source: https://juris.ohchr.org/
Method: Sequential ID iteration on prerendered case detail pages + docstore download
License: United Nations / OHCHR
Rate limit: ~2 seconds between requests

Treaty bodies covered: CAT, CCPR, CEDAW, CERD, CRC, CMW, CRPD, CED

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (~4000+ decisions)
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from html import unescape
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional
from xml.etree import ElementTree

import requests

SOURCE_ID = "UN/OHCHR-Juris"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://juris.ohchr.org"
MAX_ID = 4200  # Upper bound for case IDs

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY = 2  # seconds between requests

# Treaty body code to name mapping
TREATY_BODIES = {
    "CAT": "Committee against Torture",
    "CCPR": "Human Rights Committee",
    "CEDAW": "Committee on the Elimination of Discrimination against Women",
    "CERD": "Committee on the Elimination of Racial Discrimination",
    "CRC": "Committee on the Rights of the Child",
    "CMW": "Committee on Migrant Workers",
    "CRPD": "Committee on the Rights of Persons with Disabilities",
    "CED": "Committee on Enforced Disappearances",
}


def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code >= 500:
                print(f"  Server error {resp.status_code}, retrying...")
                time.sleep(DELAY * 2)
                continue
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt+1}): {e}")
            time.sleep(DELAY)
    return None


def fetch_binary(url: str, session: requests.Session) -> Optional[bytes]:
    """Fetch binary content (document files)."""
    for attempt in range(2):
        try:
            resp = session.get(url, headers=HEADERS, timeout=60, allow_redirects=True)
            if resp.status_code == 200:
                return resp.content
            return None
        except requests.RequestException as e:
            print(f"  Download error (attempt {attempt+1}): {e}")
            time.sleep(DELAY)
    return None


def extract_text_from_docx(data: bytes) -> str:
    """Extract text from DOCX bytes without external dependencies."""
    try:
        with zipfile.ZipFile(BytesIO(data)) as zf:
            if "word/document.xml" not in zf.namelist():
                return ""
            xml_content = zf.read("word/document.xml")
            tree = ElementTree.fromstring(xml_content)
            # Extract all text from w:t elements
            ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
            paragraphs = []
            for para in tree.iter("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p"):
                texts = []
                for t in para.iter("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t"):
                    if t.text:
                        texts.append(t.text)
                if texts:
                    paragraphs.append("".join(texts))
            return "\n\n".join(paragraphs)
    except Exception as e:
        print(f"  DOCX extraction error: {e}")
        return ""


def extract_text_from_html_doc(data: bytes) -> str:
    """Extract text from HTML document content."""
    try:
        text = data.decode("utf-8", errors="replace")
        # Strip HTML tags
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
    except Exception as e:
        print(f"  HTML extraction error: {e}")
        return ""


def parse_case_page(html: str, case_id: int) -> Optional[dict]:
    """Parse a case detail page and extract metadata."""
    # Check if it's a valid case (error pages are smaller)
    if len(html) < 10000:
        return None
    if "Oops" in html and "error" in html.lower():
        return None

    data = {"case_id": case_id}

    # Title
    m = re.search(r'<h1[^>]*class="[^"]*un-style-page-title[^"]*"[^>]*>(.*?)</h1>', html, re.DOTALL)
    if m:
        data["title"] = unescape(re.sub(r'<[^>]+>', '', m.group(1)).strip())

    # Symbol
    m = re.search(r'un-style-detail-symbol.*?<strong>(.*?)</strong>', html, re.DOTALL)
    if m:
        data["symbol"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    # Extract detail boxes
    boxes = re.findall(
        r'un-style-detail-label[^>]*>(.*?)</p>\s*(?:<p[^>]*>(.*?)</p>|</div>)',
        html, re.DOTALL
    )
    for label, value in boxes:
        label_clean = re.sub(r'<[^>]+>', '', label).strip().rstrip(':').strip()
        value_clean = re.sub(r'<[^>]+>', '', value).strip().rstrip(':').strip()
        label_lower = label_clean.lower().replace('\xa0', ' ')

        if 'communication number' in label_lower:
            data["communication_number"] = value_clean
        elif 'author' in label_lower:
            data["author"] = value_clean
        elif 'type of decision' in label_lower:
            data["decision_type"] = value_clean
        elif 'session' in label_lower:
            data["session"] = value_clean
        elif 'country' in label_lower:
            data["country"] = value_clean
        elif 'submission date' in label_lower:
            data["submission_date"] = value_clean
        elif 'date of decision' in label_lower:
            data["decision_date"] = value_clean

    # Substantive issues
    issues = re.findall(
        r'Substantive issues:.*?<ul[^>]*>(.*?)</ul>',
        html, re.DOTALL
    )
    if issues:
        items = re.findall(r'<li>(.*?)</li>', issues[0])
        data["substantive_issues"] = [re.sub(r'<[^>]+>', '', i).strip() for i in items]

    # Substantive articles
    articles = re.findall(
        r'Substantive articles:.*?<ul[^>]*>(.*?)</ul>',
        html, re.DOTALL
    )
    if articles:
        items = re.findall(r'<li>(.*?)</li>', articles[0])
        data["substantive_articles"] = [re.sub(r'<[^>]+>', '', i).strip() for i in items]

    # Extract English document links (from the table)
    # Find table rows
    doc_links = {}
    rows = re.findall(r'<tr>\s*<td>(.*?)</td>(.*?)</tr>', html, re.DOTALL)
    for lang_cell, links_cell in rows:
        lang = re.sub(r'<[^>]+>', '', lang_cell).strip()
        if lang.lower() == "english":
            # Extract links by column order: Doc, Docx, Pdf, html
            hrefs = re.findall(r'href="(https://docstore[^"]+)"', links_cell)
            formats = ["doc", "docx", "pdf", "html"]
            for i, href in enumerate(hrefs):
                if i < len(formats):
                    doc_links[formats[i]] = href
            break

    data["doc_links"] = doc_links

    # Determine treaty body from symbol
    symbol = data.get("symbol", "")
    for code in TREATY_BODIES:
        if symbol.startswith(code + "/"):
            data["treaty_body"] = code
            data["treaty_body_name"] = TREATY_BODIES[code]
            break

    return data


def fetch_full_text(doc_links: dict, session: requests.Session) -> str:
    """Download and extract full text from document links. Try HTML first, then DOCX."""
    # Try HTML first (cleanest extraction)
    if "html" in doc_links:
        print("    Downloading HTML document...")
        data = fetch_binary(doc_links["html"], session)
        if data:
            text = extract_text_from_html_doc(data)
            if len(text) > 100:
                return text
        time.sleep(DELAY)

    # Try DOCX (good extraction without external deps)
    if "docx" in doc_links:
        print("    Downloading DOCX document...")
        data = fetch_binary(doc_links["docx"], session)
        if data:
            text = extract_text_from_docx(data)
            if len(text) > 100:
                return text
        time.sleep(DELAY)

    return ""


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string to ISO format."""
    if not date_str:
        return None
    for fmt in ["%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    date = parse_date(raw.get("decision_date", ""))

    return {
        "_id": f"UN/OHCHR-Juris/{raw.get('case_id', '')}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "symbol": raw.get("symbol", ""),
        "communication_number": raw.get("communication_number", ""),
        "author": raw.get("author", ""),
        "treaty_body": raw.get("treaty_body", ""),
        "treaty_body_name": raw.get("treaty_body_name", ""),
        "decision_type": raw.get("decision_type", ""),
        "session": raw.get("session", ""),
        "country": raw.get("country", ""),
        "date": date,
        "submission_date": parse_date(raw.get("submission_date", "")),
        "substantive_issues": raw.get("substantive_issues", []),
        "substantive_articles": raw.get("substantive_articles", []),
        "text": raw.get("text", ""),
        "url": f"{BASE_URL}/casedetails/{raw.get('case_id', '')}/en-US",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents with full text."""
    session = requests.Session()

    if sample:
        # Fetch a spread of cases across the ID range
        test_ids = [1, 5, 10, 50, 100, 200, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4100]
    else:
        test_ids = range(1, MAX_ID + 1)

    count = 0
    for case_id in test_ids:
        print(f"  [{case_id}] Fetching case detail...")
        url = f"{BASE_URL}/casedetails/{case_id}/en-US"
        html = fetch_page(url, session)
        time.sleep(DELAY)

        if not html:
            continue

        raw = parse_case_page(html, case_id)
        if not raw:
            print(f"    Skipped (invalid/empty)")
            continue

        print(f"    {raw.get('title', '?')} | {raw.get('symbol', '?')}")

        # Fetch full text
        doc_links = raw.get("doc_links", {})
        if doc_links:
            text = fetch_full_text(doc_links, session)
            raw["text"] = text
            if text:
                print(f"    Full text: {len(text)} chars")
            else:
                print(f"    Warning: could not extract full text")
            time.sleep(DELAY)
        else:
            print(f"    No document links found")

        record = normalize(raw)
        if record.get("text"):
            yield record
            count += 1

        if sample and count >= 15:
            break


def save_sample(records: list):
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for rec in records:
        case_id = rec["_id"].split("/")[-1]
        path = SAMPLE_DIR / f"case_{case_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_connectivity():
    """Test that we can reach OHCHR Juris."""
    session = requests.Session()
    url = f"{BASE_URL}/casedetails/1/en-US"
    print(f"Testing: {url}")
    html = fetch_page(url, session)
    if html and "un-style-page-title" in html:
        print("OK: OHCHR Juris is reachable and returning case details")
        return True
    else:
        print("FAIL: Could not reach OHCHR Juris or unexpected response")
        return False


def main():
    parser = argparse.ArgumentParser(description="UN/OHCHR-Juris bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    if args.command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        records = []
        for record in fetch_all(sample=args.sample):
            records.append(record)

        if args.sample:
            save_sample(records)
        else:
            out_path = Path(__file__).parent / "data.jsonl"
            with open(out_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            print(f"Saved {len(records)} records to {out_path}")

        texts = [r for r in records if r.get("text")]
        avg_len = sum(len(r["text"]) for r in texts) / len(texts) if texts else 0
        print(f"\nSummary: {len(records)} records, {len(texts)} with text, avg text length: {avg_len:.0f} chars")


if __name__ == "__main__":
    main()
