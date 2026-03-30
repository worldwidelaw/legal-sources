#!/usr/bin/env python3
"""
North Macedonia Judicial Portal - Supreme Court Practice Data Fetcher

Fetches case law from the Supreme Court of North Macedonia (Врховен суд)
via the IBM Domino XPages database at vrhoven.sud.mk.

Data source: http://vrhoven.sud.mk/VSUD/MatraVSUD.nsf
- Court practice (sentencies, legal positions, opinions)
- Civil, criminal, and administrative law areas
- Full text in HTML documents via ?OpenDocument
- Macedonian language, no authentication required

Approach:
1. Search with broad legal terms via XPages FT search
2. Paginate through results using XPages POST mechanism
3. Collect unique document UNIDs
4. Fetch each document via Domino ?OpenDocument URL
5. Parse HTML for metadata and full text
"""

import html as html_module
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import http.cookiejar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

# Constants
BASE_URL = "http://vrhoven.sud.mk/VSUD/MatraVSUD.nsf"
SEARCH_URL = f"{BASE_URL}/SearchResultsVS.xsp"
DOC_URL_TEMPLATE = f"{BASE_URL}/0/{{unid}}?OpenDocument"
RATE_LIMIT_DELAY = 2
DOCS_PER_PAGE = 10

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-collection)",
    "Accept": "text/html,application/xhtml+xml",
}

# Broad search terms to maximize coverage
SEARCH_TERMS = [
    "суд",          # court
    "пресуда",      # judgment
    "одлука",       # decision
    "решение",      # resolution
    "закон",        # law
    "кривичн",      # criminal
    "граѓанск",     # civil
    "управн",       # administrative
    "имот",         # property
    "штета",        # damage
    "договор",      # contract
    "работ",        # labor/work
    "казн",         # penal
    "жалба",        # appeal
    "тужба",        # lawsuit
    "право",        # right/law
    "обврск",       # obligation
    "парич",        # monetary
    "данок",        # tax
    "затвор",       # imprisonment
    "наследств",    # inheritance
    "развод",       # divorce
    "дрог",         # drugs
    "убиств",       # murder
    "крадеж",       # theft
    "измам",        # fraud
]


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.S)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.S)
    text = re.sub(r'<[^>]+>', '\n', text)
    text = html_module.unescape(text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def create_opener():
    """Create a urllib opener with cookie support."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = list(HEADERS.items())
    return opener


def search_page(opener, search_term: str) -> tuple:
    """
    Fetch the first page of search results for a given term.
    Returns (html_content, list_of_doc_ids, viewid).
    """
    url = f"{SEARCH_URL}?search={urllib.parse.quote(search_term)}"
    try:
        resp = opener.open(url, timeout=30)
        html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"  Error searching '{search_term}': {e}", file=sys.stderr)
        return "", [], None

    doc_ids = re.findall(r'Dis_PP\.xsp\?documentId=([A-F0-9]+)', html)
    viewid_m = re.search(r'name="\$\$viewid"[^>]*value="([^"]+)"', html)
    viewid = viewid_m.group(1) if viewid_m else None

    return html, doc_ids, viewid


def navigate_page(opener, search_term: str, viewid: str, action: str) -> tuple:
    """
    Navigate to next/last page using XPages POST mechanism.
    action: 'Next' or 'Last'
    Returns (html_content, list_of_doc_ids, new_viewid).
    """
    url = f"{SEARCH_URL}?search={urllib.parse.quote(search_term)}"
    pager_id = f"view:_id1:_id2:callback1:pager1__{action}"

    post_data = urllib.parse.urlencode({
        '$$viewid': viewid,
        '$$xspsubmitid': pager_id,
        '$$xspexecid': '',
        '$$xspsubmitvalue': '',
        '$$xspsubmitscroll': '0|0',
        'view:_id1': 'view:_id1',
    }).encode()

    try:
        req = urllib.request.Request(url, data=post_data)
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        resp = opener.open(req, timeout=30)
        html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"  Error navigating page: {e}", file=sys.stderr)
        return "", [], None

    doc_ids = re.findall(r'Dis_PP\.xsp\?documentId=([A-F0-9]+)', html)
    viewid_m = re.search(r'name="\$\$viewid"[^>]*value="([^"]+)"', html)
    new_viewid = viewid_m.group(1) if viewid_m else viewid

    return html, doc_ids, new_viewid


def get_total_pages(opener, search_term: str, viewid: str) -> int:
    """Navigate to last page to find total page count."""
    _, _, new_viewid = navigate_page(opener, search_term, viewid, "Last")
    if not new_viewid:
        return 1

    # Re-fetch last page to get page number from status
    url = f"{SEARCH_URL}?search={urllib.parse.quote(search_term)}"
    # The viewid from the last page response tells us the page number
    # Let's re-navigate to last and check the status
    time.sleep(RATE_LIMIT_DELAY)
    html, _, _ = navigate_page(opener, search_term, new_viewid, "Last")
    status = re.findall(r'xspStatus"[^>]*>(\d+)', html)
    if status:
        return int(status[0])
    return 1


def collect_all_doc_ids(max_per_term: Optional[int] = None) -> set:
    """
    Collect all unique document IDs by searching with multiple terms
    and paginating through results.
    """
    all_ids = set()
    opener = create_opener()

    for i, term in enumerate(SEARCH_TERMS):
        print(f"Searching term {i+1}/{len(SEARCH_TERMS)}: '{term}'...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        html, doc_ids, viewid = search_page(opener, term)
        if not doc_ids:
            print(f"  No results for '{term}'", file=sys.stderr)
            continue

        new_ids = set(doc_ids) - all_ids
        all_ids.update(doc_ids)
        print(f"  Page 1: {len(doc_ids)} docs ({len(new_ids)} new, {len(all_ids)} total)", file=sys.stderr)

        if not viewid:
            continue

        # Get total pages by going to last page
        time.sleep(RATE_LIMIT_DELAY)
        last_html, last_ids, last_viewid = navigate_page(opener, term, viewid, "Last")
        last_status = re.findall(r'xspStatus"[^>]*>(\d+)', last_html)
        total_pages = int(last_status[0]) if last_status else 1

        if last_ids:
            new_last = set(last_ids) - all_ids
            all_ids.update(last_ids)

        if total_pages <= 1:
            continue

        print(f"  Total pages: {total_pages} (~{total_pages * DOCS_PER_PAGE} docs)", file=sys.stderr)

        if max_per_term and total_pages > max_per_term:
            total_pages = max_per_term
            print(f"  Limiting to {max_per_term} pages", file=sys.stderr)

        # Go back to page 1 and paginate through sequentially
        time.sleep(RATE_LIMIT_DELAY)
        opener2 = create_opener()
        _, page1_ids, viewid2 = search_page(opener2, term)
        if not viewid2:
            continue

        current_viewid = viewid2
        for page_num in range(2, total_pages + 1):
            time.sleep(RATE_LIMIT_DELAY)
            _, page_ids, current_viewid = navigate_page(opener2, term, current_viewid, "Next")
            if not page_ids:
                print(f"  Page {page_num}: no results, stopping", file=sys.stderr)
                break
            new_page = set(page_ids) - all_ids
            all_ids.update(page_ids)
            if page_num % 10 == 0 or page_num == total_pages:
                print(f"  Page {page_num}/{total_pages}: {len(new_page)} new ({len(all_ids)} total)", file=sys.stderr)

    return all_ids


def fetch_document(unid: str) -> Optional[dict]:
    """Fetch a single document by its UNID and parse metadata + full text."""
    url = DOC_URL_TEMPLATE.format(unid=unid)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=30)
        raw_html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"  Error fetching {unid}: {e}", file=sys.stderr)
        return None

    return parse_document(raw_html, unid)


def parse_document(raw_html: str, unid: str) -> Optional[dict]:
    """Parse a Domino OpenDocument HTML page into structured data."""
    text = clean_html(raw_html)
    if not text or len(text) < 50:
        return None

    # Parse metadata fields from the structured format
    def extract_field(field_name: str) -> str:
        pattern = rf'{re.escape(field_name)}:\s*\n(.*?)(?:\n[А-Яа-яA-Za-z]|\Z)'
        m = re.search(pattern, text, re.S)
        if m:
            return m.group(1).strip()
        return ""

    title = extract_field("Наслов") or ""
    doc_type = extract_field("Вид на документ") or ""
    number = extract_field("Број") or ""
    legal_area = extract_field("Правна област") or ""
    legal_position_type = extract_field("Вид на правен став") or extract_field("Вид на правен\nстав") or ""
    date_str = extract_field("Датум на донесување") or ""
    legal_basis_group = extract_field("Група на правен основ") or ""
    legal_basis = extract_field("Правен основ") or ""
    court = extract_field("Суд од кој произлегува") or ""
    department = extract_field("Орган на судот") or ""

    # Parse date
    date_iso = None
    if date_str:
        # Format: MM/DD/YYYY or M/D/YYYY with optional time
        date_clean = re.sub(r',?\s*\d+:\d+\s*(AM|PM)?', '', date_str).strip()
        for fmt in ("%m/%d/%Y", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_clean, fmt)
                date_iso = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Extract the body text (everything after the metadata section)
    # Find the text after "Орган на судот:" field
    body_markers = ["Сентенца\n", "Образложение\n", "Барањето", "Со поднесеното",
                    "Врховниот суд", "Со решение", "По повод"]
    body_text = ""
    for marker in body_markers:
        idx = text.find(marker)
        if idx > 0:
            candidate = text[idx:].strip()
            # Remove trailing Domino form metadata
            candidate = re.sub(r'\n\s*XSP\..*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\n\s*dojo\..*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\n\s*var dojoConfig.*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\n\s*if\(!navigator.*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\n\s*function _xsp.*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\n\s*Search\.\.\.\s*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\nДома\s*>\s*.*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\nПравна поделба\s*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\nПо датум\s*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\nДизајнирано од\s*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\nПребарувај:.*$', '', candidate, flags=re.S)
            candidate = re.sub(r'\nКористете кирил.*$', '', candidate, flags=re.S)
            candidate = candidate.strip()
            if len(candidate) > len(body_text):
                body_text = candidate
                break

    # If no body markers found, try getting text after all metadata fields
    if not body_text:
        # Take everything after the last known metadata field
        last_field_end = 0
        for field in ["Орган на судот", "Суд од кој произлегува",
                       "Правен основ", "Група на правен основ",
                       "Датум на донесување"]:
            idx = text.find(field)
            if idx > last_field_end:
                # Find end of this field's value
                next_newline = text.find('\n', idx + len(field) + 2)
                if next_newline > 0:
                    # Skip the field value line too
                    value_end = text.find('\n', next_newline + 1)
                    if value_end > last_field_end:
                        last_field_end = value_end

        if last_field_end > 0:
            body_text = text[last_field_end:].strip()
            # Clean trailing noise
            body_text = re.sub(r'\n\s*XSP\..*$', '', body_text, flags=re.S)
            body_text = re.sub(r'\n\s*\$\(document\).*$', '', body_text, flags=re.S)
            body_text = re.sub(r'\nТип\s+Големина\s+Прилог.*$', '', body_text, flags=re.S)
            body_text = re.sub(r'\napplication/pdf.*$', '', body_text, flags=re.S)

    # If body_text is very short or empty, use the full cleaned text
    if len(body_text) < 50:
        body_text = text

    if not title and not body_text:
        return None

    # Build document URL
    doc_url = f"{BASE_URL}/0/{unid}?OpenDocument"

    return {
        "unid": unid,
        "title": title,
        "doc_type": doc_type,
        "number": number,
        "legal_area": legal_area,
        "legal_position_type": legal_position_type,
        "date": date_str,
        "date_iso": date_iso,
        "legal_basis_group": legal_basis_group,
        "legal_basis": legal_basis,
        "court": court,
        "department": department,
        "body_text": body_text,
        "url": doc_url,
    }


def normalize(raw: dict) -> dict:
    """Transform raw document data into standard schema."""
    doc_id = f"MK/JudicialPortal/{raw['unid']}"

    # Use the title or number as display title
    title = raw.get("title", "")
    if not title:
        title = raw.get("number", raw["unid"])

    text = raw.get("body_text", "")

    return {
        "_id": doc_id,
        "_source": "MK/JudicialPortal",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": raw.get("date_iso"),
        "url": raw.get("url", ""),
        "doc_number": raw.get("number", ""),
        "doc_type": raw.get("doc_type", ""),
        "legal_area": raw.get("legal_area", ""),
        "legal_position_type": raw.get("legal_position_type", ""),
        "legal_basis_group": raw.get("legal_basis_group", ""),
        "legal_basis": raw.get("legal_basis", ""),
        "court": raw.get("court", ""),
        "department": raw.get("department", ""),
        "language": "mk",
    }


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all documents: collect IDs then fetch each one."""
    print("Phase 1: Collecting document IDs via search...", file=sys.stderr)
    all_ids = collect_all_doc_ids()
    print(f"\nPhase 1 complete: {len(all_ids)} unique document IDs collected", file=sys.stderr)

    print("\nPhase 2: Fetching individual documents...", file=sys.stderr)
    fetched = 0
    errors = 0
    for i, unid in enumerate(sorted(all_ids)):
        if max_docs and fetched >= max_docs:
            break

        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_document(unid)
        if raw:
            record = normalize(raw)
            if record.get("text") and len(record["text"]) > 20:
                yield record
                fetched += 1
            else:
                errors += 1
        else:
            errors += 1

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(all_ids)} fetched, {fetched} valid, {errors} errors",
                  file=sys.stderr)

    print(f"\nPhase 2 complete: {fetched} documents with full text, {errors} errors",
          file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    # The Domino search doesn't support date filtering well,
    # so we fetch all and filter by date
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    for doc in fetch_all():
        if doc.get("date"):
            try:
                doc_dt = datetime.fromisoformat(doc["date"])
                if doc_dt >= since_dt:
                    yield doc
            except ValueError:
                yield doc  # Include if date can't be parsed
        else:
            yield doc


def bootstrap_sample():
    """Fetch a sample of documents for testing."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    print("Collecting sample document IDs...", file=sys.stderr)

    # Use just a couple of search terms for sample
    opener = create_opener()
    sample_ids = set()

    for term in ["суд", "пресуда", "одлука"]:
        time.sleep(RATE_LIMIT_DELAY)
        _, doc_ids, _ = search_page(opener, term)
        sample_ids.update(doc_ids)
        if len(sample_ids) >= 25:
            break

    print(f"Collected {len(sample_ids)} document IDs for sampling", file=sys.stderr)

    saved = 0
    for unid in sorted(sample_ids):
        if saved >= 15:
            break

        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_document(unid)
        if not raw:
            continue

        record = normalize(raw)
        if not record.get("text") or len(record["text"]) < 50:
            print(f"  Skipping {unid}: no/short text ({len(record.get('text', ''))} chars)",
                  file=sys.stderr)
            continue

        filename = f"{saved + 1:04d}_{unid[:16]}.json"
        filepath = sample_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  Saved {filename}: {record['title'][:60]}... ({len(record['text'])} chars)",
              file=sys.stderr)
        saved += 1

    print(f"\nSample complete: {saved} documents saved to {sample_dir}", file=sys.stderr)
    return saved


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        if "--sample" in sys.argv:
            count = bootstrap_sample()
            if count >= 10:
                print(f"SUCCESS: {count} sample documents with full text")
            else:
                print(f"WARNING: Only {count} samples (need 10+)")
                sys.exit(1)
        else:
            count = 0
            for doc in fetch_all():
                count += 1
            print(f"Fetched {count} documents total")
    else:
        print("Usage: bootstrap.py bootstrap [--sample]")
        print("  bootstrap --sample  Fetch sample data for testing")
        print("  bootstrap           Fetch all data")
