#!/usr/bin/env python3
"""
French Rights Defender (Défenseur des droits) Data Fetcher

Fetches ombudsman decisions from the PMB library catalog at
juridique.defenseurdesdroits.fr. Covers recommendations, settlements,
court observations, reform proposals, and predecessor body decisions (HALDE, etc.).

~15,000+ records with full text inline in HTML.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
CATALOG_URL = "https://juridique.defenseurdesdroits.fr"
SEARCH_URL = f"{CATALOG_URL}/index.php"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
PER_PAGE = 200
MAX_RETRIES = 3

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (academic research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
})


def get_csrf_token() -> str:
    """Fetch the catalog index page and extract the CSRF token."""
    resp = SESSION.get(f"{SEARCH_URL}?lvl=index", timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "csrf_token"})
    if token_input and token_input.get("value"):
        return token_input["value"]
    # Try regex fallback
    m = re.search(r'name="csrf_token"\s+value="([^"]+)"', resp.text)
    if m:
        return m.group(1)
    raise RuntimeError("Could not extract CSRF token from catalog")


def search_all_notices(csrf_token: str) -> list[int]:
    """Search for all notices and return list of notice IDs."""
    all_ids = []

    # Initial search POST
    resp = SESSION.post(
        f"{SEARCH_URL}?lvl=more_results&autolevel1=1",
        data={
            "look_TITLE": "1",
            "look_ALL": "1",
            "user_query": "*",
            "csrf_token": csrf_token,
        },
        timeout=60,
    )
    resp.raise_for_status()

    # Extract total count from pagination "X - Y / TOTAL"
    pag_match = re.search(r"(\d+)\s*-\s*(\d+)\s*/\s*(\d+)", resp.text)
    total = 0
    if pag_match:
        total = int(pag_match.group(3))
    print(f"Total records found: {total}")

    # Extract IDs from first page
    ids = extract_notice_ids(resp.text)
    all_ids.extend(ids)
    print(f"Page 1: {len(ids)} IDs (total so far: {len(all_ids)})")

    if total == 0:
        return all_ids

    # Paginate
    num_pages = (total + PER_PAGE - 1) // PER_PAGE
    for page in range(2, num_pages + 1):
        time.sleep(RATE_LIMIT_DELAY)
        for attempt in range(MAX_RETRIES):
            try:
                resp = SESSION.post(
                    f"{SEARCH_URL}?lvl=more_results",
                    data={
                        "user_query": "*",
                        "mode": "tous",
                        "count": str(total),
                        "l_typdoc": "c,b,m,g,n,l,a,j,k,d,p,i,q,o",
                        "page": str(page),
                        "nb_per_page_custom": str(PER_PAGE),
                        "csrf_token": csrf_token,
                    },
                    timeout=60,
                )
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    print(f"  Retry page {page} ({e})")
                    time.sleep(3)
                else:
                    print(f"  Failed page {page}: {e}")
                    continue

        ids = extract_notice_ids(resp.text)
        all_ids.extend(ids)
        if page % 10 == 0:
            print(f"Page {page}/{num_pages}: {len(ids)} IDs (total so far: {len(all_ids)})")

    return list(set(all_ids))  # deduplicate


def extract_notice_ids(html: str) -> list[int]:
    """Extract unique notice IDs from search results HTML."""
    seen = set()
    result = []
    for m in re.findall(r"notice_display&(?:amp;)?id=(\d+)", html):
        nid = int(m)
        if nid not in seen:
            seen.add(nid)
            result.append(nid)
    return result


def fetch_notice(notice_id: int) -> Optional[dict]:
    """Fetch and parse a single notice page."""
    url = f"{SEARCH_URL}?lvl=notice_display&id={notice_id}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return parse_notice(resp.text, notice_id, url)
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
            else:
                print(f"  Failed to fetch notice {notice_id}: {e}")
                return None


def parse_notice(html: str, notice_id: int, url: str) -> Optional[dict]:
    """Parse a PMB notice HTML page into structured data."""
    soup = BeautifulSoup(html, "html.parser")

    record = {
        "id": notice_id,
        "url": url,
        "title": "",
        "date": "",
        "decision_number": "",
        "document_type": "",
        "authors": "",
        "keywords": [],
        "text": "",
    }

    def get_row_text(class_name: str) -> str:
        """Extract text from a table row by its class name."""
        tr = soup.find("tr", class_=class_name)
        if tr:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                return tds[1].get_text(separator="\n", strip=True)
        return ""

    # PMB uses <tr> elements with specific classes in a descr_notice table
    record["title"] = get_row_text("record_tit1")
    record["date"] = get_row_text("record_year")
    record["decision_number"] = get_row_text("record_code")
    record["document_type"] = get_row_text("record_tdoc")
    record["authors"] = get_row_text("record_responsabilites")
    record["text"] = get_row_text("record_resume")

    # Extract keywords from record_categories
    categories_text = get_row_text("record_categories")
    if categories_text:
        # Keywords are in [Mots-clés] sections
        kw_matches = re.findall(r"\[Mots-clés\]\s*([^\[]+)", categories_text)
        record["keywords"] = [k.strip() for k in kw_matches if k.strip()]

    # Fallback title from page title
    if not record["title"]:
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            title_text = re.sub(r"\s*[-|]\s*(Catalogue|PMB).*$", "", title_text).strip()
            if title_text:
                record["title"] = title_text

    return record


def normalize(raw: dict) -> dict:
    """Normalize raw notice data to standard schema."""
    # Parse date
    date_str = raw.get("date", "")
    normalized_date = None
    if date_str:
        # Try various date formats
        for fmt in ("%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                normalized_date = dt.strftime("%Y-%m-%d") if fmt != "%Y" else f"{date_str.strip()}-01-01"
                break
            except ValueError:
                continue
        if not normalized_date:
            # Extract year if present
            year_match = re.search(r"((?:19|20)\d{2})", date_str)
            if year_match:
                normalized_date = f"{year_match.group(1)}-01-01"

    decision_num = raw.get("decision_number", "")
    notice_id = raw.get("id", 0)

    return {
        "_id": f"FR/DefenseurDroits/{decision_num or notice_id}",
        "_source": "FR/DefenseurDroits",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": normalized_date,
        "url": raw.get("url", ""),
        "decision_number": decision_num,
        "document_type": raw.get("document_type", ""),
        "authors": raw.get("authors", ""),
        "keywords": raw.get("keywords", []),
    }


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all decisions from the catalog."""
    print("Fetching CSRF token...")
    csrf_token = get_csrf_token()
    print(f"CSRF token obtained: {csrf_token[:10]}...")

    print("Searching for all notices...")
    notice_ids = search_all_notices(csrf_token)
    print(f"Found {len(notice_ids)} unique notice IDs")

    for i, nid in enumerate(notice_ids):
        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_notice(nid)
        if raw:
            record = normalize(raw)
            if record["text"]:
                yield record
            else:
                # Still yield but warn
                print(f"  Warning: notice {nid} has no text content")
                yield record

        if (i + 1) % 100 == 0:
            print(f"Progress: {i + 1}/{len(notice_ids)} notices fetched")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    # PMB doesn't support date-based filtering easily
    # Fall back to full fetch
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch a small sample for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching CSRF token...")
    csrf_token = get_csrf_token()
    print(f"CSRF token: {csrf_token[:10]}...")

    print("Searching for notices...")
    # Initial search
    resp = SESSION.post(
        f"{SEARCH_URL}?lvl=more_results&autolevel1=1",
        data={
            "look_TITLE": "1",
            "look_ALL": "1",
            "user_query": "*",
            "csrf_token": csrf_token,
        },
        timeout=60,
    )
    resp.raise_for_status()

    # Get total from pagination
    pag_match = re.search(r"(\d+)\s*-\s*(\d+)\s*/\s*(\d+)", resp.text)
    total = int(pag_match.group(3)) if pag_match else 0
    print(f"Total records: {total}")

    # Request page with more results
    notice_ids = extract_notice_ids(resp.text)
    if total > len(notice_ids):
        time.sleep(RATE_LIMIT_DELAY)
        resp2 = SESSION.post(
            f"{SEARCH_URL}?lvl=more_results",
            data={
                "user_query": "*",
                "mode": "tous",
                "count": str(total),
                "l_typdoc": "c,b,m,g,n,l,a,j,k,d,p,i,q,o",
                "page": "1",
                "nb_per_page_custom": str(min(count * 3, 200)),
                "csrf_token": csrf_token,
            },
            timeout=60,
        )
        resp2.raise_for_status()
        notice_ids = extract_notice_ids(resp2.text)
    print(f"Found {len(notice_ids)} IDs")

    saved = 0
    no_text = 0
    for nid in notice_ids[:count * 2]:  # Try more to ensure we get enough with text
        if saved >= count:
            break

        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_notice(nid)
        if not raw:
            continue

        record = normalize(raw)
        saved += 1

        if not record["text"]:
            no_text += 1

        out_path = sample_dir / f"{nid}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"  [{saved}/{count}] Saved notice {nid}: {record['title'][:60]}... "
              f"(text: {len(record['text'])} chars)")

    print(f"\nSample complete: {saved} records saved, {no_text} without text content")
    return saved, no_text


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        source_dir = Path(__file__).parent
        sample_dir = source_dir / "sample"

        if "--sample" in sys.argv:
            saved, no_text = bootstrap_sample(sample_dir)
            if saved < 10:
                print(f"\n[ERROR] Only {saved} records saved (need 10+)")
                sys.exit(1)
            if no_text > saved // 2:
                print(f"\n[WARNING] {no_text}/{saved} records have no text content")
        else:
            print("Use --sample for sample mode")
    else:
        print("Usage: bootstrap.py bootstrap --sample")
