#!/usr/bin/env python3
"""
French Anti-Discrimination Authority (HALDE) Data Fetcher

Fetches HALDE decisions from the PMB library catalog at
juridique.defenseurdesdroits.fr. HALDE (Haute Autorité de Lutte contre les
Discriminations et pour l'Égalité) was merged into Défenseur des droits in 2011.

~1,900 records with full text inline in HTML.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
CATALOG_URL = "https://juridique.defenseurdesdroits.fr"
SEARCH_URL = f"{CATALOG_URL}/index.php"
SEARCH_QUERY = "HALDE"
RATE_LIMIT_DELAY = 1.5
PER_PAGE = 200
MAX_RETRIES = 3

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "WorldWideLaw/1.0 (academic research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
})


def get_csrf_token() -> str:
    """Fetch the catalog index page and extract the CSRF token."""
    resp = SESSION.get(f"{SEARCH_URL}?lvl=index", timeout=30)
    resp.raise_for_status()
    m = re.search(r'name="csrf_token"\s+value="([^"]+)"', resp.text)
    if m:
        return m.group(1)
    raise RuntimeError("Could not extract CSRF token from catalog")


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


def search_all_notices(csrf_token: str) -> list[int]:
    """Search for HALDE notices and return list of notice IDs."""
    all_ids = []

    resp = SESSION.post(
        f"{SEARCH_URL}?lvl=more_results&autolevel1=1",
        data={
            "look_TITLE": "1",
            "look_ALL": "1",
            "user_query": SEARCH_QUERY,
            "csrf_token": csrf_token,
        },
        timeout=60,
    )
    resp.raise_for_status()

    pag_match = re.search(r"(\d+)\s*-\s*(\d+)\s*/\s*(\d+)", resp.text)
    total = int(pag_match.group(3)) if pag_match else 0
    print(f"Total HALDE records found: {total}")

    ids = extract_notice_ids(resp.text)
    all_ids.extend(ids)
    print(f"Page 1: {len(ids)} IDs (total so far: {len(all_ids)})")

    if total == 0:
        return all_ids

    num_pages = (total + PER_PAGE - 1) // PER_PAGE
    for page in range(2, num_pages + 1):
        time.sleep(RATE_LIMIT_DELAY)
        for attempt in range(MAX_RETRIES):
            try:
                resp = SESSION.post(
                    f"{SEARCH_URL}?lvl=more_results",
                    data={
                        "user_query": SEARCH_QUERY,
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
        if page % 5 == 0:
            print(f"Page {page}/{num_pages}: {len(ids)} IDs (total so far: {len(all_ids)})")

    return list(set(all_ids))


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
        tr = soup.find("tr", class_=class_name)
        if tr:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                return tds[1].get_text(separator="\n", strip=True)
        return ""

    record["title"] = get_row_text("record_tit1")
    record["date"] = get_row_text("record_year")
    record["decision_number"] = get_row_text("record_code")
    record["document_type"] = get_row_text("record_tdoc")
    record["authors"] = get_row_text("record_responsabilites")
    record["text"] = get_row_text("record_resume")

    categories_text = get_row_text("record_categories")
    if categories_text:
        kw_matches = re.findall(r"\[Mots-clés\]\s*([^\[]+)", categories_text)
        record["keywords"] = [k.strip() for k in kw_matches if k.strip()]

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
    date_str = raw.get("date", "")
    normalized_date = None
    if date_str:
        for fmt in ("%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                normalized_date = dt.strftime("%Y-%m-%d") if fmt != "%Y" else f"{date_str.strip()}-01-01"
                break
            except ValueError:
                continue
        if not normalized_date:
            year_match = re.search(r"((?:19|20)\d{2})", date_str)
            if year_match:
                normalized_date = f"{year_match.group(1)}-01-01"

    decision_num = raw.get("decision_number", "")
    notice_id = raw.get("id", 0)

    return {
        "_id": f"FR/Halde/{decision_num or notice_id}",
        "_source": "FR/Halde",
        "_type": "doctrine",
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
    """Fetch all HALDE decisions from the catalog."""
    print("Fetching CSRF token...")
    csrf_token = get_csrf_token()
    print("Searching for HALDE notices...")
    notice_ids = search_all_notices(csrf_token)
    print(f"Found {len(notice_ids)} unique notice IDs")

    for i, nid in enumerate(notice_ids):
        time.sleep(RATE_LIMIT_DELAY)
        raw = fetch_notice(nid)
        if raw:
            yield normalize(raw)
        if (i + 1) % 100 == 0:
            print(f"Progress: {i + 1}/{len(notice_ids)} notices fetched")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch a small sample for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching CSRF token...")
    csrf_token = get_csrf_token()

    print("Searching for HALDE notices...")
    resp = SESSION.post(
        f"{SEARCH_URL}?lvl=more_results&autolevel1=1",
        data={
            "look_TITLE": "1",
            "look_ALL": "1",
            "user_query": SEARCH_QUERY,
            "csrf_token": csrf_token,
        },
        timeout=60,
    )
    resp.raise_for_status()

    pag_match = re.search(r"(\d+)\s*-\s*(\d+)\s*/\s*(\d+)", resp.text)
    total = int(pag_match.group(3)) if pag_match else 0
    print(f"Total HALDE records: {total}")

    notice_ids = extract_notice_ids(resp.text)
    if total > len(notice_ids):
        time.sleep(RATE_LIMIT_DELAY)
        resp2 = SESSION.post(
            f"{SEARCH_URL}?lvl=more_results",
            data={
                "user_query": SEARCH_QUERY,
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
    for nid in notice_ids[:count * 2]:
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
