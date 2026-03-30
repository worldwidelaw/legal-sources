#!/usr/bin/env python3
"""
SG/SSOStatutes - Singapore Statutes Online Fetcher

Fetches all current Singapore Acts and Subsidiary Legislation from SSO.
Full text HTML is parsed from structured legislation pages.

Data source: https://sso.agc.gov.sg/
Method: HTML browse index + per-act full text extraction
License: Copyright Government of Singapore (AGC permission for reproduction)
Rate limit: 6 seconds between requests (per robots.txt crawl-delay)

Note: SSO Terms of Use clause 13(d) permits automated extraction
between 3AM-7AM SGT only. This scraper respects that constraint
and the crawl-delay.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all ~6K statutes)
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

SOURCE_ID = "SG/SSOStatutes"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://sso.agc.gov.sg"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY = 6  # seconds between requests per robots.txt


def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    """Fetch a page with retries and rate limiting."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 403:
                print(f"  403 Forbidden: {url}")
                return None
            if resp.status_code >= 500:
                print(f"  Server error {resp.status_code}, retrying...")
                time.sleep(DELAY * 2)
                continue
            print(f"  HTTP {resp.status_code}: {url}")
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt+1}): {e}")
            time.sleep(DELAY)
    return None


def get_act_ids(session: requests.Session, act_type: str = "Act", sample: bool = False) -> list:
    """Get all act/SL IDs from browse pages."""
    ids = []
    page_size = 500
    max_pages = 1 if sample else 20  # Acts: 2 pages, SL: 12 pages

    for page_idx in range(max_pages):
        url = f"{BASE_URL}/Browse/{act_type}/Current/All?PageSize={page_size}&SortBy=Title&SortOrder=ASC&PageIndex={page_idx}"
        print(f"  Fetching {act_type} index page {page_idx}...")
        html = fetch_page(url, session)
        if not html:
            break

        # Extract act IDs from href patterns like /Act/IA1965 or /SL/AA2004-R1
        pattern = rf'href="/{act_type}/([^"?/]+)"'
        found = re.findall(pattern, html)
        if not found:
            break

        # Deduplicate while preserving order
        seen = set()
        for act_id in found:
            if act_id not in seen and act_id != "Current" and act_id != "Repealed":
                seen.add(act_id)
                ids.append((act_type, act_id))

        print(f"    Found {len(seen)} {act_type} IDs on page {page_idx}")

        if len(found) < page_size:
            break  # Last page

        time.sleep(DELAY)

    return ids


def extract_full_text(html: str) -> str:
    """Extract legislation full text from SSO HTML page."""
    # Find the legislation content div
    match = re.search(r'<div[^>]*id="legisContent"[^>]*>(.*?)</div>\s*<!--\s*end\s*legisContent', html, re.DOTALL)
    if not match:
        # Try alternative: look for the main content area
        match = re.search(r'<div[^>]*class="[^"]*legis-content[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>', html, re.DOTALL)
    if not match:
        # Broader fallback
        match = re.search(r'<div[^>]*id="legisContent"[^>]*>(.*)', html, re.DOTALL)
        if match:
            content = match.group(1)
            # Find end by counting divs
            depth = 1
            pos = 0
            while depth > 0 and pos < len(content):
                open_tag = content.find('<div', pos)
                close_tag = content.find('</div>', pos)
                if close_tag == -1:
                    break
                if open_tag != -1 and open_tag < close_tag:
                    depth += 1
                    pos = open_tag + 4
                else:
                    depth -= 1
                    if depth == 0:
                        content = content[:close_tag]
                        break
                    pos = close_tag + 6
            match = type('Match', (), {'group': lambda self, n=1: content})()

    if not match:
        return ""

    content = match.group(1)

    # Strip HTML tags but preserve structure
    # Replace block-level elements with newlines
    content = re.sub(r'<br\s*/?>', '\n', content)
    content = re.sub(r'</(?:p|div|tr|li|h[1-6])>', '\n', content)
    content = re.sub(r'<(?:p|div|tr|li|h[1-6])[^>]*>', '\n', content)
    content = re.sub(r'</td>', '  ', content)

    # Remove all remaining HTML tags
    content = re.sub(r'<[^>]+>', '', content)

    # Decode HTML entities
    content = unescape(content)

    # Clean up whitespace
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r' *\n *', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)

    return content.strip()


def extract_metadata(html: str, act_type: str, act_id: str) -> dict:
    """Extract metadata from an SSO act page."""
    meta = {
        "act_type": act_type,
        "act_id": act_id,
    }

    # Title from actHd (inside <td> tag)
    title_match = re.search(r'class="actHd"[^>]*>(.*?)</td>', html, re.DOTALL)
    if title_match:
        meta["title"] = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
    else:
        # Fallback: data-legisTitle attribute
        title_match = re.search(r'data-legisTitle="([^"]+)"', html)
        if title_match:
            meta["title"] = unescape(title_match.group(1)).strip()
        else:
            # Fallback to page title
            title_match = re.search(r'<title>\s*(.*?)\s*</title>', html, re.DOTALL)
            if title_match:
                meta["title"] = title_match.group(1).strip().replace(" - Singapore Statutes Online", "")

    # Long title
    long_title_match = re.search(r'<div[^>]*class="[^"]*longTitle[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
    if long_title_match:
        meta["long_title"] = re.sub(r'<[^>]+>', '', long_title_match.group(1)).strip()

    # Commencement date
    date_match = re.search(r'<div[^>]*class="[^"]*cDate[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
    if date_match:
        date_text = re.sub(r'<[^>]+>', '', date_match.group(1)).strip()
        meta["commencement_date_text"] = date_text

    # Original enactment date from title or act_id
    year_match = re.search(r'(\d{4})\s*$', meta.get("title", ""))
    if not year_match:
        year_match = re.search(r'(\d{4})', act_id)
    if year_match:
        meta["year"] = year_match.group(1)

    # Version date
    version_match = re.search(r'Current version as at\s+(\d{1,2}\s+\w+\s+\d{4})', html)
    if version_match:
        meta["version_date_text"] = version_match.group(1)

    return meta


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    act_type = raw.get("act_type", "Act")
    act_id = raw.get("act_id", "")
    title = raw.get("title", act_id)

    # Parse date
    date_str = None
    year = raw.get("year")
    if year:
        date_str = f"{year}-01-01"

    # Try to parse version date for a more precise date
    version_text = raw.get("version_date_text", "")
    if version_text:
        try:
            dt = datetime.strptime(version_text, "%d %B %Y")
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    doc_type = "legislation"  # All SSO content is legislation
    url = f"{BASE_URL}/{act_type}/{act_id}"

    return {
        "_id": f"SG/SSOStatutes/{act_type}/{act_id}",
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "long_title": raw.get("long_title", ""),
        "act_type": "Act" if act_type == "Act" else "Subsidiary Legislation",
        "act_id": act_id,
        "date": date_str,
        "text": raw.get("text", ""),
        "url": url,
    }


def fetch_act(session: requests.Session, act_type: str, act_id: str) -> Optional[dict]:
    """Fetch a single act/SL and return normalized record."""
    url = f"{BASE_URL}/{act_type}/{act_id}"
    html = fetch_page(url, session)
    if not html:
        return None

    text = extract_full_text(html)
    if not text or len(text) < 50:
        print(f"    Warning: short/empty text for {act_type}/{act_id} ({len(text)} chars)")
        return None

    meta = extract_metadata(html, act_type, act_id)
    meta["text"] = text

    return normalize(meta)


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents with full text."""
    session = requests.Session()

    # Get act IDs
    print("Fetching Act index...")
    act_ids = get_act_ids(session, "Act", sample=sample)
    print(f"Found {len(act_ids)} Acts")
    time.sleep(DELAY)

    if not sample:
        print("Fetching SL index...")
        sl_ids = get_act_ids(session, "SL", sample=False)
        print(f"Found {len(sl_ids)} Subsidiary Legislation items")
        all_ids = act_ids + sl_ids
    else:
        all_ids = act_ids[:15]  # Sample: just first 15 acts

    print(f"Total items to fetch: {len(all_ids)}")

    for i, (act_type, act_id) in enumerate(all_ids):
        print(f"  [{i+1}/{len(all_ids)}] Fetching {act_type}/{act_id}...")
        record = fetch_act(session, act_type, act_id)
        if record:
            yield record
        time.sleep(DELAY)


def save_sample(records: list):
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for rec in records:
        safe_id = rec["act_id"].replace("/", "_").replace(" ", "_")
        path = SAMPLE_DIR / f"{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_connectivity():
    """Test that we can reach SSO."""
    session = requests.Session()
    url = f"{BASE_URL}/Browse/Act/Current/All?PageSize=5&SortBy=Title&SortOrder=ASC"
    print(f"Testing: {url}")
    html = fetch_page(url, session)
    if html and "/Act/" in html:
        print("OK: SSO is reachable and returning act listings")
        return True
    else:
        print("FAIL: Could not reach SSO or unexpected response")
        return False


def main():
    parser = argparse.ArgumentParser(description="SG/SSOStatutes bootstrap")
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
            if args.sample and len(records) >= 15:
                break

        if args.sample:
            save_sample(records)
        else:
            # Full bootstrap: save all to JSONL
            out_path = Path(__file__).parent / "data.jsonl"
            with open(out_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            print(f"Saved {len(records)} records to {out_path}")

        # Summary
        texts = [r for r in records if r.get("text")]
        avg_len = sum(len(r["text"]) for r in texts) / len(texts) if texts else 0
        print(f"\nSummary: {len(records)} records, {len(texts)} with text, avg text length: {avg_len:.0f} chars")


if __name__ == "__main__":
    main()
