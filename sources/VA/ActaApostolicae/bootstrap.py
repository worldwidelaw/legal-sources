#!/usr/bin/env python3
"""
VA/ActaApostolicae - Vatican Apostolic Documents Fetcher

Fetches papal legislative documents (encyclicals, motu proprio, apostolic
constitutions, exhortations, letters, and bulls) from vatican.va using the
Adobe Experience Manager / Apache Sling JSON API.

Data source: https://www.vatican.va/
Method: AEM/Sling JSON API (append .N.json to any URL path)
License: Holy See / Vatican
Rate limit: ~2 seconds between requests

Popes covered: Leo XIV, Francis, Benedict XVI, John Paul II, Paul VI,
               John XXIII, Pius XII (~1,900 legislative documents)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

SOURCE_ID = "VA/ActaApostolicae"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.vatican.va"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
}

DELAY = 2  # seconds between requests

# Popes with content on vatican.va (slug -> display name)
POPES = {
    "leo-xiv": "Pope Leo XIV",
    "francesco": "Pope Francis",
    "benedict-xvi": "Pope Benedict XVI",
    "john-paul-ii": "Pope John Paul II",
    "paul-vi": "Pope Paul VI",
    "john-xxiii": "Pope John XXIII",
    "pius-xii": "Pope Pius XII",
}

# Legislative document types to fetch
LEG_TYPES = {
    "encyclicals": "Encyclical",
    "motu_proprio": "Motu Proprio",
    "apost_constitutions": "Apostolic Constitution",
    "apost_exhortations": "Apostolic Exhortation",
    "apost_letters": "Apostolic Letter",
    "bulls": "Papal Bull",
}


def strip_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"</?div[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_json(url: str, session: requests.Session) -> Optional[dict]:
    """Fetch JSON with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code >= 500:
                print(f"  Server error {resp.status_code}, retrying...")
                time.sleep(DELAY * 2)
                continue
            return None
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"  Request error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def extract_text_from_jcr(jcr_content: dict) -> str:
    """Extract full text from JCR content structure."""
    container = jcr_content.get("container", {})

    # Try direct vaticanrichtext
    if "vaticanrichtext" in container and "text" in container["vaticanrichtext"]:
        return strip_html(container["vaticanrichtext"]["text"])

    # Try any key with a 'text' field in container
    for key, val in container.items():
        if isinstance(val, dict) and "text" in val and len(val["text"]) > 50:
            return strip_html(val["text"])

    # Try nested: container.X.Y.text
    for key, val in container.items():
        if isinstance(val, dict):
            for key2, val2 in val.items():
                if isinstance(val2, dict) and "text" in val2 and len(val2["text"]) > 50:
                    return strip_html(val2["text"])

    return ""


def parse_date(date_str: str) -> Optional[str]:
    """Parse Vatican date strings to ISO format."""
    if not date_str:
        return None
    # Format: "Fri Jul 16 2021 12:00:00 GMT+0200"
    match = re.match(r"\w+ (\w+) (\d+) (\d{4})", date_str)
    if match:
        months = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
        }
        mon = months.get(match.group(1), "01")
        return f"{match.group(3)}-{mon}-{match.group(2).zfill(2)}"
    # Try ISO format
    match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if match:
        return match.group(1)
    return None


def list_documents(pope: str, section: str, session: requests.Session) -> list:
    """List all document slugs in a section."""
    url = f"{BASE_URL}/content/{pope}/en/{section}/documents.1.json"
    data = fetch_json(url, session)
    if not data:
        return []
    skip_prefixes = ("jcr:", "sling:", "cq:", "rep:")
    return [k for k in data.keys() if not any(k.startswith(p) for p in skip_prefixes)]


def fetch_document(pope: str, section: str, slug: str,
                   session: requests.Session) -> Optional[dict]:
    """Fetch a single document's full content."""
    url = f"{BASE_URL}/content/{pope}/en/{section}/documents/{slug}.3.json"
    data = fetch_json(url, session)
    if not data:
        return None

    jcr = data.get("jcr:content", {})
    title = strip_html(jcr.get("jcr:title", slug))
    date_str = jcr.get("eventDate", "")
    date_iso = parse_date(date_str)
    tags = jcr.get("cq:tags", [])

    text = extract_text_from_jcr(jcr)

    # Also try abstract
    abstract_html = ""
    abstract_obj = jcr.get("abstract", {})
    if isinstance(abstract_obj, dict) and "text" in abstract_obj:
        abstract_html = abstract_obj["text"]

    abstract_text = strip_html(abstract_html)
    if abstract_text and text:
        text = abstract_text + "\n\n" + text
    elif abstract_text and not text:
        text = abstract_text

    doc_url = f"{BASE_URL}/content/{pope}/en/{section}/documents/{slug}.html"

    doc_id = f"VA-{pope}-{slug}"
    if len(doc_id) > 100:
        h = hashlib.md5(doc_id.encode()).hexdigest()[:8]
        doc_id = doc_id[:90] + "_" + h

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": doc_url,
        "pope": POPES.get(pope, pope),
        "pope_slug": pope,
        "document_type": LEG_TYPES.get(section, section),
        "section": section,
        "tags": tags if isinstance(tags, list) else [tags] if tags else [],
    }


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all legislative documents."""
    limit = 15 if sample else None
    fetched = 0
    skipped = 0

    for pope in POPES:
        for section, type_name in LEG_TYPES.items():
            slugs = list_documents(pope, section, session)
            if not slugs:
                continue

            print(f"  {pope}/{section}: {len(slugs)} documents")
            time.sleep(DELAY)

            for slug in slugs:
                time.sleep(DELAY)
                record = fetch_document(pope, section, slug, session)
                if not record:
                    skipped += 1
                    continue

                if record["text"]:
                    yield record
                    fetched += 1
                    if limit and fetched >= limit:
                        return
                else:
                    skipped += 1
                    if skipped <= 10:
                        print(f"    No text: {slug[:50]}")

    print(f"  Total: {fetched} fetched, {skipped} skipped")


def save_record(record: dict, sample_dir: Path) -> None:
    """Save a record to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
    if len(safe_id) > 80:
        h = hashlib.md5(record["_id"].encode()).hexdigest()[:8]
        safe_id = safe_id[:70] + "_" + h
    filename = safe_id + ".json"
    filepath = sample_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    """Test that the Vatican JSON API is reachable."""
    session = requests.Session()
    print("Testing Vatican JSON API connectivity...")

    # Test section listing
    url = f"{BASE_URL}/content/francesco/en.1.json"
    data = fetch_json(url, session)
    if not data:
        print("FAIL: Cannot reach Vatican JSON API")
        return False
    sections = [k for k in data.keys() if k in LEG_TYPES]
    print(f"  Sections API: OK ({len(sections)} legislative sections for Pope Francis)")

    # Test document listing
    url = f"{BASE_URL}/content/francesco/en/encyclicals/documents.1.json"
    data = fetch_json(url, session)
    if not data:
        print("FAIL: Cannot list documents")
        return False
    skip_prefixes = ("jcr:", "sling:", "cq:", "rep:")
    docs = [k for k in data.keys() if not any(k.startswith(p) for p in skip_prefixes)]
    print(f"  Document listing: OK ({len(docs)} encyclicals)")

    # Test full document fetch
    if docs:
        time.sleep(DELAY)
        url = f"{BASE_URL}/content/francesco/en/encyclicals/documents/{docs[0]}.3.json"
        data = fetch_json(url, session)
        if data:
            jcr = data.get("jcr:content", {})
            text = extract_text_from_jcr(jcr)
            print(f"  Document fetch: OK (title: {jcr.get('jcr:title', '?')[:50]}, text: {len(text)} chars)")
        else:
            print("  Document fetch: FAIL")
            return False

    print("All tests passed.")
    return True


def bootstrap(sample: bool = False) -> None:
    """Run the bootstrap process."""
    session = requests.Session()
    sample_dir = SAMPLE_DIR
    records_saved = 0

    if sample:
        if sample_dir.exists():
            for f in sample_dir.glob("*.json"):
                f.unlink()

    print(f"{'Sample' if sample else 'Full'} bootstrap starting...")

    for record in fetch_all(session, sample=sample):
        save_record(record, sample_dir)
        records_saved += 1
        if records_saved % 50 == 0:
            print(f"  Saved {records_saved} records...")

    print(f"\nBootstrap complete: {records_saved} records saved to {sample_dir}")

    if records_saved == 0:
        print("ERROR: No records saved!")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="VA/ActaApostolicae bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only ~15 sample records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
