#!/usr/bin/env python3
"""
French Advertising Ethics Jury (JDP/ARPP) Data Fetcher

Fetches JDP decisions from https://www.jdp-pub.org/avis/
The JDP (Jury de Déontologie Publicitaire) is the ethics body of the ARPP
(Autorité de Régulation Professionnelle de la Publicité).

~1070 decisions from 2008 to present, all publicly available.
Decisions cover advertising complaint rulings across all media types.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://www.jdp-pub.org"
AVIS_LIST_URL = f"{BASE_URL}/avis/"
RATE_LIMIT_DELAY = 1.5
MAX_PAGES = 120  # Safety limit

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (academic research; legal data collection)",
    "Accept": "text/html,application/xhtml+xml",
})

FRENCH_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
}


def parse_french_date(text: str) -> Optional[str]:
    """Parse a French date string like '12 mars 2026' to ISO format."""
    if not text:
        return None
    text = text.strip().lower()
    # Match "12 mars 2026" or "1er janvier 2020"
    m = re.search(r'(\d{1,2})(?:er)?\s+(\w+)\s+(\d{4})', text)
    if not m:
        return None
    day, month_name, year = m.groups()
    month = FRENCH_MONTHS.get(month_name)
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


def clean_text(html: str) -> str:
    """Strip HTML tags and clean text content."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Remove script/style elements
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def fetch_list_page(page: int) -> list[dict]:
    """Fetch a single page of the avis listing and extract decision links."""
    if page == 1:
        url = AVIS_LIST_URL
    else:
        url = f"{AVIS_LIST_URL}page/{page}/"

    try:
        resp = SESSION.get(url, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [WARN] Failed to fetch list page {page}: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    decisions = []

    # Find all links to individual avis pages
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/avis/" in href and href != AVIS_LIST_URL and "/page/" not in href:
            # Normalize URL
            if href.startswith("/"):
                href = BASE_URL + href
            # Skip if it's a category/tag link
            if href.startswith(BASE_URL + "/avis/") and href != BASE_URL + "/avis/":
                title = link.get_text(strip=True)
                if title and len(title) > 3:
                    decisions.append({
                        "url": href,
                        "list_title": title,
                    })

    # Deduplicate by URL
    seen = set()
    unique = []
    for d in decisions:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)
    return unique


def fetch_decision(url: str, list_title: str = "") -> Optional[dict]:
    """Fetch and parse a single JDP decision page."""
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [WARN] Failed to fetch decision {url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract title
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Extract main content first (needed for fallback metadata extraction)
    date_published = None
    # Look for "Avis publié le ..." text
    date_text = soup.find(string=re.compile(r'publi[ée]\s+le', re.IGNORECASE))
    if date_text:
        date_published = parse_french_date(date_text)

    # Also check meta tags
    if not date_published:
        meta_date = soup.find("meta", {"property": "article:published_time"})
        if meta_date and meta_date.get("content"):
            date_published = meta_date["content"][:10]

    # Also check time tags
    if not date_published:
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            date_published = time_tag["datetime"][:10]

    # Extract main content
    # WordPress typically uses entry-content or similar classes
    content_div = soup.find("div", class_=re.compile(r"entry-content|post-content|wp-block-post-content"))
    if not content_div:
        # Fallback: find the main content area
        content_div = soup.find("main") or soup.find("article")

    full_text = ""
    if content_div:
        full_text = clean_text(str(content_div))

    # If still no text, try the body minus nav/header/footer
    if not full_text or len(full_text) < 100:
        for tag in soup(["nav", "header", "footer", "aside", "script", "style"]):
            tag.decompose()
        body = soup.find("body")
        if body:
            full_text = clean_text(str(body))

    if not full_text or len(full_text) < 50:
        print(f"  [WARN] No content extracted from {url}", file=sys.stderr)
        return None

    # Extract decision number from title or body text
    decision_number = ""
    # Try title first: "Avis JDP n°648/20"
    m = re.search(r'n[°º]?\s*(\d+/\d+)', title)
    if m:
        decision_number = m.group(1)
    else:
        # Try body text: "COMPANY – 1111/26" pattern near the top
        m = re.search(r'[\s–-]\s*(\d{2,4}/\d{2,4})\b', full_text[:500])
        if m:
            decision_number = m.group(1)

    # Extract outcome from title, list_title, or body text
    outcome = ""
    # Normalize non-breaking spaces for matching
    search_text = (title + " " + list_title + " " + full_text[:1000]).lower().replace('\xa0', ' ')
    if "plainte partiellement fondée" in search_text or "plaintes partiellement fondées" in search_text:
        outcome = "plainte_partiellement_fondee"
    elif "plainte fondée" in search_text or "plaintes fondées" in search_text:
        outcome = "plainte_fondee"
    elif ("plainte rejetée" in search_text or "plaintes rejetées" in search_text
          or "plainte non fondée" in search_text or "plaintes non fondées" in search_text):
        outcome = "plainte_rejetee"

    # Extract sector from list_title or URL slug
    sector = ""
    # List title often has format: "COMPANY – Sector – Plainte fondée"
    if list_title:
        parts = re.split(r'\s*–\s*', list_title)
        if len(parts) >= 2:
            # Sector is typically the second part (medium/sector)
            sector = parts[1].strip()
            # Remove outcome text from sector if present
            sector = re.sub(r'\s*(Plainte[s]?\s+(non\s+)?fondée[s]?|Plainte[s]?\s+rejetée[s]?).*', '', sector, flags=re.IGNORECASE).strip()

    return {
        "url": url,
        "title": title,
        "decision_number": decision_number,
        "date": date_published,
        "sector": sector,
        "outcome": outcome,
        "text": full_text,
    }


def normalize(raw: dict) -> dict:
    """Transform raw decision data into standard schema."""
    # Build a unique ID
    slug = raw["url"].rstrip("/").split("/")[-1]
    doc_id = f"FR-ARPP-{slug}"

    return {
        "_id": doc_id,
        "_source": "FR/ARPP",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "decision_number": raw.get("decision_number", ""),
        "outcome": raw.get("outcome", ""),
        "sector": raw.get("sector", ""),
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all JDP decisions."""
    count = 0
    for page in range(1, MAX_PAGES + 1):
        if max_items and count >= max_items:
            return

        print(f"  Fetching list page {page}...", file=sys.stderr)
        entries = fetch_list_page(page)

        if not entries:
            print(f"  No more entries on page {page}, stopping.", file=sys.stderr)
            break

        for entry in entries:
            if max_items and count >= max_items:
                return

            time.sleep(RATE_LIMIT_DELAY)
            raw = fetch_decision(entry["url"], list_title=entry.get("list_title", ""))
            if raw:
                yield normalize(raw)
                count += 1
                if count % 10 == 0:
                    print(f"  Fetched {count} decisions...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

    print(f"  Total decisions fetched: {count}", file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch decisions published since a given date."""
    since_date = datetime.fromisoformat(since).date()

    for page in range(1, MAX_PAGES + 1):
        print(f"  Fetching list page {page} (updates since {since})...", file=sys.stderr)
        entries = fetch_list_page(page)

        if not entries:
            break

        found_old = False
        for entry in entries:
            time.sleep(RATE_LIMIT_DELAY)
            raw = fetch_decision(entry["url"], list_title=entry.get("list_title", ""))
            if raw:
                if raw.get("date"):
                    try:
                        doc_date = datetime.fromisoformat(raw["date"]).date()
                        if doc_date < since_date:
                            found_old = True
                            continue
                    except ValueError:
                        pass
                yield normalize(raw)

        if found_old:
            break

        time.sleep(RATE_LIMIT_DELAY)


def bootstrap(sample: bool = False):
    """Bootstrap the data source with sample or full data."""
    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    max_items = 15 if sample else None
    count = 0

    for record in fetch_all(max_items=max_items):
        count += 1
        filename = f"{record['_id']}.json"
        # Sanitize filename
        filename = re.sub(r'[^\w\-.]', '_', filename)
        filepath = sample_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"  [{count}] Saved: {record['title'][:80]}")

    print(f"\nBootstrap complete: {count} records saved to {sample_dir}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FR/ARPP (JDP) Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch_all", "fetch_updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample (15 records)")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since this date (ISO format)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample)
    elif args.command == "fetch_all":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "fetch_updates":
        if not args.since:
            print("Error: --since is required for fetch_updates", file=sys.stderr)
            sys.exit(1)
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))
