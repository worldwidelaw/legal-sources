#!/usr/bin/env python3
"""
TR/KVKK - Turkey Personal Data Protection Authority (Kişisel Verileri Koruma Kurumu)

Fetches KVKK board decisions, principle decisions, and decision summaries.
Three sections are scraped:
  - Kurul Karar Özetleri (Decision Summaries) — ~256 enforcement decisions
  - İlke Kararları (Principle Decisions) — ~18 policy decisions
  - Kurul Kararları (Board Decisions) — ~20 formal decisions

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all decisions
"""

import argparse
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
SOURCE_ID = "TR/KVKK"
BASE_URL = "https://www.kvkk.gov.tr"
KARAR_OZETLERI_URL = f"{BASE_URL}/Icerik/5406/kurul-karar-ozetleri"
ILKE_KARARLARI_URL = f"{BASE_URL}/Icerik/7201/ilke-kararlari"
KURUL_KARARLARI_URL = f"{BASE_URL}/Icerik/5419/kurul-kararlari"
RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    })
    return session


def parse_decision_date(text: str) -> Optional[str]:
    """Parse Turkish date formats to ISO 8601."""
    if not text:
        return None
    text = text.strip()
    # Try DD/MM/YYYY
    m = re.search(r'(\d{2})[/.](\d{2})[/.](\d{4})', text)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        try:
            return f"{year}-{month}-{day}"
        except ValueError:
            pass
    # Try YYYY
    m = re.search(r'(\d{4})', text)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def extract_decision_number(text: str) -> Optional[str]:
    """Extract decision number like 2024/1385 from text."""
    m = re.search(r'(\d{4}/\d+)', text)
    if m:
        return m.group(1)
    return None


def clean_text(html_content: str) -> str:
    """Extract clean text from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # Clean up whitespace
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def fetch_decision_page(session: requests.Session, url: str) -> Optional[dict]:
    """Fetch an individual decision page and extract content."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [WARN] Failed to fetch {url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the main content area
    content_div = soup.find("div", class_="content-detail") or \
                  soup.find("div", class_="icerik-detay") or \
                  soup.find("div", class_="col-md-9") or \
                  soup.find("article") or \
                  soup.find("div", id="content")

    if not content_div:
        # Fallback: use main tag or body
        content_div = soup.find("main") or soup.find("body")

    if not content_div:
        return None

    # Remove navigation, menus, footer
    for tag in content_div.find_all(["nav", "header", "footer", "script", "style"]):
        tag.decompose()
    # Remove breadcrumb
    for bc in content_div.find_all(class_=re.compile(r'breadcrumb|menu|sidebar|navbar')):
        bc.decompose()

    # Get title
    title_tag = content_div.find(["h1", "h2", "h3"])
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Get the full text content
    text = content_div.get_text(separator="\n")
    # Clean up
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    text = "\n".join(lines)

    # Try to extract decision number and date from content
    decision_number = extract_decision_number(text)
    date_str = None
    # Look for date patterns in text
    date_match = re.search(r'(\d{2})[/.](\d{2})[/.](\d{4})', text)
    if date_match:
        day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
        date_str = f"{year}-{month}-{day}"

    return {
        "title": title,
        "text": text,
        "decision_number": decision_number,
        "date": date_str,
    }


def get_list_page_links(session: requests.Session, list_url: str, page: int = 1) -> list[dict]:
    """Fetch a list page and extract decision links."""
    url = f"{list_url}?page={page}" if page > 1 else list_url
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [WARN] Failed to fetch list page {url}: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    decisions = []

    # Find links to individual decision pages
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        # Match KVKK internal content links with decision numbers
        if "/Icerik/" in href and href != list_url:
            # Skip non-decision navigation links
            full_url = href if href.startswith("http") else BASE_URL + href
            # Filter: only include links that look like individual decisions
            # Decision URLs typically have a numeric slug like /Icerik/8140/2024-1385
            if re.search(r'/Icerik/\d+/\d{4}-\d+', full_url):
                link_text = a_tag.get_text(strip=True)
                decisions.append({
                    "url": full_url,
                    "link_text": link_text,
                })

    # Deduplicate
    seen = set()
    unique = []
    for d in decisions:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)

    return unique


def get_max_pages(session: requests.Session, list_url: str) -> int:
    """Determine the total number of pages for a list."""
    try:
        resp = session.get(list_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return 1

    soup = BeautifulSoup(resp.text, "html.parser")
    # Look for pagination links
    max_page = 1
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        m = re.search(r'[?&]page=(\d+)', href, re.IGNORECASE)
        if m:
            page_num = int(m.group(1))
            if page_num > max_page:
                max_page = page_num
    # Also check for page text like "... 32"
    pag_text = soup.get_text()
    for m in re.finditer(r'page=(\d+)', pag_text, re.IGNORECASE):
        page_num = int(m.group(1))
        if page_num > max_page:
            max_page = page_num

    return max_page


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all KVKK decisions."""
    session = get_session()
    fetched_at = datetime.now(timezone.utc).isoformat()
    count = 0
    max_records = 15 if sample else 9999

    sections = [
        ("karar_ozetleri", KARAR_OZETLERI_URL, "Decision Summary"),
        ("ilke_kararlari", ILKE_KARARLARI_URL, "Principle Decision"),
        ("kurul_kararlari", KURUL_KARARLARI_URL, "Board Decision"),
    ]

    seen_urls = set()

    for section_id, section_url, category in sections:
        if count >= max_records:
            break

        print(f"\n--- Fetching {category} from {section_url} ---")
        max_pages = get_max_pages(session, section_url)
        if sample:
            max_pages = min(max_pages, 2)
        print(f"  Total pages: {max_pages}")

        for page in range(1, max_pages + 1):
            if count >= max_records:
                break

            print(f"  Page {page}/{max_pages}...")
            links = get_list_page_links(session, section_url, page)
            print(f"  Found {len(links)} decision links")
            time.sleep(RATE_LIMIT_DELAY)

            for link_info in links:
                if count >= max_records:
                    break

                url = link_info["url"]
                if url in seen_urls:
                    print(f"  [DUP] Skipping already-fetched: {url}")
                    continue
                seen_urls.add(url)
                print(f"  Fetching: {url}")
                decision = fetch_decision_page(session, url)
                time.sleep(RATE_LIMIT_DELAY)

                if not decision or not decision.get("text") or len(decision["text"]) < 100:
                    print(f"    [SKIP] Insufficient text content")
                    continue

                # Build normalized record
                decision_number = decision.get("decision_number") or \
                    extract_decision_number(url) or \
                    extract_decision_number(link_info.get("link_text", ""))

                title = decision.get("title") or link_info.get("link_text", "")
                if not title and decision_number:
                    title = f"KVKK Decision {decision_number}"

                record = {
                    "_id": f"TR/KVKK/{decision_number}" if decision_number else f"TR/KVKK/{url.split('/')[-2]}",
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": fetched_at,
                    "title": title,
                    "text": decision["text"],
                    "date": decision.get("date"),
                    "url": url,
                    "decision_number": decision_number,
                    "decision_category": category,
                }

                count += 1
                print(f"    [OK] #{count} — {len(decision['text'])} chars — {decision_number or 'no-num'}")
                yield record

    print(f"\n=== Total records fetched: {count} ===")


def normalize(raw: dict) -> dict:
    """Normalize a raw record (already normalized during fetch)."""
    return raw


def bootstrap(sample: bool = False):
    """Main bootstrap function."""
    output_dir = SAMPLE_DIR if sample else DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        # Save individual record
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
        filepath = output_dir / f"{safe_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1

    print(f"\nSaved {count} records to {output_dir}/")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="TR/KVKK data fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample or not args.full)
