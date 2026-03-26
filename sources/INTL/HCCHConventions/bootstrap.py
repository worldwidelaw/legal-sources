#!/usr/bin/env python3
"""
INTL/HCCHConventions - Hague Conference Conventions Full Text Fetcher

Fetches full text of ~50 HCCH conventions on private international law.
Covers child abduction, apostille, service, evidence, trusts, judgments, etc.

Data source: https://www.hcch.net/en/instruments/conventions/full-text
Method: HTML scraping of convention full-text pages
License: Free access

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
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
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.hcch.net"
CONVENTIONS_URL = "https://www.hcch.net/en/instruments/conventions"
FULL_TEXT_URL = "https://www.hcch.net/en/instruments/conventions/full-text/?cid={cid}"
OLD_CONVENTIONS_URL = "https://www.hcch.net/en/instruments/the-old-conventions"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/HCCHConventions"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
}

RATE_LIMIT_DELAY = 2.0


def clean_html(html_str):
    """Strip HTML tags and decode entities, preserving structure."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    return unescape(soup.get_text(separator="\n", strip=True))


def discover_conventions(session):
    """Discover all convention CIDs and old convention URLs."""
    conventions = []

    # Phase 1: Main conventions listing page
    print("Discovering conventions from listing page...")
    resp = session.get(CONVENTIONS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find all links that point to full-text pages with ?cid=
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "full-text/?cid=" in href or "full-text?cid=" in href:
            parsed = parse_qs(urlparse(href).query)
            cid = parsed.get("cid", [None])[0]
            if cid:
                title = link.get_text(strip=True)
                if title and cid not in [c.get("cid") for c in conventions]:
                    conventions.append({
                        "cid": cid,
                        "title": title,
                        "type": "convention",
                    })

    # Phase 2: Check specialised-sections pages for CIDs
    print("Checking specialised-sections pages...")
    specialised_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/specialised-sections/" in href:
            full_url = urljoin(BASE_URL, href)
            title = link.get_text(strip=True)
            if full_url not in [s[0] for s in specialised_links]:
                specialised_links.append((full_url, title))

    for url, title in specialised_links:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                continue
            sub_soup = BeautifulSoup(resp.text, "html.parser")
            for sub_link in sub_soup.find_all("a", href=True):
                href = sub_link["href"]
                if "full-text/?cid=" in href or "full-text?cid=" in href:
                    parsed = parse_qs(urlparse(href).query)
                    cid = parsed.get("cid", [None])[0]
                    if cid and cid not in [c.get("cid") for c in conventions]:
                        link_title = sub_link.get_text(strip=True)
                        conventions.append({
                            "cid": cid,
                            "title": link_title or title,
                            "type": "convention",
                        })
        except Exception as e:
            print(f"  Error checking {url}: {e}")

    # Phase 3: Old conventions
    print("Checking old conventions...")
    resp = session.get(OLD_CONVENTIONS_URL, headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        old_soup = BeautifulSoup(resp.text, "html.parser")
        for link in old_soup.find_all("a", href=True):
            href = link["href"]
            if "/the-old-conventions/" in href and href != "/en/instruments/the-old-conventions":
                full_url = urljoin(BASE_URL, href)
                title = link.get_text(strip=True)
                if title and full_url not in [c.get("url") for c in conventions]:
                    conventions.append({
                        "url": full_url,
                        "title": title,
                        "type": "old_convention",
                    })

    print(f"Discovered {len(conventions)} conventions")
    return conventions


def fetch_convention_text(session, convention):
    """Fetch full text of a single convention."""
    if convention["type"] == "old_convention":
        url = convention["url"]
    else:
        url = FULL_TEXT_URL.format(cid=convention["cid"])

    resp = session.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code} for {url}")
        return None, url

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract the main content area
    content = soup.select_one("div.page-content")
    if not content:
        for selector in ["article", "div.content", "main"]:
            content = soup.select_one(selector)
            if content:
                break
    if not content:
        content = soup.find("body")

    if not content:
        return "", url

    # Remove navigation, menus, footers, scripts, breadcrumbs
    for tag in content.find_all(["nav", "header", "footer", "script", "style",
                                  "noscript", "iframe"]):
        tag.decompose()
    for tag in content.find_all("div", class_=lambda c: c and (
            "breadcrumb" in " ".join(c).lower() or
            "sidebar" in " ".join(c).lower() or
            "menu" in " ".join(c).lower())):
        tag.decompose()
    # Remove the convention number/title header block if it duplicates
    for tag in content.find_all("ul", class_="breadcrumb"):
        tag.decompose()

    text = content.get_text(separator="\n", strip=True)
    text = unescape(text)

    # Clean up excessive whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Extract PDF link if present
    pdf_url = ""
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "assets.hcch.net" in href and href.endswith(".pdf"):
            pdf_url = href
            break

    return text, url, pdf_url


def extract_year(title):
    """Extract year from convention title."""
    match = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
    return match.group(1) if match else None


def normalize(convention, text, url, pdf_url=""):
    """Transform convention data into standard schema."""
    cid = convention.get("cid", "")
    title = convention.get("title", "")
    year = extract_year(title)
    doc_id = f"HCCH-{cid}" if cid else f"HCCH-OLD-{re.sub(r'[^a-zA-Z0-9]', '-', title[:40])}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": f"{year}-01-01" if year else None,
        "url": url,
        "pdf_url": pdf_url,
        "convention_number": cid or None,
        "year": year,
        "instrument_type": convention.get("type", "convention"),
    }


def fetch_all(sample=False):
    """Yield normalized records with full text."""
    session = requests.Session()
    conventions = discover_conventions(session)

    if sample:
        conventions = conventions[:15]
        print(f"Sample mode: fetching {len(conventions)} conventions")

    for i, conv in enumerate(conventions):
        time.sleep(RATE_LIMIT_DELAY)
        result = fetch_convention_text(session, conv)
        if result is None:
            continue

        if len(result) == 3:
            text, url, pdf_url = result
        else:
            text, url = result
            pdf_url = ""

        if not text:
            print(f"  [{i+1}] No text for: {conv.get('title', '?')}")
            continue

        record = normalize(conv, text, url, pdf_url)

        if sample:
            print(f"  [{i+1}/{len(conventions)}] {record['title'][:60]}... "
                  f"text={len(text)} chars")

        yield record


def test_connectivity():
    """Test HCCH website connectivity."""
    print("Testing HCCH connectivity...")
    session = requests.Session()

    resp = session.get(CONVENTIONS_URL, headers=HEADERS, timeout=30)
    print(f"Conventions listing: {resp.status_code}")

    resp2 = session.get(
        FULL_TEXT_URL.format(cid=24), headers=HEADERS, timeout=30)
    print(f"Child Abduction (cid=24): {resp2.status_code} ({len(resp2.text)} chars)")

    soup = BeautifulSoup(resp2.text, "html.parser")
    body = soup.find("body")
    if body:
        text = body.get_text(strip=True)[:200]
        print(f"Text preview: {text}...")
    print("OK - connectivity working")


def main():
    parser = argparse.ArgumentParser(description="INTL/HCCHConventions bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
        return

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    text_count = 0

    for record in fetch_all(sample=args.sample):
        if args.sample:
            out_path = SAMPLE_DIR / f"{record['_id']}.json"
            out_path.write_text(
                json.dumps(record, ensure_ascii=False, indent=2))

        count += 1
        if record.get("text"):
            text_count += 1

    if count:
        print(f"\nDone: {count} records, {text_count} with full text "
              f"({text_count/count*100:.0f}%)")
    else:
        print("\nNo records fetched")


if __name__ == "__main__":
    main()
