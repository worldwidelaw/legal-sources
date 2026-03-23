#!/usr/bin/env python3
"""
UK/TPR - The Pensions Regulator Fetcher

Fetches TPR publications (codes of practice, guidance, enforcement activity,
consultations) from the Pensions Regulator website via sitemap.

Data source: https://www.thepensionsregulator.gov.uk
Method: Sitemap parsing + HTML scraping for full text
License: Open Government Licence v3.0
Rate limit: 1.5 seconds between requests

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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SITEMAP_URL = "https://www.thepensionsregulator.gov.uk/sitemap.xml"
BASE_URL = "https://www.thepensionsregulator.gov.uk"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/TPR"

HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (Open Data Research; github.com/worldwidelaw/legal-sources)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# URL path segments that indicate substantive content
CONTENT_PATHS = [
    "/document-library/",
    "/code-of-practice/",
    "/guidance/",
    "/regulatory-and-enforcement-policy/",
]

# Welsh language prefix to exclude
WELSH_PREFIX = "/cy/"


def fetch_sitemap() -> List[Tuple[str, Optional[str]]]:
    """Fetch and parse the sitemap to get all content URLs.

    Returns:
        List of (url, lastmod) tuples
    """
    resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    urls = []
    for url_elem in root.findall(".//ns:url", ns):
        loc = url_elem.find("ns:loc", ns)
        if loc is None or not loc.text:
            continue

        url = loc.text.strip()

        # Skip Welsh translations
        path = url.replace(BASE_URL, "")
        if path.startswith(WELSH_PREFIX):
            continue

        # Only include document-library and substantive content
        if not any(seg in path for seg in CONTENT_PATHS):
            continue

        lastmod_elem = url_elem.find("ns:lastmod", ns)
        lastmod = lastmod_elem.text.strip() if lastmod_elem is not None and lastmod_elem.text else None

        urls.append((url, lastmod))

    return urls


def classify_document(url: str, title: str) -> str:
    """Classify a document by its URL path and title."""
    path = url.lower()
    title_lower = title.lower()

    if "/code-of-practice/" in path or "code of practice" in title_lower:
        return "code_of_practice"
    elif "/enforcement-activity/" in path or "enforcement" in title_lower:
        return "enforcement"
    elif "/consultations/" in path or "consultation" in title_lower:
        return "consultation"
    elif "/statements/" in path or "statement" in title_lower:
        return "statement"
    elif "/research-and-analysis/" in path:
        return "research"
    elif "/regulatory-and-enforcement-policy/" in path:
        return "policy"
    elif "/scheme-management/" in path:
        return "guidance"
    elif "/automatic-enrolment/" in path:
        return "guidance"
    else:
        return "guidance"


def fetch_full_text(url: str) -> Tuple[str, str, Optional[str]]:
    """
    Fetch the full text content of a TPR page.

    Returns:
        Tuple of (text, title, date_str)
    """
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract title
    title = ""
    title_elem = soup.find("h1")
    if title_elem:
        title = title_elem.get_text(strip=True)

    # Extract date
    date_str = None
    # Look for published/updated date patterns
    for pattern in [
        soup.find("time"),
        soup.find("span", class_=re.compile(r"date|published", re.I)),
        soup.find("div", class_=re.compile(r"date|published", re.I)),
    ]:
        if pattern:
            date_str = pattern.get("datetime") or pattern.get_text(strip=True)
            if date_str:
                break

    # Look for date in meta tags
    if not date_str:
        for name in ["date", "DC.date", "article:published_time"]:
            meta = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
            if meta and meta.get("content"):
                date_str = meta["content"]
                break

    # Extract main content
    text_parts = []

    # Try various content containers
    content_area = None
    for selector in ["main", "article", "div.content", "div.page-content",
                      "div[role='main']", "div.article-content", "div.body-content"]:
        content_area = soup.select_one(selector)
        if content_area and len(content_area.get_text(strip=True)) > 200:
            break
        content_area = None

    if not content_area:
        # Fallback to body
        content_area = soup.find("body")

    if content_area:
        # Remove nav, header, footer, sidebar, scripts
        for unwanted in content_area.select(
            "nav, header, footer, aside, script, style, .cookie-banner, "
            ".breadcrumb, .pagination, .sidebar, .nav, .menu, .share, "
            ".related-links, form, .skip-link"
        ):
            unwanted.decompose()

        for elem in content_area.find_all(["h1", "h2", "h3", "h4", "h5", "h6",
                                            "p", "li", "td", "blockquote", "pre"]):
            text = elem.get_text(separator=" ", strip=True)
            if text and len(text) > 1:
                if elem.name.startswith("h"):
                    text_parts.append(f"\n## {text}\n")
                elif elem.name == "li":
                    text_parts.append(f"- {text}")
                else:
                    text_parts.append(text)

    full_text = "\n".join(text_parts).strip()
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    full_text = re.sub(r" {2,}", " ", full_text)

    return full_text, title, date_str


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse a date string into ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%B %Y", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str[:19] if "T" in date_str else date_str, fmt.replace("%z", "")).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try extracting just YYYY-MM-DD from start
    m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if m:
        return m.group(1)
    return None


def generate_doc_id(url: str) -> str:
    """Generate a document ID from a URL."""
    path = url.replace(BASE_URL, "").strip("/")
    # Remove /en/ prefix
    if path.startswith("en/"):
        path = path[3:]
    doc_id = re.sub(r"[^a-zA-Z0-9/_-]", "_", path)
    doc_id = re.sub(r"_+", "_", doc_id)
    return doc_id[:200]


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all TPR publications with full text."""
    print("Fetching TPR sitemap...")
    urls = fetch_sitemap()
    print(f"Found {len(urls)} content URLs in sitemap")

    total_yielded = 0
    errors = 0

    for url, lastmod in urls:
        if max_records and total_yielded >= max_records:
            return

        slug = url.split("/")[-1] or url.split("/")[-2]
        print(f"\n[{total_yielded + 1}] Fetching: {slug}")

        try:
            text, page_title, page_date = fetch_full_text(url)
        except requests.RequestException as e:
            print(f"  Error: {e}")
            errors += 1
            time.sleep(2.0)
            continue

        if not text or len(text) < 100:
            print(f"  Skipping: insufficient text ({len(text) if text else 0} chars)")
            continue

        title = page_title or slug.replace("-", " ").title()
        date = parse_date(page_date) or parse_date(lastmod)
        doc_type = classify_document(url, title)

        record = {
            "_id": generate_doc_id(url),
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "document_type": doc_type,
            "language": "en",
        }

        yield record
        total_yielded += 1

        if total_yielded % 10 == 0:
            print(f"  Progress: {total_yielded} records ({errors} errors)")

        time.sleep(1.5)

    print(f"\nCompleted: {total_yielded} records ({errors} errors)")


def normalize(raw: dict) -> dict:
    """Validate and normalize a record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")
    if not raw.get("text") or len(raw["text"]) < 100:
        raise ValueError(f"Insufficient text content ({len(raw.get('text', ''))} chars)")
    return raw


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample TPR publications...")
    print("=" * 60)

    count = 0
    for record in fetch_all(max_records=sample_count):
        try:
            record = normalize(record)
        except ValueError as e:
            print(f"  Validation error: {e}")
            continue

        filename = SAMPLE_DIR / f"{count:03d}_{record['_id'][:60].replace('/', '_')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        text_len = len(record.get("text", ""))
        print(f"  Saved: {filename.name} ({text_len} chars)")
        count += 1

    print(f"\n{'=' * 60}")
    print(f"Sample complete: {count} records saved to {SAMPLE_DIR}/")

    if count < 10:
        print(f"WARNING: Only {count} records - need at least 10!")
        return False

    for f in SAMPLE_DIR.glob("*.json"):
        with open(f) as fh:
            rec = json.load(fh)
        if not rec.get("text") or len(rec["text"]) < 100:
            print(f"WARNING: {f.name} has insufficient text!")
            return False

    print("All validation checks passed!")
    return True


def test_connectivity():
    """Test connectivity."""
    print("Testing TPR sitemap...")
    try:
        urls = fetch_sitemap()
        print(f"  Sitemap: OK ({len(urls)} content URLs)")
    except Exception as e:
        print(f"  Sitemap: FAILED - {e}")
        return False

    if urls:
        print(f"\nTesting full text fetch: {urls[0][0]}")
        try:
            text, title, date = fetch_full_text(urls[0][0])
            print(f"  Full text: OK ({len(text)} chars)")
            print(f"  Title: {title[:80]}")
            print(f"  Date: {date}")
            if len(text) > 200:
                print(f"  Preview: {text[:200]}...")
        except Exception as e:
            print(f"  Full text: FAILED - {e}")
            return False

    print("\nAll connectivity tests passed!")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UK/TPR data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            for record in fetch_all():
                pass
