#!/usr/bin/env python3
"""
Hungarian Competition Authority (GVH) Resolutions Data Fetcher

Fetches competition authority decisions from www.gvh.hu.
Uses the Solr-backed search API (Ponte Portal) to enumerate all 4,181+ decisions,
then fetches inline HTML full text from each decision page.

Data source: https://www.gvh.hu/dontesek/versenyhivatali_dontesek
License: Public (Government of Hungary)
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.gvh.hu"
SEARCH_PAGE = "/dontesek/versenyhivatali_dontesek/kereses-versenyhivatali-dontesekben"
SEARCH_ENDPOINT_SUFFIX = "/$rspid0x1532870x14/$risearch"
RATE_LIMIT_DELAY = 1.5
PAGE_SIZE = 25

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "text/html,application/xhtml+xml",
}

# Hungarian month names -> month numbers
HU_MONTHS = {
    "jan": 1, "feb": 2, "márc": 3, "ápr": 4, "máj": 5, "jún": 6,
    "júl": 7, "aug": 8, "szept": 9, "okt": 10, "nov": 11, "dec": 12,
}


def parse_hu_date(date_str: str) -> Optional[str]:
    """Parse Hungarian date like '2026. márc. 25.' to ISO format."""
    if not date_str:
        return None
    m = re.match(r"(\d{4})\.\s*(\w+)\.\s*(\d{1,2})\.", date_str.strip())
    if m:
        year = int(m.group(1))
        month_name = m.group(2).lower().rstrip(".")
        day = int(m.group(3))
        month = HU_MONTHS.get(month_name)
        if month:
            return f"{year}-{month:02d}-{day:02d}"
    return None


class GVHSession:
    """Manages session, CSRF token, and search API access."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.csrf_token = None

    def init_session(self):
        """Get session cookie and CSRF token from search page."""
        url = BASE_URL + SEARCH_PAGE
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        match = re.search(r'<meta name="_csrf" content="([^"]+)"', r.text)
        if not match:
            raise RuntimeError("Could not find CSRF token on search page")
        self.csrf_token = match.group(1)
        print(f"Session initialized, CSRF obtained", file=sys.stderr)

    def search(self, page_index: int = 0) -> dict:
        """Search for decisions on a given page."""
        if not self.csrf_token:
            self.init_session()

        url = BASE_URL + SEARCH_PAGE + SEARCH_ENDPOINT_SUFFIX
        headers = {
            "X-SECURITY": self.csrf_token,
            "Content-Type": "application/json",
        }
        data = {"query": "*", "pageIndex": page_index}

        r = self.session.post(url, headers=headers, json=data, timeout=30)
        if r.status_code == 403:
            # CSRF expired, refresh
            self.init_session()
            headers["X-SECURITY"] = self.csrf_token
            r = self.session.post(url, headers=headers, json=data, timeout=30)

        r.raise_for_status()
        return r.json()

    def fetch_decision_page(self, path: str) -> Optional[str]:
        """Fetch the HTML of a decision page."""
        url = BASE_URL + path if not path.startswith("http") else path
        try:
            r = self.session.get(url, timeout=30)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"Error fetching {url}: {e}", file=sys.stderr)
            return None


def parse_search_item(item: dict) -> dict:
    """Parse a search result item's HTML snippet."""
    soup = BeautifulSoup(item.get("data", ""), "html.parser")
    link = soup.find("a", href=True)
    date_span = soup.find("span", class_="list-date")

    return {
        "solr_id": item.get("id", ""),
        "path": link["href"] if link else "",
        "case_number": link.get_text(strip=True) if link else "",
        "date_raw": date_span.get_text(strip=True) if date_span else "",
    }


def extract_decision_text(html: str) -> dict:
    """Extract text and metadata from a decision page."""
    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # Main content - look for article-body first
    article = soup.find("div", class_="article-body")
    if not article:
        article = soup.find("main")

    text = ""
    if article:
        for e in article.find_all(["nav", "aside", "script", "style"]):
            e.decompose()
        text = article.get_text(separator="\n")
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text).strip()

    # Check if it's a 404 page
    if "nem található" in text[:300]:
        return {"title": "", "text": "", "pdf_path": None}

    # Remove "Nyomtatható verzió PDF formátumban" prefix line
    text = re.sub(r"^Nyomtatható verzió PDF formátumban\s*\n*", "", text)

    # Find PDF path if available
    pdf_path = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "pfile" in href and ".pdf" in href:
            link_text = a.get_text(strip=True).lower()
            if "nyomtatható" in link_text or "pdf" in link_text:
                pdf_path = href
                break

    return {
        "title": title,
        "text": text,
        "pdf_path": pdf_path,
    }


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Enumerate all GVH decisions via search API and fetch full text."""
    gvh = GVHSession()
    gvh.init_session()

    # Get total count
    first_page = gvh.search(0)
    total = first_page.get("numFound", 0)
    page_count = first_page.get("pageCount", 0)
    print(f"Total decisions: {total}, pages: {page_count}", file=sys.stderr)

    doc_count = 0

    for page_idx in range(page_count):
        if page_idx > 0:
            time.sleep(RATE_LIMIT_DELAY)
            result = gvh.search(page_idx)
        else:
            result = first_page

        items = result.get("items", [])

        for item in items:
            parsed = parse_search_item(item)
            if not parsed["path"]:
                continue

            time.sleep(RATE_LIMIT_DELAY)
            html = gvh.fetch_decision_page(parsed["path"])
            if not html:
                continue

            content = extract_decision_text(html)
            if not content["text"] or len(content["text"]) < 100:
                continue

            raw = {
                "case_number": parsed["case_number"],
                "date_raw": parsed["date_raw"],
                "path": parsed["path"],
                "title": content["title"] or parsed["case_number"],
                "text": content["text"],
                "pdf_path": content["pdf_path"],
                "solr_id": parsed["solr_id"],
            }

            yield raw
            doc_count += 1

            if doc_count % 25 == 0:
                print(f"Fetched {doc_count} decisions (page {page_idx + 1}/{page_count})...", file=sys.stderr)

            if max_docs and doc_count >= max_docs:
                return


def normalize(raw: dict) -> dict:
    """Transform raw decision into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    case_num = raw.get("case_number", "")
    # Clean up case number for ID (e.g., "Vj-33/2024/85" -> "Vj-33-2024-85")
    doc_id = re.sub(r"[/\\]", "-", case_num) if case_num else raw.get("solr_id", "unknown")

    date = parse_hu_date(raw.get("date_raw", ""))
    full_url = BASE_URL + raw["path"] if not raw["path"].startswith("http") else raw["path"]

    return {
        "_id": f"GVH-{doc_id}",
        "_source": "HU/GVH",
        "_type": "case_law",
        "_fetched_at": now,
        "title": raw.get("title") or case_num,
        "text": raw["text"],
        "date": date,
        "url": full_url,
        "case_number": case_num,
        "language": "hu",
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count + 5):
        record = normalize(raw)

        if not record["text"] or len(record["text"]) < 200:
            print(f"Skipping {record['_id']}: insufficient text ({len(record.get('text', ''))} chars)", file=sys.stderr)
            continue

        samples.append(record)

        filename = re.sub(r"[^\w\-.]", "_", f"{record['_id']}.json")
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GVH Competition Authority decisions fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Generate sample data only")
    parser.add_argument("--count", type=int, default=15,
                       help="Number of samples to generate")
    parser.add_argument("--since", type=str,
                       help="Fetch updates since date (YYYY-MM-DD)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            for raw in fetch_all():
                record = normalize(raw)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for raw in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        # Use dateFrom filter in search API
        for raw in fetch_all():
            record = normalize(raw)
            if record["text"] and record.get("date") and record["date"] >= args.since:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
