#!/usr/bin/env python3
"""
UK HMRC Tax Guidance Manuals Fetcher

Fetches HMRC internal manuals from GOV.UK Content API:
- 247 manuals with 84,654+ sections covering all UK tax guidance
- Capital Gains, Business Income, VAT, PAYE, Corporation Tax, etc.

Data source: https://www.gov.uk/hmrc-internal-manuals
License: Open Government Licence v3.0
API: GOV.UK Content API (JSON, no authentication required)
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("beautifulsoup4 required: pip install beautifulsoup4", file=sys.stderr)
    sys.exit(1)

SEARCH_API = "https://www.gov.uk/api/search.json"
CONTENT_API = "https://www.gov.uk/api/content"
GOV_UK = "https://www.gov.uk"

RATE_LIMIT_DELAY = 1.5  # seconds between content API requests
SEARCH_PAGE_SIZE = 500  # max useful page size for search API
CURL_TIMEOUT = 30


def fetch_json(url: str, retries: int = 2) -> Optional[dict]:
    """Fetch JSON from a URL using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: application/json",
                 "-w", "\n%{http_code}", url],
                capture_output=True, text=True, timeout=CURL_TIMEOUT + 10
            )
            parts = result.stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status_code = parts[0], parts[1].strip()
            else:
                body, status_code = result.stdout, "000"

            if status_code == "429":
                wait = 5 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue

            if not status_code.startswith("2"):
                if attempt == retries:
                    print(f"HTTP {status_code} for {url}", file=sys.stderr)
                    return None
                time.sleep(2)
                continue

            if body:
                return json.loads(body)

        except json.JSONDecodeError as e:
            if attempt == retries:
                print(f"JSON decode error for {url}: {e}", file=sys.stderr)
                return None
            time.sleep(2)
        except Exception as e:
            if attempt == retries:
                print(f"Failed to fetch {url}: {e}", file=sys.stderr)
                return None
            time.sleep(2)
    return None


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text from an HTML body string."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")

    # Remove scripts and styles
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()

    # Get text with newlines for block elements
    text = soup.get_text(separator="\n")

    # Clean up whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    text = text.strip()

    return text


def search_sections(start: int = 0, count: int = SEARCH_PAGE_SIZE) -> Optional[dict]:
    """Search for HMRC manual sections via the GOV.UK Search API."""
    url = (
        f"{SEARCH_API}"
        f"?filter_document_type=hmrc_manual_section"
        f"&count={count}"
        f"&start={start}"
        f"&fields=link,title,public_timestamp"
    )
    return fetch_json(url)


def fetch_section_content(link: str) -> Optional[dict]:
    """Fetch full content of a manual section via the Content API."""
    url = f"{CONTENT_API}{link}"
    return fetch_json(url)


def iterate_all_sections(max_sections: Optional[int] = None) -> Generator[dict, None, None]:
    """Iterate through all HMRC manual sections using the search API."""
    start = 0
    total = None
    yielded = 0

    while True:
        if max_sections and yielded >= max_sections:
            return

        data = search_sections(start=start)
        if not data or not data.get("results"):
            break

        if total is None:
            total = data.get("total", 0)
            print(f"Total sections available: {total:,}", file=sys.stderr)

        results = data["results"]
        if not results:
            break

        for item in results:
            yield item
            yielded += 1
            if max_sections and yielded >= max_sections:
                return

        start += len(results)
        if start >= (total or 0):
            break

        # Small delay between search pages
        time.sleep(0.5)


def extract_section_record(search_item: dict) -> Optional[dict]:
    """Fetch and extract a full section record from a search result item."""
    link = search_item.get("link", "")
    if not link:
        return None

    time.sleep(RATE_LIMIT_DELAY)

    content = fetch_section_content(link)
    if not content:
        return None

    details = content.get("details", {})
    body_html = details.get("body", "")

    # Strip HTML to get clean text
    text = strip_html(body_html)
    if not text:
        return None

    # Extract identifiers from the link path
    # e.g. /hmrc-internal-manuals/capital-gains-manual/cg10100
    parts = link.strip("/").split("/")
    if len(parts) >= 3:
        manual_slug = parts[1]
        section_slug = parts[2]
    elif len(parts) == 2:
        manual_slug = parts[1]
        section_slug = ""
    else:
        manual_slug = ""
        section_slug = ""

    section_id = details.get("section_id", section_slug.upper())
    title = content.get("title", search_item.get("title", ""))

    # Date: prefer public_updated_at from content API, fallback to search result
    date_str = content.get("public_updated_at") or search_item.get("public_timestamp")
    date = None
    if date_str:
        # Normalize to ISO date
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date = dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            date = date_str[:10] if len(date_str) >= 10 else None

    url = f"{GOV_UK}{link}"

    return {
        "link": link,
        "manual_slug": manual_slug,
        "section_id": section_id,
        "section_slug": section_slug,
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "body_html": body_html,
    }


def normalize(raw: dict) -> dict:
    """Transform raw section data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    section_id = raw.get("section_id", "")
    manual_slug = raw.get("manual_slug", "")

    # Build stable ID: MANUAL-SLUG/SECTION-ID
    if section_id:
        doc_id = f"{manual_slug}/{section_id}"
    else:
        doc_id = raw.get("link", "").strip("/").replace("hmrc-internal-manuals/", "")

    return {
        "_id": doc_id,
        "_source": "UK/HMRC-Manuals",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": raw["title"],
        "text": raw["text"],
        "date": raw.get("date"),
        "url": raw["url"],
        "manual_slug": manual_slug,
        "section_id": section_id,
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    # Fetch a diverse set: pick sections from different manuals
    # First get a batch from search, then fetch content for the best ones
    print("Fetching section list from search API...", file=sys.stderr)

    # Get sections from different starting points for diversity
    candidates = []
    offsets = [0, 10000, 25000, 40000, 60000]
    for offset in offsets:
        data = search_sections(start=offset, count=5)
        if data and data.get("results"):
            candidates.extend(data["results"])
        time.sleep(0.5)

    if not candidates:
        print("ERROR: No sections found from search API", file=sys.stderr)
        sys.exit(1)

    print(f"Got {len(candidates)} candidate sections from {len(offsets)} offsets", file=sys.stderr)

    samples = []
    seen_manuals = set()
    errors = 0

    for item in candidates:
        if len(samples) >= count:
            break

        link = item.get("link", "")
        parts = link.strip("/").split("/")
        manual = parts[1] if len(parts) >= 3 else "unknown"

        print(f"  Fetching: {link}...", file=sys.stderr)
        raw = extract_section_record(item)

        if not raw or not raw["text"] or len(raw["text"]) < 200:
            errors += 1
            print(f"  -> Skipped (insufficient text)", file=sys.stderr)
            continue

        record = normalize(raw)
        samples.append(record)
        seen_manuals.add(manual)

        # Save individual file
        safe_id = re.sub(r'[^\w\-.]', '_', record["_id"])
        filename = f"{safe_id}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  -> Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

    # Save combined samples
    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Unique manuals: {len(seen_manuals)}", file=sys.stderr)
        print(f"Errors/skipped: {errors}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)
    else:
        print("ERROR: No samples generated!", file=sys.stderr)
        sys.exit(1)


def bootstrap_full() -> None:
    """Full bootstrap: stream all sections as JSONL to stdout."""
    count = 0
    errors = 0

    for item in iterate_all_sections():
        raw = extract_section_record(item)
        if not raw or not raw["text"]:
            errors += 1
            continue

        record = normalize(raw)
        print(json.dumps(record, ensure_ascii=False))
        count += 1

        if count % 100 == 0:
            print(f"Progress: {count:,} sections fetched, {errors} errors",
                  file=sys.stderr)

    print(f"\n=== Bootstrap Complete ===", file=sys.stderr)
    print(f"Total fetched: {count:,}", file=sys.stderr)
    print(f"Errors: {errors}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UK HMRC Tax Guidance Manuals fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Generate sample data only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of samples to generate")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            bootstrap_full()

    elif args.command == "fetch":
        max_docs = args.count if args.sample else None
        for item in iterate_all_sections(max_sections=max_docs):
            raw = extract_section_record(item)
            if raw and raw["text"]:
                record = normalize(raw)
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        print("Updates mode not yet implemented", file=sys.stderr)


if __name__ == "__main__":
    main()
