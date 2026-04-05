#!/usr/bin/env python3
"""
IRS Internal Revenue Bulletins Data Fetcher

Fetches U.S. tax doctrine from IRS Internal Revenue Bulletins (IRBs).
Each weekly bulletin contains revenue rulings, revenue procedures, notices,
treasury decisions, and announcements — all with full text.

Data source: https://www.irs.gov/irb/YYYY-WW_IRB
License: U.S. Government Work (Public Domain)
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

# Constants
BASE_URL = "https://www.irs.gov/irb/{year}-{week:02d}_IRB"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
START_YEAR = 2005
CURRENT_YEAR = datetime.now().year

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "text/html,application/xhtml+xml",
}


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    # Remove script and style elements
    for element in soup(["script", "style", "nav", "header", "footer"]):
        element.decompose()
    text = soup.get_text(separator="\n")
    # Clean up whitespace
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def extract_bulletin_content(html: str) -> dict:
    """Extract structured content from an IRB HTML page."""
    soup = BeautifulSoup(html, "html.parser")

    # Extract title
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Find the largest field--name-body div (IRS pages have multiple, content is in the largest)
    body_divs = soup.find_all("div", class_="field--name-body")
    content_div = None
    if body_divs:
        content_div = max(body_divs, key=lambda d: len(d.get_text()))
    if not content_div or len(content_div.get_text(strip=True)) < 500:
        content_div = (
            soup.find("div", class_="book")
            or soup.find("article")
            or soup.find("main")
        )

    if content_div:
        # Remove navigation elements within content
        for nav in content_div.find_all(["nav", "aside"]):
            nav.decompose()
        text = content_div.get_text(separator="\n")
    else:
        # Fallback: get all text from body
        body = soup.find("body")
        if body:
            for element in body(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            text = body.get_text(separator="\n")
        else:
            text = soup.get_text(separator="\n")

    # Clean up whitespace
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    text = text.strip()

    # Extract date from title (e.g., "Internal Revenue Bulletin: 2024-52")
    date_match = re.search(r"(\d{4})-(\d{1,2})", title)

    return {
        "title": title,
        "text": text,
        "date_match": date_match,
    }


def fetch_bulletin(year: int, week: int) -> Optional[dict]:
    """Fetch a single IRB bulletin."""
    url = BASE_URL.format(year=year, week=week)

    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 404:
            return None
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None

    content = extract_bulletin_content(response.text)

    if not content["text"] or len(content["text"]) < 200:
        return None

    return {
        "year": year,
        "week": week,
        "url": url,
        "title": content["title"],
        "text": content["text"],
        "html_length": len(response.text),
    }


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all IRB bulletins from START_YEAR to present."""
    doc_count = 0
    consecutive_misses = 0

    for year in range(CURRENT_YEAR, START_YEAR - 1, -1):
        max_week = 53
        consecutive_misses = 0

        for week in range(1, max_week + 1):
            raw = fetch_bulletin(year, week)

            if raw is None:
                consecutive_misses += 1
                if consecutive_misses >= 5 and week > 40:
                    # Past the last bulletin for this year
                    break
                continue

            consecutive_misses = 0
            yield raw
            doc_count += 1

            if max_docs and doc_count >= max_docs:
                return

            if doc_count % 10 == 0:
                print(f"Fetched {doc_count} bulletins (at {year}-{week:02d})...", file=sys.stderr)

            time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch bulletins from a given date onwards."""
    since_year = since.year
    # Approximate week from date
    since_week = max(1, since.isocalendar()[1] - 1)

    for year in range(since_year, CURRENT_YEAR + 1):
        start_week = since_week if year == since_year else 1
        for week in range(start_week, 54):
            raw = fetch_bulletin(year, week)
            if raw:
                yield raw
                time.sleep(RATE_LIMIT_DELAY)


def normalize(raw: dict) -> dict:
    """Transform raw bulletin data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    year = raw["year"]
    week = raw["week"]
    doc_id = f"IRB-{year}-{week:02d}"

    # Approximate date from year and week (Monday of that week)
    try:
        date = datetime.strptime(f"{year}-W{week:02d}-1", "%Y-W%W-%w").strftime("%Y-%m-%d")
    except ValueError:
        date = f"{year}-01-01"

    return {
        "_id": doc_id,
        "_source": "US/IRS-IRB",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": raw.get("title") or f"Internal Revenue Bulletin {year}-{week:02d}",
        "text": raw["text"],
        "date": date,
        "url": raw["url"],
        "year": year,
        "week": week,
        "language": "en",
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count + 5):
        record = normalize(raw)

        if not record["text"] or len(record["text"]) < 500:
            print(f"Skipping {record['_id']}: insufficient text ({len(record.get('text', ''))} chars)", file=sys.stderr)
            continue

        samples.append(record)

        # Save individual sample
        filename = f"{record['_id']}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    # Save combined samples
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

    parser = argparse.ArgumentParser(description="IRS Internal Revenue Bulletins fetcher")
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
        since = datetime.fromisoformat(args.since)
        for raw in fetch_updates(since):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
