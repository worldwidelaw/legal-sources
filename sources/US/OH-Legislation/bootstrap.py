#!/usr/bin/env python3
"""
US/OH-Legislation -- Ohio Revised Code

Fetches the Ohio Revised Code from law.onecle.com which mirrors the official
codes.ohio.gov content in static HTML (codes.ohio.gov itself is unreliable
and frequently times out for programmatic access).

Strategy:
  - GET /ohio/ to discover all title slugs
  - For each title, GET /ohio/{title-slug}/index.html to list chapters
  - For each chapter, GET /ohio/{title-slug}/chapter-{N}/index.html to list sections
  - For each section, GET /ohio/{title-slug}/{section}.html for full text
  - Parse HTML to extract section text and metadata

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/OH-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OH-Legislation")

BASE_URL = "https://law.onecle.com/ohio"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 2  # seconds between requests


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h\d>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_url(url: str) -> Optional[str]:
    """Fetch a URL with retry logic."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


def get_title_slugs() -> list:
    """Get all title slugs from the Ohio index page."""
    html = fetch_url(f"{BASE_URL}/")
    if not html:
        return []

    slugs = []
    # Match links like /ohio/general-provisions/index.html or /ohio/title-29/index.html
    for match in re.finditer(r'href="(?:/ohio/)?([a-z0-9-]+)/(?:index\.html)?"', html):
        slug = match.group(1)
        if slug not in slugs and (slug.startswith("title-") or slug == "general-provisions"):
            slugs.append(slug)

    return slugs


def get_chapters_for_title(title_slug: str) -> list:
    """Get list of chapter info for a title."""
    html = fetch_url(f"{BASE_URL}/{title_slug}/index.html")
    if not html:
        return []

    chapters = []
    # Match links to chapter pages: chapter-1/index.html
    for match in re.finditer(
        r'href="(?:/ohio/[^/]+/)?chapter-(\d+)/(?:index\.html)?"[^>]*>\s*(?:<[^>]+>)*\s*([^<]+)',
        html, re.IGNORECASE
    ):
        ch_num = match.group(1)
        ch_name = match.group(2).strip().rstrip('.')
        # Clean up "Chapter N. Name" prefixes
        ch_name = re.sub(r'^Chapter\s+\d+[A-Z]?\.\s*', '', ch_name)
        chapters.append((ch_num, ch_name))

    return chapters


def get_sections_for_chapter(title_slug: str, chapter_num: str) -> list:
    """Get list of section info for a chapter."""
    html = fetch_url(f"{BASE_URL}/{title_slug}/chapter-{chapter_num}/index.html")
    if not html:
        return []

    sections = []
    # Match links to section pages: /ohio/title-slug/SECTION.html
    for match in re.finditer(
        r'href="(?:/ohio/[^/]+/)?(\d+[\w.]+)\.html"[^>]*>\s*(?:<[^>]+>)*\s*([^<]+)',
        html, re.IGNORECASE
    ):
        sec_id = match.group(1)
        sec_name = match.group(2).strip().rstrip('.')
        # Clean up "Section X.XX " prefix
        sec_name = re.sub(r'^(?:Section\s+)?\d+[\w.]*\s*[-–—]\s*', '', sec_name)
        sec_name = re.sub(r'^(?:Section\s+)?\d+[\w.]*\s+', '', sec_name) if not sec_name else sec_name
        sections.append((sec_id, sec_name))

    return sections


def fetch_section_text(title_slug: str, section_id: str) -> Optional[str]:
    """Fetch the full text of a section."""
    html = fetch_url(f"{BASE_URL}/{title_slug}/{section_id}.html")
    if not html:
        return None

    # Extract the main content area - typically between the heading and the footer/nav
    # Look for the statute text area
    content = html

    # Remove navigation, header, footer, ads
    content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<header[^>]*>.*?</header>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<footer[^>]*>.*?</footer>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

    # Try to extract just the statute content div
    main_match = re.search(
        r'<div[^>]*(?:class|id)="[^"]*(?:content|statute|section|law|text)[^"]*"[^>]*>(.*?)</div>\s*(?:<div|<footer|</body)',
        content, re.DOTALL | re.IGNORECASE
    )
    if main_match:
        content = main_match.group(1)

    text = clean_html(content)

    # Remove boilerplate lines
    lines = text.split('\n')
    filtered = []
    skip_patterns = [
        r'^\s*(?:Home|Ohio|Search|About|Contact|Privacy|Terms|Copyright|©)',
        r'^\s*(?:Onecle|law\.onecle)',
        r'^\s*(?:Previous|Next)\s*(?:Section|Chapter)',
        r'^\s*(?:Disclaimer|Note:.*onecle)',
    ]
    for line in lines:
        if any(re.match(p, line, re.IGNORECASE) for p in skip_patterns):
            continue
        filtered.append(line)

    return '\n'.join(filtered).strip()


def extract_title_info(title_slug: str) -> dict:
    """Extract title number and name from slug."""
    if title_slug == "general-provisions":
        return {"number": 0, "name": "General Provisions"}
    match = re.match(r'title-(\d+)', title_slug)
    if match:
        return {"number": int(match.group(1)), "name": f"Title {match.group(1)}"}
    return {"number": 0, "name": title_slug}


def normalize_section(title_slug: str, title_info: dict, chapter_num: str,
                      chapter_name: str, section_id: str, section_name: str,
                      text: str) -> dict:
    """Normalize a section record."""
    title_part = f"ORC Title {title_info['number']}" if title_info['number'] > 0 else "ORC General Provisions"
    full_title = f"{title_part}, §{section_id}"
    if section_name:
        full_title += f" - {section_name}"

    return {
        "_id": f"US/OH-Legislation/ORC-{section_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": text,
        "date": None,
        "url": f"https://codes.ohio.gov/ohio-revised-code/section-{section_id}",
        "title_number": title_info["number"],
        "title_name": title_info["name"],
        "chapter_number": chapter_num,
        "chapter_name": chapter_name,
        "section_number": section_id,
        "section_name": section_name,
        "jurisdiction": "US-OH",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all section records."""
    title_slugs = get_title_slugs()
    logger.info(f"Found {len(title_slugs)} titles")

    for title_slug in title_slugs:
        title_info = extract_title_info(title_slug)
        logger.info(f"Processing {title_slug} ({title_info['name']})...")

        chapters = get_chapters_for_title(title_slug)
        time.sleep(CRAWL_DELAY)
        logger.info(f"  Found {len(chapters)} chapters")

        for ch_num, ch_name in chapters:
            sections = get_sections_for_chapter(title_slug, ch_num)
            time.sleep(CRAWL_DELAY)

            for sec_id, sec_name in sections:
                text = fetch_section_text(title_slug, sec_id)
                time.sleep(CRAWL_DELAY)

                if not text or len(text) < 50:
                    logger.debug(f"    Skipped {sec_id}: too short or empty")
                    continue

                record = normalize_section(
                    title_slug, title_info, ch_num, ch_name,
                    sec_id, sec_name, text
                )
                logger.info(f"    §{sec_id} - {len(text)} chars")
                yield record


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from diverse titles."""
    records = []

    # Sample from diverse titles
    sample_titles = ["general-provisions", "title-1", "title-13", "title-29",
                     "title-33", "title-39", "title-45", "title-57"]

    title_slugs = get_title_slugs()
    if not title_slugs:
        logger.error("Could not fetch title list")
        return []

    # Filter to titles that exist
    sample_titles = [t for t in sample_titles if t in title_slugs]
    if not sample_titles:
        sample_titles = title_slugs[:8]

    for title_slug in sample_titles:
        if len(records) >= count:
            break

        title_info = extract_title_info(title_slug)
        logger.info(f"Sampling {title_slug}...")

        chapters = get_chapters_for_title(title_slug)
        time.sleep(CRAWL_DELAY)

        if not chapters:
            continue

        # Take first chapter only for sampling
        ch_num, ch_name = chapters[0]
        sections = get_sections_for_chapter(title_slug, ch_num)
        time.sleep(CRAWL_DELAY)

        if not sections:
            continue

        # Take up to 2 sections per chapter
        for sec_id, sec_name in sections[:2]:
            if len(records) >= count:
                break

            text = fetch_section_text(title_slug, sec_id)
            time.sleep(CRAWL_DELAY)

            if not text or len(text) < 50:
                continue

            record = normalize_section(
                title_slug, title_info, ch_num, ch_name,
                sec_id, sec_name, text
            )
            records.append(record)
            logger.info(f"  [{len(records)}] §{sec_id} - {len(text)} chars")

    return records


def test_api():
    """Test connectivity to law.onecle.com Ohio pages."""
    logger.info("Testing law.onecle.com connectivity...")

    # Test index page
    html = fetch_url(f"{BASE_URL}/")
    if not html:
        logger.error("Index page unreachable")
        return False
    logger.info(f"Index page OK - {len(html)} bytes")

    # Get titles
    slugs = get_title_slugs()
    if not slugs:
        logger.error("No title slugs found")
        return False
    logger.info(f"Found {len(slugs)} titles: {slugs[:5]}...")

    # Test chapter listing
    time.sleep(CRAWL_DELAY)
    chapters = get_chapters_for_title(slugs[0])
    if not chapters:
        logger.error("Chapter listing failed")
        return False
    logger.info(f"Title '{slugs[0]}' has {len(chapters)} chapters")

    # Test section listing
    time.sleep(CRAWL_DELAY)
    ch_num, ch_name = chapters[0]
    sections = get_sections_for_chapter(slugs[0], ch_num)
    if not sections:
        logger.error("Section listing failed")
        return False
    logger.info(f"Chapter {ch_num} has {len(sections)} sections")

    # Test section full text
    time.sleep(CRAWL_DELAY)
    sec_id = sections[0][0]
    text = fetch_section_text(slugs[0], sec_id)
    if not text or len(text) < 50:
        logger.error("Section text extraction failed")
        return False
    logger.info(f"Section {sec_id}: {len(text)} chars")
    logger.info(f"Preview: {text[:200]}...")

    return True


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    return len(records) >= 10 and avg_text > 200


def main():
    parser = argparse.ArgumentParser(description="US/OH-Legislation Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
