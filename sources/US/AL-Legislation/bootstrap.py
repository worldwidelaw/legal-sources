#!/usr/bin/env python3
"""
US/AL-Legislation -- Alabama Code of 1975

Fetches Alabama statutes from the ALISON GraphQL API.
~57,000 items total (titles + chapters + sections); only leaf sections
(numChildren == 0) carry full text in the `content` field.

Strategy:
  - POST to /graphql with codesOfAlabama query
  - Paginate with limit/offset (max 10000 per page)
  - Filter to leaf sections (numChildren == 0, content not null)
  - Strip HTML from content field

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample sections
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

SOURCE_ID = "US/AL-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AL-Legislation")

GRAPHQL_URL = "https://alison.legislature.state.al.us/graphql"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
# Increase connection pool to avoid exhaustion on long crawls
adapter = requests.adapters.HTTPAdapter(
    pool_connections=5, pool_maxsize=10, max_retries=0  # we handle retries ourselves
)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

CRAWL_DELAY = 1.5  # seconds between requests
PAGE_SIZE = 2000  # reduced from 10000 to avoid VPS OOM/timeout crashes


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


# Sentinel to distinguish "server error / transient failure" from "valid empty response"
_QUERY_FAILED = object()

MAX_RETRIES = 5


def graphql_query(query: str, variables: dict = None):
    """Execute a GraphQL query with retry logic.

    Returns:
        dict  -- the ``data`` payload on success
        None  -- if the server returned a valid response with GraphQL-level errors
        _QUERY_FAILED -- on transport / HTTP / decode failures after all retries
    """
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.post(GRAPHQL_URL, json=payload, timeout=90)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                logger.warning(f"GraphQL errors: {data['errors']}")
                return None
            return data.get("data")
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        ) as e:
            wait = min(10 * (attempt + 1), 60)
            logger.warning(f"Request error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)
            else:
                logger.error(f"GraphQL request failed after {MAX_RETRIES} attempts: {e}")
                return _QUERY_FAILED
        except (ValueError, KeyError) as e:
            # JSON decode errors, unexpected structure
            logger.warning(f"Response parse error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(f"Response parse failed after {MAX_RETRIES} attempts: {e}")
                return _QUERY_FAILED
        except Exception as e:
            logger.error(f"Unexpected error in graphql_query: {e}")
            return _QUERY_FAILED


def fetch_page(limit: int, offset: int):
    """Fetch a page of Code of Alabama items.

    Returns:
        list            -- items on success (may be empty for last page)
        _QUERY_FAILED   -- on transport failure (caller should retry / skip)
    """
    query = """
    {
      codesOfAlabama(limit: %d, offset: %d) {
        data {
          id
          title
          displayId
          codeId
          parentId
          numChildren
          content
          effectiveDate
        }
      }
    }
    """ % (limit, offset)

    data = graphql_query(query)
    if data is _QUERY_FAILED:
        return _QUERY_FAILED
    if not data or "codesOfAlabama" not in data:
        return []
    return data["codesOfAlabama"].get("data", [])


def fetch_hierarchy() -> list:
    """Fetch the top-level title hierarchy."""
    query = """
    {
      codeOfAlabamaHierarchy {
        id
        title
        displayId
        codeId
        numChildren
      }
    }
    """
    data = graphql_query(query)
    if not data or "codeOfAlabamaHierarchy" not in data:
        return []
    return data["codeOfAlabamaHierarchy"]


def normalize(item: dict) -> dict:
    """Normalize a Code of Alabama section into standard schema."""
    display_id = item.get("displayId", "")
    raw_title = item.get("title", "")
    content_html = item.get("content", "")
    text = clean_html(content_html)

    # Parse title number from displayId (e.g., "1-1-1" -> title 1)
    title_num = ""
    parts = display_id.split("-")
    if parts:
        title_num = parts[0]

    url = f"https://alison.legislature.state.al.us/code-of-alabama?section={display_id}"

    return {
        "_id": f"US/AL-Legislation/{display_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw_title,
        "text": text,
        "date": item.get("effectiveDate") or "2025-01-01",
        "url": url,
        "display_id": display_id,
        "title_number": title_num,
        "jurisdiction": "US-AL",
        "language": "en",
    }


MAX_CONSECUTIVE_FAILURES = 5   # give up only after 5 back-to-back transport errors


def fetch_all() -> Generator[dict, None, None]:
    """Yield all section records with full text."""
    offset = 0
    total = 0
    errors = 0
    empty_pages = 0
    consecutive_failures = 0

    while True:
        logger.info(f"Fetching page at offset {offset}...")
        items = fetch_page(PAGE_SIZE, offset)
        time.sleep(CRAWL_DELAY)

        # ---- transport / HTTP failure (distinct from empty data) ----
        if items is _QUERY_FAILED:
            consecutive_failures += 1
            errors += 1
            logger.warning(
                f"Page at offset {offset} failed "
                f"({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES} consecutive)"
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    f"Aborting: {MAX_CONSECUTIVE_FAILURES} consecutive page failures. "
                    f"Total records so far: {total}, total errors: {errors}"
                )
                break
            # Skip this page and try the next one
            offset += PAGE_SIZE
            continue

        # ---- valid but empty page (end-of-data detection) ----
        if not items:
            empty_pages += 1
            consecutive_failures = 0  # server responded OK, just no data
            if empty_pages >= 2:
                break
            offset += PAGE_SIZE
            continue

        empty_pages = 0
        consecutive_failures = 0
        page_sections = 0

        for item in items:
            try:
                # Only yield leaf sections with actual content
                if item.get("numChildren", 0) > 0:
                    continue
                if not item.get("content"):
                    continue

                record = normalize(item)
                if len(record["text"]) < 20:
                    continue

                total += 1
                page_sections += 1
                yield record
            except Exception as e:
                errors += 1
                item_id = item.get("displayId", item.get("id", "?"))
                logger.warning(f"Skipping item {item_id}: {e}")
                continue

        logger.info(
            f"  Page returned {len(items)} items, {page_sections} sections "
            f"with text (total: {total}, errors: {errors})"
        )

        if len(items) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    logger.info(f"Total sections with full text: {total} (errors skipped: {errors})")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from a single page."""
    records = []

    # Fetch first page -- it contains Title 1 sections
    logger.info("Fetching sample sections...")
    items = fetch_page(PAGE_SIZE, 0)

    if not items:
        logger.error("No items returned from GraphQL API")
        return []

    for item in items:
        if len(records) >= count:
            break
        if item.get("numChildren", 0) > 0:
            continue
        if not item.get("content"):
            continue

        record = normalize(item)
        if len(record["text"]) < 20:
            continue
        records.append(record)

    return records


def test_api():
    """Test connectivity to the ALISON GraphQL API."""
    logger.info("Testing ALISON GraphQL API connectivity...")

    # Test hierarchy
    titles = fetch_hierarchy()
    if not titles:
        logger.error("Hierarchy query failed")
        return False
    logger.info(f"Hierarchy OK - {len(titles)} titles")

    time.sleep(CRAWL_DELAY)

    # Test content fetch
    items = fetch_page(5, 0)
    if not items:
        logger.error("Content query failed")
        return False

    sections_with_text = [i for i in items if i.get("content")]
    logger.info(f"Content OK - {len(items)} items fetched, {len(sections_with_text)} with content")

    if sections_with_text:
        sample = sections_with_text[0]
        text = clean_html(sample["content"])
        logger.info(f"Sample: {sample.get('title', 'N/A')[:80]}")
        logger.info(f"Text preview ({len(text)} chars): {text[:200]}...")
        return True

    # If first 5 items don't have content (likely titles), fetch more
    time.sleep(CRAWL_DELAY)
    items = fetch_page(100, 0)
    sections_with_text = [i for i in items if i.get("content")]
    if sections_with_text:
        sample = sections_with_text[0]
        text = clean_html(sample["content"])
        logger.info(f"Sample: {sample.get('title', 'N/A')[:80]}")
        logger.info(f"Text preview ({len(text)} chars): {text[:200]}...")
        return True

    logger.error("No sections with content found")
    return False


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
    parser = argparse.ArgumentParser(description="US/AL-Legislation Data Fetcher")
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
            write_errors = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            try:
                for record in fetch_all():
                    count += 1
                    try:
                        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                        filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                        with open(filepath, "w", encoding="utf-8") as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        write_errors += 1
                        logger.warning(f"Failed to write record #{count}: {e}")
            except (KeyboardInterrupt, SystemExit):
                logger.warning(f"Interrupted after {count} records")
            except Exception as e:
                logger.error(f"Generator crashed after {count} records: {e}")
            logger.info(f"Processed {count} records ({write_errors} write errors)")
            sys.exit(0)  # partial data is still valid


if __name__ == "__main__":
    main()
