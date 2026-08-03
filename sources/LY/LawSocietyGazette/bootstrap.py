#!/usr/bin/env python3
"""
LY/LawSocietyGazette — Libya Law Society Legislation Archive

Fetches Libyan legislation from lawsociety.ly — 19,993 documents including
laws, decisions, decrees, regulations, circulars, and constitutional texts.
Full text in Arabic with structured metadata.

Data source: https://lawsociety.ly/legislation/
Method: HTML scraping (list pages + individual pages)
License: Open Government Data (official legislation)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap --full     # Full bootstrap
  python bootstrap.py test                 # Test connectivity
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote

import requests

BASE_URL = "https://lawsociety.ly"
LIST_URL = f"{BASE_URL}/legislation/"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "LY/LawSocietyGazette"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept-Language": "ar,en;q=0.5",
}

RATE_LIMIT_DELAY = 1.5

# Adaptive throttle shared by every request. lawsociety.ly sits behind
# Cloudflare and hard-throttles bulk crawls with HTTP 429; when that happens we
# raise the steady-state pace so subsequent requests self-slow, and relax it
# back toward the floor after sustained success.
_ADAPTIVE = {"delay": RATE_LIMIT_DELAY}
_MAX_DELAY = 12.0


def _http_get(session: requests.Session, url: str, timeout: int = 60,
              max_retries: int = 6) -> Optional[requests.Response]:
    """GET with 429/5xx backoff (honors Retry-After) and an adaptive pace.

    Returns the successful Response, or None if every retry was throttled/5xx.
    Connection-level errors are retried and re-raised only if never recovered.

    This is the fix for issue #1165: the per-document fetch previously skipped
    every 429-throttled document with no retry, truncating the ~20,700-doc
    corpus to ~1,200. Routing all fetches through here means a throttled request
    is backed off and retried instead of silently dropped.
    """
    last_exc = None
    for attempt in range(max_retries):
        time.sleep(_ADAPTIVE["delay"])  # pace every request
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
        except requests.RequestException as e:
            last_exc = e
            time.sleep(min(60.0, RATE_LIMIT_DELAY * (2 ** attempt)))
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            ra = resp.headers.get("Retry-After")
            wait = None
            if ra:
                try:
                    wait = float(ra)
                except ValueError:
                    wait = None
            if wait is None:
                wait = min(120.0, RATE_LIMIT_DELAY * (2 ** (attempt + 1)))
            _ADAPTIVE["delay"] = min(_MAX_DELAY, _ADAPTIVE["delay"] * 1.5)
            print(f"  HTTP {resp.status_code} on ...{url[-45:]} — backoff "
                  f"{wait:.1f}s (pace now {_ADAPTIVE['delay']:.1f}s), "
                  f"attempt {attempt + 1}/{max_retries}")
            time.sleep(wait)
            continue
        # Success — gently relax the pace back toward the floor.
        _ADAPTIVE["delay"] = max(RATE_LIMIT_DELAY, _ADAPTIVE["delay"] * 0.95)
        return resp
    if last_exc is not None:
        raise last_exc
    return None

# Map Arabic type labels to normalized types
TYPE_MAP = {
    "القوانين": "legislation",
    "القرارات": "legislation",
    "المراسيم": "legislation",
    "اللوائح": "legislation",
    "الدستور": "legislation",
    "المناشير": "legislation",
    "المشاريع": "legislation",
    "الاتفاقيات": "legislation",
    "الإعلانات": "legislation",
    "laws": "legislation",
    "decisions": "legislation",
    "decrees": "legislation",
    "regulations": "legislation",
    "constitutional": "legislation",
    "circulars": "legislation",
    "projects": "legislation",
    "conventions": "legislation",
    "announcements": "legislation",
}


def slug_from_url(url: str) -> str:
    """Extract the slug from a legislation URL."""
    path = url.rstrip("/").split("/legislation/")[-1]
    path = path.split("?")[0].split("#")[0].rstrip("/")
    # URL-decode for readability, then hash if too long
    decoded = unquote(path)
    if len(decoded) > 120:
        return hashlib.md5(path.encode()).hexdigest()[:16]
    return decoded


def extract_links_from_list_page(html: str) -> list[str]:
    """Extract individual legislation URLs from a list page."""
    # Match links to individual legislation pages (not filter/pagination links)
    pattern = r'href="(https://lawsociety\.ly/legislation/[^"?]+/)"'
    links = re.findall(pattern, html)
    # Deduplicate while preserving order, exclude pagination pages
    seen = set()
    unique = []
    for link in links:
        if "/page/" in link:
            continue
        # Skip comment-pagination artifacts like ".../{slug}/2/" whose last
        # path segment is purely numeric — they are sub-pages of a document,
        # not distinct documents.
        last_seg = link.rstrip("/").split("/")[-1]
        if last_seg.isdigit():
            continue
        if link not in seen:
            seen.add(link)
            unique.append(link)
    return unique


def extract_total_count(html: str) -> Optional[int]:
    """Parse the total result count from the archive header.

    The archive renders e.g. "عرض 1–20 من 20644 نتيجة" (showing 1-20 of 20644
    results). Returns the total number of legislation documents, or None.
    """
    m = re.search(r"من\s*([\d,]+)\s*نتيجة", html)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def extract_text_from_page(html: str) -> dict:
    """Extract title, full text, date, and type from an individual legislation page."""
    result = {
        "title": "",
        "text": "",
        "date": None,
        "legislation_type": None,
        "status": None,
        "issuing_body": None,
    }

    # Extract title from <h1>
    h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
    if h1_match:
        result["title"] = re.sub(r"<[^>]+>", "", h1_match.group(1)).strip()

    # Extract full text from <article>
    article_match = re.search(r"<article[^>]*>(.*?)</article>", html, re.DOTALL)
    if not article_match:
        return result

    article = article_match.group(1)

    # Extract all text content: paragraphs, list items, headings
    text_parts = []
    # Get paragraphs
    for p in re.finditer(r"<p[^>]*>(.*?)</p>", article, re.DOTALL):
        clean = re.sub(r"<[^>]+>", "", p.group(1)).strip()
        if clean:
            text_parts.append(clean)

    # Get list items
    for li in re.finditer(r"<li[^>]*>(.*?)</li>", article, re.DOTALL):
        clean = re.sub(r"<[^>]+>", "", li.group(1)).strip()
        if clean:
            text_parts.append(clean)

    # Get h2-h6 headings
    for h in re.finditer(r"<h[2-6][^>]*>(.*?)</h[2-6]>", article, re.DOTALL):
        clean = re.sub(r"<[^>]+>", "", h.group(1)).strip()
        if clean:
            text_parts.append(clean)

    result["text"] = "\n\n".join(text_parts)

    # Extract metadata from the first paragraph (contains type, date, etc.)
    first_p = text_parts[0] if text_parts else ""

    # Date extraction: look for dd-mm-yyyy pattern
    date_match = re.search(r"(\d{2})-(\d{2})-(\d{4})", first_p)
    if date_match:
        day, month, year = date_match.groups()
        try:
            result["date"] = f"{year}-{month}-{day}"
        except ValueError:
            pass

    # Also try to find date in meta tags
    if not result["date"]:
        meta_date = re.search(r'datePublished["\s:]+(\d{4}-\d{2}-\d{2})', html)
        if meta_date:
            result["date"] = meta_date.group(1)

    # Type extraction
    for ar_type, en_type in TYPE_MAP.items():
        if ar_type in first_p:
            result["legislation_type"] = en_type
            break

    # Try meta description for type
    if not result["legislation_type"]:
        type_match = re.search(
            r'legislation_type=(\w+)', html
        )
        if type_match:
            t = type_match.group(1).lower()
            result["legislation_type"] = TYPE_MAP.get(t, "legislation")

    return result


def normalize(url: str, data: dict) -> Optional[dict]:
    """Create a normalized record."""
    if not data["text"] or len(data["text"]) < 50:
        return None

    slug = slug_from_url(url)
    doc_id = f"ly-lsg-{hashlib.md5(slug.encode()).hexdigest()[:12]}"

    # Detect court rulings from title keywords
    doc_type = data.get("legislation_type") or "legislation"
    title = data.get("title", "")
    court_keywords = ["المحكمة العليا", "المحكمة", "حكم", "طعن"]
    if any(kw in title for kw in court_keywords):
        doc_type = "case_law"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": data["title"],
        "text": data["text"],
        "date": data["date"],
        "url": url,
        "country": "LY",
        "language": "ar",
        "legislation_type": data.get("legislation_type"),
        "status": data.get("status"),
    }


def _fetch_page_links(session: requests.Session, page: int, retries: int = 4) -> list[str]:
    """Fetch one archive page and return its legislation URLs.

    The archive paginates via the ``?page=N`` query parameter (NOT the WordPress
    ``/page/N/`` path, which is broken and always returns the 20 most-recent
    documents — that was the root cause of issue #1126: the same ~20 URLs were
    re-fetched thousands of times, so ingest only ever saw ~23 unique records).

    The site sits behind Cloudflare and intermittently serves an empty cache
    variant, so an empty response is retried before being reported as empty.
    """
    url = LIST_URL if page == 1 else f"{LIST_URL}?page={page}"
    for attempt in range(retries):
        try:
            resp = _http_get(session, url)
        except requests.RequestException as e:
            print(f"  Page {page}: request error ({e}) — attempt {attempt + 1}/{retries}")
            continue
        if resp is None:
            # Exhausted on 429/5xx inside _http_get — treat as a hard throttle
            # and try again (the adaptive pace has already been raised).
            print(f"  Page {page}: throttled (429/5xx) — attempt {attempt + 1}/{retries}")
            continue
        links = extract_links_from_list_page(resp.text)
        if links:
            return links
        # Empty response: could be a flaky Cloudflare cache miss — retry.
        time.sleep(RATE_LIMIT_DELAY * (attempt + 1))
    return []


def get_list_page_urls(session: requests.Session, max_pages: int = 1200) -> Generator[str, None, None]:
    """Iterate through archive pages and yield individual legislation URLs.

    Pages are bounded by the archive's advertised total result count (parsed
    from page 1) rather than by "first empty page", because flaky empty pages
    are common and must not be mistaken for end-of-pagination. Yielded URLs are
    globally de-duplicated so no document is emitted (or re-fetched) twice.
    """
    seen: set[str] = set()

    # Fetch page 1 and derive how many pages to walk from the total count.
    try:
        resp = _http_get(session, LIST_URL)
    except requests.RequestException as e:
        print(f"  Page 1: ERROR {e}")
        return
    if resp is None:
        print("  Page 1: throttled (429/5xx) after retries — aborting")
        return
    first_html = resp.text

    total = extract_total_count(first_html)
    first_links = extract_links_from_list_page(first_html)
    per_page = len(first_links) or 20
    if total:
        last_page = min(max_pages, (total + per_page - 1) // per_page)
        print(f"  Archive reports {total} documents → walking {last_page} pages")
    else:
        last_page = max_pages
        print(f"  Total count not found — walking up to {last_page} pages")

    for link in first_links:
        if link not in seen:
            seen.add(link)
            yield link

    empty_streak = 0
    for page in range(2, last_page + 1):
        links = _fetch_page_links(session, page)
        if not links:
            empty_streak += 1
            print(f"  Page {page}: empty after retries ({empty_streak} in a row)")
            # Only treat a long run of empties past the expected end as the true
            # end of data; within range, skip and continue.
            if empty_streak >= 15 and page > last_page - 20:
                print(f"  Page {page}: sustained empties near end — stopping")
                break
            time.sleep(RATE_LIMIT_DELAY)
            continue
        empty_streak = 0
        new = [l for l in links if l not in seen]
        for link in new:
            seen.add(link)
            yield link
        print(f"  Page {page}: {len(links)} links ({len(new)} new)")
        time.sleep(RATE_LIMIT_DELAY)


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all legislation documents with full text."""
    session = requests.Session()
    max_records = 15 if sample else 999999
    max_pages = 2 if sample else 1200
    count = 0
    skipped = 0

    print(f"Fetching LY/LawSocietyGazette ({'sample' if sample else 'full'} mode)...")
    print(f"Collecting legislation URLs from list pages...")

    for doc_url in get_list_page_urls(session, max_pages=max_pages):
        if count >= max_records:
            break

        try:
            resp = _http_get(session, doc_url)
        except requests.RequestException as e:
            print(f"    ERROR fetching {doc_url}: {e}")
            skipped += 1
            continue
        if resp is None:
            # Throttled past all retries — do NOT silently drop the document
            # (that truncation was issue #1165). Surface it as a skip with a
            # clear reason so a fleet re-run/resume can pick it up.
            print(f"    THROTTLED (429/5xx after retries), skipping: {doc_url[-60:]}")
            skipped += 1
            continue

        data = extract_text_from_page(resp.text)
        record = normalize(doc_url, data)

        if not record:
            print(f"    SKIP (insufficient text): {data['title'][:60] if data['title'] else doc_url[-50:]}")
            skipped += 1
            continue

        count += 1
        print(f"    [{count}] {record['title'][:70]}  ({len(record['text'])} chars)")
        yield record

    print(f"\nTotal: {count} records with full text, {skipped} skipped")


def test_connectivity() -> bool:
    """Test that the site is accessible and returns legislation data."""
    try:
        resp = requests.get(LIST_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        links = extract_links_from_list_page(resp.text)
        print(f"Connectivity OK: status {resp.status_code}, {len(links)} links on first page")

        if links:
            resp2 = requests.get(links[0], headers=HEADERS, timeout=30)
            resp2.raise_for_status()
            data = extract_text_from_page(resp2.text)
            print(f"Sample page OK: title='{data['title'][:60]}', text={len(data['text'])} chars")
            return len(data["text"]) > 100

        return len(links) > 0
    except Exception as e:
        print(f"Connectivity FAILED: {e}")
        return False


DATA_DIR = Path(__file__).parent / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"


def main():
    parser = argparse.ArgumentParser(description="LY/LawSocietyGazette bootstrap")
    # bootstrap-fast is the VPS fleet entrypoint; alias it to the full bootstrap
    # so it streams the whole corpus to data/records.jsonl (not just sample/).
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "test":
        ok = test_connectivity()
        sys.exit(0 if ok else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        # bootstrap-fast (fleet) and `bootstrap --full` do the full corpus and
        # stream to data/records.jsonl; plain `bootstrap` stays a sample run.
        full_mode = args.command == "bootstrap-fast" or args.full
        sample_mode = not full_mode

        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        records_out = None
        if full_mode:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            records_out = open(RECORDS_PATH, "w", encoding="utf-8")

        count = 0
        text_lens = []
        try:
            for record in fetch_all(sample=sample_mode):
                count += 1
                text_lens.append(len(record["text"]))
                if records_out is not None:
                    records_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    # Keep the first 15 as committed samples for validation.
                    if count > 15:
                        continue
                safe_id = re.sub(r"[^\w-]", "_", record["_id"])
                out_path = SAMPLE_DIR / f"{safe_id}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
        finally:
            if records_out is not None:
                records_out.close()

        if full_mode:
            print(f"\nWrote {count} records to {RECORDS_PATH}")
        else:
            print(f"\nSaved {count} records to {SAMPLE_DIR}/")

        if count:
            print(f"Text lengths: min={min(text_lens)}, max={max(text_lens)}, avg={sum(text_lens)//len(text_lens)}")
        else:
            print("WARNING: No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
