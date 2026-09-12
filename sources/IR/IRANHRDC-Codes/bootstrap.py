#!/usr/bin/env python3
"""
IR/IRANHRDC-Codes — Iran Human Rights Documentation Center — Iranian Legal Codes

Fetches English translations of major Iranian legal codes from iranhrdc.org.
~20 documents including Constitution, Penal Code, Civil Code, Press Law, etc.

Strategy:
  - Scrape paginated category listing pages (2 pages)
  - For each code, fetch the detail page
  - Extract full text from entry-content div (HTML)
  - If inline text is too short (<1000 chars) and PDF link exists, download and extract PDF

Source: https://iranhrdc.org/category/english/human-rights-documents/iranian-codes/
Rate limit: 1 req/sec

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

SOURCE_ID = "IR/IRANHRDC-Codes"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IR.IRANHRDC-Codes")

BASE_URL = "https://iranhrdc.org"
CATEGORY_URL = f"{BASE_URL}/category/english/human-rights-documents/iranian-codes/"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

RATE_LIMIT = 1.5
_last_request = 0.0


def _throttle():
    global _last_request
    now = time.time()
    wait = RATE_LIMIT - (now - _last_request)
    if wait > 0:
        time.sleep(wait)
    _last_request = time.time()


def _get(url, **kwargs):
    _throttle()
    resp = SESSION.get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


def get_listing_urls():
    """Scrape the category pages to get all document URLs."""
    urls = []
    page = 1
    while True:
        if page == 1:
            url = CATEGORY_URL
        else:
            url = f"{CATEGORY_URL}page/{page}/"

        logger.info(f"Fetching listing page {page}: {url}")
        try:
            resp = _get(url)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                break
            raise

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find document links — site uses h2 > a pattern
        links = soup.select("h2 a[href]")
        if not links:
            break
        for link in links:
            href = link.get("href", "")
            if not href:
                continue
            # Resolve relative URLs
            full_url = urljoin(url, href)
            # Skip non-article links (pagination, categories, etc.)
            if "/category/" in full_url or "/page/" in full_url:
                continue
            title = link.get_text(strip=True)
            if title:
                urls.append({"url": full_url, "title": title})

        # Check for next page
        next_link = soup.find("a", class_="next") or soup.find("a", string=re.compile(r"Next|»|Older"))
        if not next_link:
            # Also check for pagination links
            nav = soup.find("nav", class_="navigation") or soup.find("div", class_="nav-links")
            if nav:
                next_a = nav.find("a", class_="next")
                if not next_a:
                    break
            else:
                break

        page += 1
        if page > 10:  # Safety limit
            break

    logger.info(f"Found {len(urls)} document URLs across {page} pages")
    return urls


def extract_document(url, title_hint=""):
    """Fetch a document page and extract full text."""
    logger.info(f"Fetching document: {url}")
    resp = _get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Get title
    title_el = soup.find("h1", class_="entry-title") or soup.find("h1")
    title = title_el.get_text(strip=True) if title_el else title_hint

    # Get date
    date_str = None
    time_el = soup.find("time", class_="entry-date")
    if time_el:
        date_str = time_el.get("datetime", time_el.get_text(strip=True))
    if not date_str:
        date_meta = soup.find("meta", property="article:published_time")
        if date_meta:
            date_str = date_meta.get("content", "")

    # Parse date
    pub_date = None
    if date_str:
        for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%d", "%B %d, %Y"]:
            try:
                pub_date = datetime.strptime(date_str[:25], fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Extract content from entry-content div
    content_div = soup.find(class_="entry-content")
    if not content_div:
        content_div = soup.find("div", class_="post-content")

    text = ""
    if content_div:
        # Remove script/style/nav elements
        for tag in content_div.find_all(["script", "style", "nav", "aside"]):
            tag.decompose()

        # Remove download/attachment sections at the end
        for el in content_div.find_all(["div", "p", "span"], string=re.compile(r"Download|Attachment|Share this", re.I)):
            try:
                parent = el.parent if el.parent and el.parent != content_div else el
                if parent and parent != content_div:
                    parent.decompose()
                elif el.parent:
                    el.decompose()
            except Exception:
                pass

        text = content_div.get_text(separator="\n", strip=True)

        # Clean up artifacts
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"Download Complete Document.*$", "", text, flags=re.DOTALL)
        text = re.sub(r"Download Attachments:.*$", "", text, flags=re.DOTALL)
        text = re.sub(r"Share this:.*$", "", text, flags=re.DOTALL)
        text = text.strip()

    # Generate slug from URL
    slug = url.rstrip("/").split("/")[-1]

    return {
        "_id": slug,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": pub_date,
        "url": url,
        "language": "en",
        "original_language": "fa",
        "slug": slug,
    }


def fetch_all(sample=False):
    """Fetch all Iranian legal codes."""
    listings = get_listing_urls()
    if not listings:
        logger.error("No documents found in listing pages")
        return

    seen_urls = set()
    count = 0
    for item in listings:
        url = item["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        try:
            doc = extract_document(url, title_hint=item.get("title", ""))
            if doc and doc.get("text") and len(doc["text"]) > 100:
                yield doc
                count += 1
                logger.info(f"[{count}] {doc['title'][:60]} — {len(doc['text'])} chars")
            else:
                logger.warning(f"Skipping {url}: text too short ({len(doc.get('text',''))} chars)")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            continue

    logger.info(f"Total documents fetched: {count}")


def save_sample(records, output_dir):
    """Save sample records as JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        fname = re.sub(r"[^\w\-]", "_", rec["_id"])[:80] + ".json"
        path = output_dir / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved: {path.name}")


def test_api():
    """Quick connectivity test."""
    print(f"Testing {CATEGORY_URL} ...")
    resp = _get(CATEGORY_URL)
    print(f"Status: {resp.status_code}")
    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.find_all("article")
    links = soup.select("h2 a")
    print(f"Articles found: {len(articles)}")
    print(f"H2 links found: {len(links)}")
    for link in links[:3]:
        print(f"  - {link.get_text(strip=True)[:60]}")
    print("API test passed." if (articles or links) else "WARNING: No articles found!")


def main():
    parser = argparse.ArgumentParser(description="IR/IRANHRDC-Codes bootstrapper")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Save sample records only")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap (same as no flag for this small source)")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        records = list(fetch_all(sample=args.sample))
        if records:
            save_sample(records, SAMPLE_DIR)
            print(f"\nBootstrap complete: {len(records)} records saved to {SAMPLE_DIR}")
            text_lens = [len(r.get("text", "")) for r in records]
            print(f"Text lengths: min={min(text_lens)}, max={max(text_lens)}, avg={sum(text_lens)//len(text_lens)}")
        else:
            print("ERROR: No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
