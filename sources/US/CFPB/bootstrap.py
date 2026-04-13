#!/usr/bin/env python3
"""
US/CFPB -- Consumer Financial Protection Bureau Enforcement Actions

Fetches CFPB enforcement actions via RSS feed index + HTML detail pages.
~386 enforcement actions with full narrative text, metadata, and PDF links.

Data access:
  - RSS feed at /enforcement/actions/feed/?page=N (25 items/page, ~16 pages)
  - Individual action pages with full text body + sidebar metadata
  - PDF documents on files.consumerfinance.gov (complaints, consent orders, etc.)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CFPB")

BASE_URL = "https://www.consumerfinance.gov"
RSS_URL = BASE_URL + "/enforcement/actions/feed/"
DELAY = 2.0
MAX_RSS_PAGES = 20


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def clean_html(text: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if HAS_BS4:
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text(separator="\n", strip=True)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z",  # RSS format
        "%B %d, %Y",
        "%b %d, %Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class CFPBScraper:
    SOURCE_ID = "US/CFPB"

    def __init__(self):
        self.session = get_session()

    def _get(self, url: str) -> Optional[requests.Response]:
        full_url = urljoin(BASE_URL, url) if url.startswith("/") else url
        for attempt in range(3):
            try:
                resp = self.session.get(full_url, timeout=30)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning("Rate limited, waiting %ds...", wait)
                    time.sleep(wait)
                    continue
                logger.warning("HTTP %d for %s", resp.status_code, full_url)
                return None
            except requests.RequestException as e:
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def fetch_rss_page(self, page: int) -> List[Dict[str, str]]:
        """Fetch one page of the RSS feed and return action metadata."""
        url = f"{RSS_URL}?page={page}"
        resp = self._get(url)
        if not resp:
            return []

        items = []
        try:
            root = ET.fromstring(resp.content)
            channel = root.find("channel")
            if channel is None:
                return []
            for item in channel.findall("item"):
                title = item.findtext("title", "").strip()
                link = item.findtext("link", "").strip()
                pub_date = item.findtext("pubDate", "").strip()
                description = item.findtext("description", "").strip()
                categories = [c.text.strip() for c in item.findall("category") if c.text]

                if link:
                    items.append({
                        "title": title,
                        "url": link,
                        "pub_date": pub_date,
                        "description": description,
                        "categories": categories,
                    })
        except ET.ParseError as e:
            logger.error("RSS parse error page %d: %s", page, e)

        return items

    def fetch_action_page(self, url: str) -> Dict[str, Any]:
        """Fetch an individual enforcement action page and extract full text + metadata."""
        resp = self._get(url)
        if not resp:
            return {}

        html = resp.text
        meta: Dict[str, Any] = {
            "body_text": "",
            "court": "",
            "docket_number": "",
            "filing_date": "",
            "status": "",
            "forum": "",
            "products": [],
            "pdf_urls": [],
        }

        if HAS_BS4:
            soup = BeautifulSoup(html, "html.parser")

            # Extract main body text
            content_div = soup.find("div", class_="o-post_body") or \
                          soup.find("article") or \
                          soup.find("main")
            if content_div:
                # Remove sidebar/aside elements
                for aside in content_div.find_all(["aside", "nav"]):
                    aside.decompose()
                meta["body_text"] = content_div.get_text(separator="\n", strip=True)

            # Extract sidebar metadata
            for dl in soup.find_all("dl"):
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds):
                    label = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    if "court" in label:
                        meta["court"] = value
                    elif "docket" in label:
                        meta["docket_number"] = value
                    elif "filing date" in label or "initial filing" in label:
                        meta["filing_date"] = value
                    elif "status" in label:
                        meta["status"] = value
                    elif "forum" in label:
                        meta["forum"] = value

            # Alternative metadata extraction from field divs
            for div in soup.find_all("div", class_=re.compile(r"m-related-metadata")):
                for item_div in div.find_all("div", class_="m-related-metadata__item"):
                    label_el = item_div.find(class_="m-related-metadata__label")
                    value_el = item_div.find(class_="m-related-metadata__value")
                    if not label_el or not value_el:
                        continue
                    label = label_el.get_text(strip=True).lower()
                    value = value_el.get_text(strip=True)
                    if "court" in label:
                        meta["court"] = value
                    elif "docket" in label:
                        meta["docket_number"] = value
                    elif "filing date" in label or "initial filing" in label:
                        meta["filing_date"] = value
                    elif "status" in label:
                        meta["status"] = value
                    elif "forum" in label:
                        meta["forum"] = value

            # Products
            for li in soup.select(".o-post_categories li, .m-tag-group li"):
                tag = li.get_text(strip=True)
                if tag:
                    meta["products"].append(tag)

            # PDF links
            for a in soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE)):
                href = a.get("href", "")
                if href:
                    if href.startswith("/"):
                        href = BASE_URL + href
                    meta["pdf_urls"].append(href)
        else:
            # Regex fallback for body text
            body_match = re.search(
                r'class="o-post_body"[^>]*>(.*?)</div>\s*</div>',
                html, re.DOTALL
            )
            if body_match:
                meta["body_text"] = clean_html(body_match.group(1))
            else:
                # Try main content area
                main_match = re.search(r"<main[^>]*>(.*?)</main>", html, re.DOTALL)
                if main_match:
                    meta["body_text"] = clean_html(main_match.group(1))

            # PDF links
            for pdf_match in re.finditer(r'href="([^"]*\.pdf[^"]*)"', html, re.IGNORECASE):
                href = pdf_match.group(1)
                if href.startswith("/"):
                    href = BASE_URL + href
                meta["pdf_urls"].append(href)

        return meta

    def normalize(self, rss_item: Dict[str, Any], page_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a CFPB enforcement action into standard schema."""
        url = rss_item["url"]
        slug = url.rstrip("/").split("/")[-1]

        # Use filing date from page if available, otherwise RSS pub date
        date = parse_date(page_data.get("filing_date", "")) or \
               parse_date(rss_item.get("pub_date", ""))

        # Build text: body text from the action page
        text = page_data.get("body_text", "").strip()

        # Determine data type based on categories
        categories = rss_item.get("categories", [])
        cat_lower = [c.lower() for c in categories]
        if any("administrative" in c for c in cat_lower) or \
           any("civil action" in c for c in cat_lower):
            data_type = "case_law"
        else:
            data_type = "doctrine"

        return {
            "_id": f"cfpb-{slug}",
            "_source": self.SOURCE_ID,
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": rss_item.get("title", ""),
            "text": text,
            "date": date,
            "url": url,
            "court": page_data.get("court", ""),
            "docket_number": page_data.get("docket_number", ""),
            "status": page_data.get("status", ""),
            "forum": page_data.get("forum", ""),
            "products": page_data.get("products", []),
            "categories": categories,
            "description": clean_html(rss_item.get("description", "")),
            "pdf_urls": page_data.get("pdf_urls", []),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all enforcement actions."""
        max_pages = 2 if sample else MAX_RSS_PAGES
        count = 0
        sample_limit = 15 if sample else 999999

        for page in range(1, max_pages + 1):
            logger.info("Fetching RSS page %d...", page)
            items = self.fetch_rss_page(page)
            if not items:
                logger.info("No more RSS items at page %d, stopping.", page)
                break

            for item in items:
                if count >= sample_limit:
                    return

                time.sleep(DELAY)
                logger.info("Fetching action: %s", item["title"][:80])
                page_data = self.fetch_action_page(item["url"])
                if not page_data:
                    logger.warning("Failed to fetch page: %s", item["url"])
                    continue

                record = self.normalize(item, page_data)
                if record["text"]:
                    yield record
                    count += 1
                else:
                    logger.warning("No text for: %s", item["title"])

            time.sleep(DELAY)

        logger.info("Fetched %d enforcement actions total.", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch enforcement actions newer than `since` date."""
        since_dt = datetime.fromisoformat(since)

        for page in range(1, MAX_RSS_PAGES + 1):
            logger.info("Fetching RSS page %d (updates since %s)...", page, since)
            items = self.fetch_rss_page(page)
            if not items:
                break

            all_old = True
            for item in items:
                item_date = parse_date(item.get("pub_date", ""))
                if item_date and datetime.fromisoformat(item_date) < since_dt:
                    continue
                all_old = False

                time.sleep(DELAY)
                page_data = self.fetch_action_page(item["url"])
                if not page_data:
                    continue

                record = self.normalize(item, page_data)
                if record["text"]:
                    yield record

            if all_old:
                logger.info("All items on page %d are older than %s, stopping.", page, since)
                break
            time.sleep(DELAY)

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._get(RSS_URL)
        if not resp:
            logger.error("Cannot reach CFPB RSS feed")
            return False
        try:
            root = ET.fromstring(resp.content)
            items = root.find("channel").findall("item")
            logger.info("RSS feed OK, %d items on first page", len(items))
            return len(items) > 0
        except Exception as e:
            logger.error("RSS parse failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/CFPB bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = CFPBScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        records = scraper.fetch_all(sample=args.sample)
    elif args.command == "update":
        if not args.since:
            logger.error("--since required for update")
            sys.exit(1)
        records = scraper.fetch_updates(args.since)
    else:
        sys.exit(1)

    count = 0
    for record in records:
        out_path = sample_dir / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info("Saved %s (%d chars text)", record["_id"], len(record.get("text", "")))

    logger.info("Done. %d records saved to %s", count, sample_dir)
    if count == 0:
        logger.error("No records fetched!")
        sys.exit(1)


if __name__ == "__main__":
    main()
