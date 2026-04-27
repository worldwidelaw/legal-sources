#!/usr/bin/env python3
"""
US/CFTC -- Commodity Futures Trading Commission Enforcement Actions

Fetches CFTC enforcement actions from the listing pages + press release full text.
~2,590 enforcement actions spanning 1995-present.

Data access:
  - HTML listing at /LawRegulation/EnforcementActions/index.htm?page=N (259 pages)
  - Press release pages at /PressRoom/PressReleases/XXXX-YY with full narrative text
  - PDF documents at /media/{id}/{filename}/download

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
logger = logging.getLogger("legal-data-hunter.US.CFTC")

BASE_URL = "https://www.cftc.gov"
LISTING_URL = BASE_URL + "/LawRegulation/EnforcementActions/index.htm"
DELAY = 2.0
MAX_PAGES = 260


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
    """Strip HTML tags and clean whitespace."""
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
    if not date_str:
        return None
    date_str = date_str.strip()
    # Handle ISO datetime with timezone (e.g. 2026-04-02T16:12:42Z)
    if "T" in date_str:
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except ValueError:
            pass
    for fmt in ["%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class CFTCScraper:
    SOURCE_ID = "US/CFTC"

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

    def parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse one listing page to extract action entries."""
        entries = []

        if HAS_BS4:
            soup = BeautifulSoup(html, "html.parser")

            # Find table rows or view rows
            for row in soup.select("tr, .views-row"):
                entry = {"date": "", "title": "", "press_url": "", "pdf_urls": []}

                # Date from <time> element
                time_el = row.find("time")
                if time_el:
                    entry["date"] = time_el.get("datetime", "") or time_el.get_text(strip=True)

                # Links
                for a in row.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)

                    if "/PressRoom/PressReleases/" in href:
                        entry["press_url"] = href if href.startswith("http") else BASE_URL + href
                        if not entry["title"]:
                            entry["title"] = text
                    elif "/media/" in href and "/download" in href:
                        full_url = href if href.startswith("http") else BASE_URL + href
                        entry["pdf_urls"].append(full_url)
                    elif href.startswith("/LawRegulation/") and text and not entry["title"]:
                        entry["title"] = text

                if entry["press_url"] or entry["pdf_urls"]:
                    entries.append(entry)
        else:
            # Regex fallback
            for row_match in re.finditer(
                r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL
            ):
                row_html = row_match.group(1)
                entry = {"date": "", "title": "", "press_url": "", "pdf_urls": []}

                time_match = re.search(r'datetime="([^"]*)"', row_html)
                if time_match:
                    entry["date"] = time_match.group(1)

                for a_match in re.finditer(r'href="([^"]*)"[^>]*>([^<]*)', row_html):
                    href, text = a_match.group(1), a_match.group(2).strip()
                    if "/PressRoom/PressReleases/" in href:
                        entry["press_url"] = href if href.startswith("http") else BASE_URL + href
                        if not entry["title"]:
                            entry["title"] = text
                    elif "/media/" in href and "/download" in href:
                        full_url = href if href.startswith("http") else BASE_URL + href
                        entry["pdf_urls"].append(full_url)

                if entry["press_url"] or entry["pdf_urls"]:
                    entries.append(entry)

        return entries

    def fetch_press_release(self, url: str) -> str:
        """Fetch a press release page and extract the full text."""
        resp = self._get(url)
        if not resp:
            return ""

        html = resp.text

        if HAS_BS4:
            soup = BeautifulSoup(html, "html.parser")

            # The CFTC site uses JS-heavy rendering but <p> tags with actual
            # content ARE present inside div.field--name-body
            body_div = soup.select_one("div.field--name-body")
            if body_div:
                paragraphs = []
                for p in body_div.find_all("p"):
                    text = p.get_text(strip=True)
                    if text:
                        paragraphs.append(text)
                if paragraphs:
                    return "\n\n".join(paragraphs)

            # Fallback: look for press-release div
            pr_div = soup.select_one("div.press-release")
            if pr_div:
                paragraphs = [p.get_text(strip=True) for p in pr_div.find_all("p") if p.get_text(strip=True)]
                if paragraphs:
                    return "\n\n".join(paragraphs)

            # Last resort: all <p> in main
            main = soup.find("main") or soup.find("body")
            if main:
                paragraphs = [p.get_text(strip=True) for p in main.find_all("p") if len(p.get_text(strip=True)) > 50]
                return "\n\n".join(paragraphs)

            return ""
        else:
            # Regex fallback: extract <p> tags from within field--name-body
            body_match = re.search(
                r'field--name-body[^>]*>(.*?)</div>\s*</div>',
                html, re.DOTALL
            )
            search_html = body_match.group(1) if body_match else html
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', search_html, re.DOTALL)
            if paragraphs:
                cleaned = [clean_html(p) for p in paragraphs if len(clean_html(p)) > 20]
                return "\n\n".join(cleaned)

            return ""

    def normalize(self, entry: Dict[str, Any], press_text: str) -> Dict[str, Any]:
        """Normalize a CFTC enforcement action into standard schema."""
        # Generate ID from press URL or title
        if entry.get("press_url"):
            slug = entry["press_url"].rstrip("/").split("/")[-1]
            _id = f"cftc-{slug}"
        else:
            slug = re.sub(r"[^a-z0-9]+", "-", entry.get("title", "unknown").lower())[:80]
            _id = f"cftc-{slug}"

        date = parse_date(entry.get("date", ""))

        return {
            "_id": _id,
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": entry.get("title", ""),
            "text": press_text,
            "date": date,
            "url": entry.get("press_url", ""),
            "pdf_urls": entry.get("pdf_urls", []),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        max_pages = 2 if sample else MAX_PAGES
        count = 0
        sample_limit = 15 if sample else 999999

        for page in range(0, max_pages):
            logger.info("Fetching listing page %d...", page)
            url = f"{LISTING_URL}?page={page}"
            resp = self._get(url)
            if not resp:
                logger.warning("Failed to fetch listing page %d", page)
                break

            entries = self.parse_listing_page(resp.text)
            if not entries:
                logger.info("No entries on page %d, stopping.", page)
                break

            for entry in entries:
                if count >= sample_limit:
                    return

                if entry.get("press_url"):
                    time.sleep(DELAY)
                    logger.info("Fetching press release: %s", entry["title"][:80])
                    press_text = self.fetch_press_release(entry["press_url"])
                else:
                    press_text = ""

                if not press_text:
                    logger.warning("No text for: %s", entry.get("title", "unknown"))
                    continue

                record = self.normalize(entry, press_text)
                yield record
                count += 1

            time.sleep(DELAY)

        logger.info("Fetched %d enforcement actions total.", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        since_dt = datetime.fromisoformat(since)

        for page in range(0, MAX_PAGES):
            logger.info("Fetching listing page %d (updates since %s)...", page, since)
            url = f"{LISTING_URL}?page={page}"
            resp = self._get(url)
            if not resp:
                break

            entries = self.parse_listing_page(resp.text)
            if not entries:
                break

            all_old = True
            for entry in entries:
                entry_date = parse_date(entry.get("date", ""))
                if entry_date and datetime.fromisoformat(entry_date) < since_dt:
                    continue
                all_old = False

                if entry.get("press_url"):
                    time.sleep(DELAY)
                    press_text = self.fetch_press_release(entry["press_url"])
                else:
                    press_text = ""

                if not press_text:
                    continue

                record = self.normalize(entry, press_text)
                yield record

            if all_old:
                logger.info("All entries on page %d are older than %s, stopping.", page, since)
                break
            time.sleep(DELAY)

    def test(self) -> bool:
        resp = self._get(LISTING_URL)
        if not resp:
            logger.error("Cannot reach CFTC listing page")
            return False
        entries = self.parse_listing_page(resp.text)
        logger.info("Listing page OK, %d entries found", len(entries))
        return len(entries) > 0


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/CFTC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CFTCScraper()

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
