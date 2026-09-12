#!/usr/bin/env python3
"""
NO/Forbrukertilsynet - Norwegian Consumer Authority Decisions Fetcher

Fetches enforcement decisions (vedtak) from forbrukertilsynet.no:
  - Prohibition orders with coercive fines (forbudsvedtak med tvangsmulkt)
  - Administrative penalty decisions (overtredelsesgebyr)
  - Compliance orders and enforcement actions

Index method: Scrape vedtak listing page + archive pagination
Full text: HTML page content extraction
License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.forbrukertilsynet.no"
VEDTAK_INDEX = f"{BASE_URL}/lov-og-rett/vedtak"
VEDTAK_ARCHIVE = f"{BASE_URL}/dokumenttype/vedtak"
SOURCE_ID = "NO/Forbrukertilsynet"
SAMPLE_DIR = Path(__file__).parent / "sample"
REQUEST_DELAY = 2.0


class ForbrukertilsynetFetcher:
    """Fetcher for Norwegian Consumer Authority enforcement decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
        })

    def _get(self, url: str) -> requests.Response:
        """GET with retry and rate limiting."""
        time.sleep(REQUEST_DELAY)
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning(f"Retry {attempt + 1} for {url}: {e}")
                    time.sleep(5 * (attempt + 1))
                else:
                    raise

    def collect_vedtak_urls(self) -> List[str]:
        """Collect all vedtak URLs from the main listing page."""
        logger.info(f"Fetching vedtak index: {VEDTAK_INDEX}")
        resp = self._get(VEDTAK_INDEX)
        soup = BeautifulSoup(resp.text, "html.parser")

        urls = set()

        # Find all links that point to vedtak pages (HTML decisions)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)
            # Match vedtak HTML pages
            if "/lov-og-rett/vedtak/" in href and href != VEDTAK_INDEX:
                # Skip the index page itself and anchor-only links
                clean = href.split("#")[0].rstrip("/")
                if clean != VEDTAK_INDEX.rstrip("/"):
                    urls.add(clean)

        # Also try the archive pages for any we missed
        for page_num in range(1, 10):
            archive_url = f"{VEDTAK_ARCHIVE}/page/{page_num}" if page_num > 1 else VEDTAK_ARCHIVE
            try:
                resp = self._get(archive_url)
                page_soup = BeautifulSoup(resp.text, "html.parser")
                for a in page_soup.find_all("a", href=True):
                    href = a["href"]
                    if not href.startswith("http"):
                        href = urljoin(BASE_URL, href)
                    if "/lov-og-rett/vedtak/" in href:
                        clean = href.split("#")[0].rstrip("/")
                        if clean != VEDTAK_INDEX.rstrip("/"):
                            urls.add(clean)
                # Check if there's a next page
                if not page_soup.find("a", class_="next") and not page_soup.find("a", string=re.compile(r"Neste|›|»")):
                    # Also check for page links
                    page_links = page_soup.find_all("a", href=re.compile(rf"/dokumenttype/vedtak/page/\d+"))
                    max_page = max([int(re.search(r"/page/(\d+)", a["href"]).group(1))
                                    for a in page_links
                                    if re.search(r"/page/(\d+)", a["href"])], default=0)
                    if page_num >= max_page:
                        break
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch archive page {page_num}: {e}")
                break

        # Filter out PDF-only URLs
        html_urls = [u for u in urls if not u.lower().endswith(".pdf")]
        pdf_urls = [u for u in urls if u.lower().endswith(".pdf")]

        if pdf_urls:
            logger.info(f"Skipping {len(pdf_urls)} PDF-only decisions (need PDF library)")

        logger.info(f"Collected {len(html_urls)} HTML vedtak URLs")
        return sorted(html_urls)

    def _extract_text(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main article text from a vedtak page."""
        content = None

        # Try WordPress content selectors
        for selector in [
            ("div", {"class": re.compile(r"entry-content|post-content|article-content")}),
            ("div", {"class": re.compile(r"wp-block-group")}),
            ("article",),
            ("div", {"class": re.compile(r"content")}),
            ("main",),
        ]:
            if len(selector) == 1:
                content = soup.find(selector[0])
            else:
                content = soup.find(*selector)
            if content and len(content.get_text(strip=True)) > 200:
                break
            content = None

        if not content:
            return None

        # Remove navigation, footer, sidebar, scripts
        for tag in content.find_all(["nav", "footer", "aside", "script", "style", "noscript", "button", "form"]):
            tag.decompose()
        for tag in content.find_all(class_=re.compile(
            r"nav|menu|sidebar|footer|breadcrumb|cookie|banner|share|social|print|sr-only|header|related",
            re.I,
        )):
            tag.decompose()

        text = content.get_text(separator="\n", strip=True)
        lines = [line.strip() for line in text.splitlines()]
        lines = [line for line in lines if line]
        text = "\n".join(lines)

        return text if len(text) > 100 else None

    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publication date from meta tags or page text."""
        # Check meta tags first
        for tag in soup.find_all("meta"):
            name = (tag.get("name") or tag.get("property") or "").lower()
            val = tag.get("content", "")
            if val and ("published" in name or "date" in name or "time" in name):
                parsed = self._parse_date(val)
                if parsed:
                    return parsed

        # Check time elements
        for time_tag in soup.find_all("time"):
            dt = time_tag.get("datetime", "")
            if dt:
                parsed = self._parse_date(dt)
                if parsed:
                    return parsed

        # Check page text for Norwegian date patterns
        page_text = soup.get_text()
        for pattern in [
            r"Publisert:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})",
            r"Oppdatert:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})",
            r"Dato:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})",
            r"Sist endret:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})",
        ]:
            m = re.search(pattern, page_text)
            if m:
                d, mo, y = m.groups()
                return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

        return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse date string to ISO 8601."""
        if not date_str:
            return None
        # ISO format
        if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str[:10]
        # Norwegian format dd.mm.yyyy
        m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
        if m:
            d, mo, y = m.groups()
            return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
        # ISO datetime
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        return None

    def _extract_reference(self, url: str, title: str) -> Optional[str]:
        """Extract FOV reference number from URL or title."""
        for text in [url, title]:
            m = re.search(r"(fov-\d{4}-\d+[a-z0-9-]*)", text, re.I)
            if m:
                return m.group(1).upper().replace("FOV-", "FOV-")
        return None

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title."""
        # Try og:title first
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"].strip()
        # Try h1
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)
        # Fall back to <title>
        title_tag = soup.find("title")
        if title_tag:
            t = title_tag.get_text(strip=True)
            # Remove site name suffix
            for sep in [" - ", " | ", " – "]:
                if sep in t:
                    t = t.split(sep)[0].strip()
            return t
        return "Unknown"

    def fetch_decision(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single decision page."""
        try:
            resp = self._get(url)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        title = self._extract_title(soup)
        text = self._extract_text(soup)
        date = self._extract_date(soup)
        reference = self._extract_reference(url, title)

        if not text:
            logger.warning(f"No text extracted from {url}")
            return None

        doc_id = "NO-FBT-" + hashlib.md5(url.encode()).hexdigest()[:12]

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "reference": reference,
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all vedtak decisions."""
        urls = self.collect_vedtak_urls()
        logger.info(f"Fetching {len(urls)} decisions...")

        for i, url in enumerate(urls, 1):
            logger.info(f"[{i}/{len(urls)}] {url}")
            doc = self.fetch_decision(url)
            if doc:
                yield doc
            else:
                logger.warning(f"Skipped: {url}")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield decisions modified since a date."""
        since_date = datetime.fromisoformat(since).date()
        for doc in self.fetch_all():
            if doc.get("date"):
                try:
                    doc_date = datetime.fromisoformat(doc["date"]).date()
                    if doc_date >= since_date:
                        yield doc
                except (ValueError, TypeError):
                    yield doc
            else:
                yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Records are already normalized during fetch."""
        return raw


def bootstrap(sample: bool = False, full: bool = False, since: Optional[str] = None):
    """Main entry point."""
    fetcher = ForbrukertilsynetFetcher()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    if since:
        docs = fetcher.fetch_updates(since)
    else:
        docs = fetcher.fetch_all()

    count = 0
    max_docs = 15 if sample else None

    for doc in docs:
        count += 1
        text_len = len(doc.get("text", ""))
        logger.info(f"  → {doc['title'][:80]} | text={text_len} chars | date={doc.get('date')}")

        if sample:
            sample_path = SAMPLE_DIR / f"{doc['_id']}.json"
            with open(sample_path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

        if max_docs and count >= max_docs:
            logger.info(f"Sample limit reached ({max_docs})")
            break

    logger.info(f"Done: {count} decisions fetched")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="NO/Forbrukertilsynet bootstrap")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Save sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", type=str, help="Fetch updates since date (ISO 8601)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        count = bootstrap(sample=args.sample, full=args.full, since=args.since)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
