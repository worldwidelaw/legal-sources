#!/usr/bin/env python3
"""
SG/SICC -- Singapore International Commercial Court

Fetches judgments from the eLitigation platform at elitigation.sg/gdviewer/sicc.

Strategy:
  - Paginate through listing pages (10 per page, 25 pages, ~242 total)
  - Parse HTML cards for case title, citation, date, case number
  - Fetch individual judgment pages for full HTML text
  - Clean HTML to plain text
  - Normalize into standard schema

Data:
  - ~242 judgments from 2015–present
  - International commercial disputes, arbitration, cross-border matters
  - Full text available as HTML (no PDF extraction needed)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch orders from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SG.SICC")

BASE_URL = "https://www.elitigation.sg/gdviewer"
LISTING_URL = f"{BASE_URL}/Home/Index"
SICC_URL = f"{BASE_URL}/sicc"


class SICCScraper:
    """Scraper for SG/SICC -- Singapore International Commercial Court."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "text/html, */*",
        })

    def _parse_listing_page(self, page: int) -> list:
        """Fetch and parse a single listing page of SICC judgments."""
        params = {
            "Filter": "sicc",
            "YearOfDecision": "All",
            "SortBy": "DateOfDecision",
            "CurrentPage": str(page),
            "SortAscending": "False",
            "PageSize": "0",
            "Verbose": "False",
            "SearchQueryTime": "0",
            "SearchTotalHits": "0",
            "SearchMode": "True",
            "SpanMultiplePages": "False",
        }

        try:
            resp = self.session.get(LISTING_URL, params=params, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch listing page %d: %s", page, e)
            return []

        html = resp.text
        entries = []

        # Split into card blocks using the judgment URL pattern
        parts = re.split(r"href='/gdviewer/sicc/", html)
        for part in parts[1:]:
            # Extract URL slug
            url_match = re.match(r"([^']+)'", part)
            if not url_match:
                continue
            slug = url_match.group(1)

            # Title
            title_match = re.search(r'gd-heardertext[^>]*>(.*?)</a>', part, re.S)
            title = ""
            if title_match:
                title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
                title = html_mod.unescape(title)
                title = re.sub(r'\s+', ' ', title)

            # Citation
            cit_match = re.search(r'citation-num-link.*?gd-addinfo-text[^>]*>([^<]+)', part, re.S)
            citation = ""
            if cit_match:
                citation = cit_match.group(1).strip().rstrip(' |').strip()
                citation = html_mod.unescape(citation)

            # Decision date
            date_match = re.search(r'DecisionDate:(?:&quot;|&#34;)(\d{4}-\d{2}-\d{2})(?:&quot;|&#34;)', part)
            date = date_match.group(1) if date_match else ""

            # Fallback date from text
            if not date:
                date_text = re.search(r'Decision Date:\s*(\d{1,2}\s+\w+\s+\d{4})', part)
                if date_text:
                    date = self._parse_date(date_text.group(1))

            # Case number
            case_match = re.search(r'case-num-link.*?gd-addinfo-text[^>]*>([^<]+)', part, re.S)
            case_no = ""
            if case_match:
                case_no = case_match.group(1).strip()
                case_no = html_mod.unescape(case_no)

            # Catchwords/subject
            catchwords = re.findall(r'gd-cw[^>]*data-searchterm="([^"]*)"', part)
            subject = ", ".join(html_mod.unescape(c) for c in catchwords) if catchwords else ""

            entries.append({
                "slug": slug,
                "title": title,
                "citation": citation,
                "date": date or "",
                "case_no": case_no,
                "subject": subject,
            })

        logger.info("Listing page %d: %d entries", page, len(entries))
        return entries

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%d %b, %Y"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _fetch_judgment_text(self, slug: str) -> Optional[str]:
        """Fetch the full text of a judgment from its detail page."""
        url = f"{SICC_URL}/{slug}"
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch judgment %s: %s", slug, e)
            return None

        html = resp.text

        # Remove script and style tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.S)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.S)
        text = re.sub(r'<nav[^>]*>.*?</nav>', '', text, flags=re.S)
        text = re.sub(r'<header[^>]*>.*?</header>', '', text, flags=re.S)
        text = re.sub(r'<footer[^>]*>.*?</footer>', '', text, flags=re.S)

        # Try to find the judgment body specifically
        # Look for content after "This judgment text has undergone conversion"
        body_start = text.find("IN THE SINGAPORE")
        if body_start < 0:
            body_start = text.find("This judgment")
        if body_start < 0:
            body_start = text.find("GROUNDS OF DECISION")
        if body_start < 0:
            body_start = text.find("JUDGMENT")

        if body_start > 0:
            text = text[body_start:]

        # Find end of judgment (before footer/navigation)
        body_end = text.find("Back to Top")
        if body_end < 0:
            body_end = text.find("Judgments Homepage")
        if body_end > 0:
            text = text[:body_end]

        # Clean HTML tags
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'<p[^>]*>', '\n', text)
        text = re.sub(r'</p>', '\n', text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html_mod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        if len(text) > 200:
            return text
        return None

    def _get_total_pages(self) -> int:
        """Detect total pages from the listing."""
        try:
            resp = self.session.get(f"{SICC_URL}", timeout=30)
            resp.raise_for_status()
            pages = re.findall(r'CurrentPage=(\d+)', resp.text)
            if pages:
                return max(int(p) for p in pages)
        except Exception as e:
            logger.error("Failed to detect total pages: %s", e)
        return 25  # fallback

    def fetch_all(self) -> Generator:
        """Yield all SICC judgment entries."""
        total_pages = self._get_total_pages()
        logger.info("Total listing pages: %d", total_pages)

        for page in range(1, total_pages + 1):
            entries = self._parse_listing_page(page)
            for entry in entries:
                yield entry
            time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield judgments from recent pages until we pass the since date."""
        for page in range(1, 50):
            entries = self._parse_listing_page(page)
            if not entries:
                break

            all_old = True
            for entry in entries:
                entry_date = entry.get("date", "")
                if entry_date:
                    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
                    if entry_dt >= since.replace(tzinfo=None):
                        all_old = False
                        yield entry
                    else:
                        continue
                else:
                    yield entry
                    all_old = False

            if all_old:
                logger.info("All entries on page %d are older than %s", page, since.isoformat())
                break
            time.sleep(1.5)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        slug = raw.get("slug", "")
        citation = raw.get("citation", "")
        title = raw.get("title", "")

        # Build unique ID from slug
        doc_id = f"SICC-{slug}"

        # Fetch full text
        text = self._fetch_judgment_text(slug)
        if not text:
            logger.warning("No text extracted for %s (%s)", doc_id, citation)
            return None

        # Build display title
        display_title = title
        if citation:
            display_title = f"{title} {citation}"

        return {
            "_id": doc_id,
            "_source": "SG/SICC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": display_title,
            "text": text,
            "date": raw.get("date", ""),
            "url": f"https://www.elitigation.sg/gdviewer/sicc/{slug}",
            "citation": citation,
            "case_no": raw.get("case_no", ""),
            "subject": raw.get("subject", ""),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="SG/SICC bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent judgments")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = SICCScraper()

    if args.command == "test":
        logger.info("Testing SICC/eLitigation connectivity...")
        entries = scraper._parse_listing_page(1)
        logger.info("Page 1: %d entries", len(entries))
        if entries:
            s = entries[0]
            logger.info("Sample: %s | %s | %s", s["citation"], s["title"], s["date"])
        total = scraper._get_total_pages()
        logger.info("Total pages: %d (approx %d judgments)", total, total * 10)
        logger.info("Test PASSED")

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        limit = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                out_path = sample_dir / f"{rec['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] Saved %s (%d chars text)", count, rec["_id"],
                            len(rec.get("text", "")))
                if count >= limit:
                    break

        logger.info("Bootstrap complete: %d records saved to %s", count, sample_dir)

    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0

        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                out_path = sample_dir / f"{rec['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] Saved %s", count, rec["_id"])

        logger.info("Update complete: %d records saved", count)

    else:
        parser.print_help()
