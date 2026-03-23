#!/usr/bin/env python3
"""
CA/CanadaGazette -- Canada Gazette Part II Fetcher

Fetches official regulations from the Canada Gazette Part II via RSS feed
and HTML scraping. No auth required.

Strategy:
  - Parse RSS feed for gazette issue URLs
  - Scrape each issue index page for regulation links (SOR/DORS)
  - Fetch each regulation HTML page and extract full text

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.CanadaGazette")

BASE_URL = "https://gazette.gc.ca"


class CanadaGazetteScraper(BaseScraper):
    """
    Scraper for CA/CanadaGazette -- Canada Gazette Part II (Regulations).
    Country: CA
    URL: https://gazette.gc.ca

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- Helpers ------------------------------------------------------------

    def _get_rss_issues(self, max_issues=None):
        """Parse RSS feed for gazette issue URLs and dates."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/rss/p2-eng.xml")
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
        except Exception as e:
            logger.error(f"Failed to fetch RSS: {e}")
            return []

        issues = []
        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            title_elem = item.find("title")
            link_elem = item.find("link")
            pub_date_elem = item.find("pubDate")

            if link_elem is None or link_elem.text is None:
                continue

            title = title_elem.text if title_elem is not None else ""
            link = link_elem.text
            pub_date = pub_date_elem.text if pub_date_elem is not None else ""

            # Extract date from title or link (e.g., 2026-03-11)
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', link)
            date_str = date_match.group(1) if date_match else ""

            issues.append({
                "title": title,
                "url": link,
                "date": date_str,
                "pub_date": pub_date,
            })

            if max_issues and len(issues) >= max_issues:
                break

        logger.info(f"Found {len(issues)} gazette issues in RSS")
        return issues

    def _get_regulations_from_issue(self, issue_url):
        """Scrape an issue index page for individual regulation URLs."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(issue_url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch issue {issue_url}: {e}")
            return []

        # Find SOR/DORS regulation links (relative to the issue page)
        reg_links = re.findall(r'href="(sor-dors\d+-eng\.html)"', resp.text)
        # Also SI (Statutory Instruments)
        si_links = re.findall(r'href="(si-tr\d+-eng\.html)"', resp.text)

        all_links = list(set(reg_links + si_links))

        # Build full URLs
        base = issue_url.rsplit("/", 1)[0] + "/"
        regulations = []
        for link in all_links:
            regulations.append({
                "url": base + link,
                "filename": link,
            })

        return regulations

    def _fetch_regulation_text(self, url):
        """Fetch a regulation HTML page and extract title and clean text."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch regulation {url}: {e}")
            return None, ""

        html = resp.text

        # Extract title from <title> tag or <h1>
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if not title_match:
            title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
        title = ""
        if title_match:
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()

        # Extract main content - look for the regulation body
        # The content is typically in a <main> or specific div
        main_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL)
        if main_match:
            content = main_match.group(1)
        else:
            content = html

        # Clean HTML
        text = re.sub(r'<script[^>]*>.*?</script>', ' ', content, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<nav[^>]*>.*?</nav>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<header[^>]*>.*?</header>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<footer[^>]*>.*?</footer>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&#\d+;', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return title, text

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulations from all gazette issues."""
        issues = self._get_rss_issues()

        for issue in issues:
            issue_url = issue["url"]
            issue_date = issue["date"]
            logger.info(f"Processing gazette issue {issue_date}...")

            regulations = self._get_regulations_from_issue(issue_url)

            for reg in regulations:
                title, text = self._fetch_regulation_text(reg["url"])
                if text and len(text) >= 100:
                    # Extract regulation ID from filename
                    reg_id_match = re.search(r'(sor-dors\d+|si-tr\d+)', reg["filename"])
                    reg_id = reg_id_match.group(1) if reg_id_match else reg["filename"]

                    yield {
                        "regulation_id": reg_id,
                        "title": title,
                        "text": text,
                        "date": issue_date,
                        "url": reg["url"],
                        "gazette_issue": issue["title"],
                    }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch regulations from issues published since a date."""
        since_str = since.strftime("%Y-%m-%d")
        issues = self._get_rss_issues()

        for issue in issues:
            if issue["date"] >= since_str:
                issue_url = issue["url"]
                regulations = self._get_regulations_from_issue(issue_url)

                for reg in regulations:
                    title, text = self._fetch_regulation_text(reg["url"])
                    if text and len(text) >= 100:
                        reg_id_match = re.search(r'(sor-dors\d+|si-tr\d+)', reg["filename"])
                        reg_id = reg_id_match.group(1) if reg_id_match else reg["filename"]

                        yield {
                            "regulation_id": reg_id,
                            "title": title,
                            "text": text,
                            "date": issue["date"],
                            "url": reg["url"],
                            "gazette_issue": issue["title"],
                        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw gazette document into standard schema."""
        return {
            "_id": raw.get("regulation_id", ""),
            "_source": "CA/CanadaGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
            "regulation_id": raw.get("regulation_id", ""),
            "gazette_issue": raw.get("gazette_issue", ""),
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample records for validation."""
        samples = []
        issues = self._get_rss_issues(max_issues=5)

        for issue in issues:
            regulations = self._get_regulations_from_issue(issue["url"])
            logger.info(f"Issue {issue['date']}: {len(regulations)} regulations")

            for reg in regulations[:3]:  # Max 3 per issue
                title, text = self._fetch_regulation_text(reg["url"])
                if not text or len(text) < 100:
                    continue

                reg_id_match = re.search(r'(sor-dors\d+|si-tr\d+)', reg["filename"])
                reg_id = reg_id_match.group(1) if reg_id_match else reg["filename"]

                raw = {
                    "regulation_id": reg_id,
                    "title": title,
                    "text": text,
                    "date": issue["date"],
                    "url": reg["url"],
                    "gazette_issue": issue["title"],
                }
                normalized = self.normalize(raw)
                samples.append(normalized)
                logger.info(f"  {reg_id}: {title[:60]} ({len(text)} chars)")

                if len(samples) >= 12:
                    return samples

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CA/CanadaGazette data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = CanadaGazetteScraper()

    if args.command == "test-api":
        print("Testing Canada Gazette RSS feed...")
        issues = scraper._get_rss_issues(max_issues=3)
        if issues:
            print(f"OK: {len(issues)} issues found")
            for i in issues:
                print(f"  {i['date']}: {i['title'][:60]}")
        else:
            print("FAIL: No issues found")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                    assert s.get("date"), f"Missing date: {s['_id']}"
                print("All validation checks passed!")
            return

        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
