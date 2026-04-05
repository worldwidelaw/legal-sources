"""
Legal Data Hunter - Algeria Supreme Court Decisions Scraper

Fetches case law from the Algeria Supreme Court (المحكمة العليا / Cour Suprême).
Data source: WordPress REST API at coursupreme.dz
Method: JSON API pagination (wp/v2/decision endpoint)
Coverage: ~1,261 decisions (2000-2023) with full text in Arabic
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DZ/SupremeCourt-Decisions")


class AlgeriaSupremeCourtScraper(BaseScraper):
    """
    Scraper for: Algeria Supreme Court (المحكمة العليا)
    Country: DZ
    URL: https://coursupreme.dz

    Data types: case_law
    Auth: none
    """

    BASE_URL = "https://coursupreme.dz"
    PAGE_SIZE = 100

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            },
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions by paginating through the WP REST API."""
        page = 1
        total = 0

        while True:
            self.rate_limiter.wait()
            try:
                url = f"/wp-json/wp/v2/decision?per_page={self.PAGE_SIZE}&page={page}"
                resp = self.client.get(url)

                if resp.status_code == 400:
                    # WP returns 400 when page exceeds total
                    break

                resp.raise_for_status()
                decisions = resp.json()

                if not decisions:
                    break

                for d in decisions:
                    total += 1
                    yield d

                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                logger.info(f"Page {page}/{total_pages}: fetched {len(decisions)} decisions")

                if page >= total_pages:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

        logger.info(f"Total fetched: {total} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions modified since the given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1

        while True:
            self.rate_limiter.wait()
            try:
                url = f"/wp-json/wp/v2/decision?per_page={self.PAGE_SIZE}&page={page}&modified_after={since_str}"
                resp = self.client.get(url)

                if resp.status_code == 400:
                    break

                resp.raise_for_status()
                decisions = resp.json()

                if not decisions:
                    break

                for d in decisions:
                    yield d

                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch updates page {page}: {e}")
                break

    def normalize(self, raw: dict) -> dict:
        """Transform a WP REST API decision into standard schema."""
        wp_id = raw.get("id", 0)
        title_raw = raw.get("title", {}).get("rendered", "")
        content_html = raw.get("content", {}).get("rendered", "")
        date_str = raw.get("date", "")
        link = raw.get("link", "")

        # Clean title
        title = BeautifulSoup(title_raw, "html.parser").get_text(strip=True)

        # Clean content - strip HTML and VC shortcodes
        text = self._clean_content(content_html)

        # Parse date
        date_iso = ""
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str)
                date_iso = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        # Extract structured fields from text
        decision_number = self._extract_field(text, r"رقم القرار[:\s]*([^\n]+)")
        decision_date = self._extract_field(text, r"تاريخ القرار[:\s]*([^\n]+)")
        subject = self._extract_field(text, r"الموضوع[:\s]*([^\n]+)")

        # Get taxonomy terms
        chambers = []
        for tax in ["civil-chambers", "criminal-chambers", "combined-chambers"]:
            terms = raw.get(tax, [])
            if terms:
                chambers.extend([str(t) for t in terms])

        return {
            "_id": str(wp_id),
            "_source": "DZ/SupremeCourt-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or f"Decision {wp_id}",
            "text": text,
            "date": date_iso,
            "url": link or f"{self.BASE_URL}/?p={wp_id}",
            "wp_id": wp_id,
            "decision_number": decision_number,
            "decision_date": decision_date,
            "subject": subject,
            "chamber_ids": chambers,
        }

    def _clean_content(self, html: str) -> str:
        """Strip HTML tags and VC shortcodes from content."""
        # Remove Visual Composer shortcodes [vc_*]...[/vc_*]
        text = re.sub(r"\[/?vc_[^\]]*\]", "", html)
        # Remove other shortcodes
        text = re.sub(r"\[/?[a-zA-Z_][^\]]*\]", "", text)
        # Parse remaining HTML
        soup = BeautifulSoup(text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        # Remove zero-width chars
        text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
        return text

    def _extract_field(self, text: str, pattern: str) -> str:
        """Extract a named field from Arabic text."""
        match = re.search(pattern, text)
        return match.group(1).strip() if match else ""


# ── CLI Entry Point ──────────────────────────────────────────────

def main():
    scraper = AlgeriaSupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
