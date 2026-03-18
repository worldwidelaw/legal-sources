"""
World Wide Law - Bulgarian Data Protection Authority (CPDP) Scraper

Fetches decisions and opinions from the Bulgarian Commission for Personal Data Protection.
Data source: https://cpdp.bg (WordPress REST API)
Method: REST API via /wp-json/wp/v2/posts
Coverage: 2007 onwards (decisions and opinions)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from html import unescape

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BG/CPDP")


class BulgarianCPDPScraper(BaseScraper):
    """
    Scraper for: Bulgarian Commission for Personal Data Protection (КЗЛД)
    Country: BG
    URL: https://cpdp.bg

    Data types: regulatory_decisions
    Auth: none
    """

    # Category IDs for CPDP decisions by year (from WordPress API)
    DECISION_CATEGORIES = {
        199: "2007",  # Решения на КЗЛД за 2007 г. (1)
        189: "2008",  # Решения на КЗЛД за 2008 г. (51)
        197: "2009",  # Решения на КЗЛД за 2009 г. (60)
        222: "2010",  # Решения на КЗЛД за 2010 г. (92)
        232: "2011",  # Решения на КЗЛД за 2011 г. (153)
        201: "2012",  # Решения на КЗЛД за 2012 г. (220)
        243: "2013",  # Решения на КЗЛД за 2013 г. (167)
        248: "2014",  # Решения на КЗЛД за 2014 г. (131)
        258: "2015",  # Решения на КЗЛД за 2015 г. (22)
        264: "2016",  # Решения на КЗЛД за 2016 г. (3)
        202: "2017",  # Решения на КЗЛД за 2017 г. (14)
        191: "2018",  # Решения на КЗЛД за 2018 г. (22)
        282: "2019",  # Решения на КЗЛД за 2019 г. (50)
        280: "2020",  # Решения на КЗЛД за 2020 г. (18)
        221: "2021",  # Решения на КЗЛД за 2021 г. (18)
        278: "2022",  # Решения на КЗЛД за 2022 г. (24)
        291: "2023",  # Решения на КЗЛД за 2023 г. (7)
    }

    # Category IDs for CPDP opinions by year
    OPINION_CATEGORIES = {
        195: "2007",  # Становища на КЗЛД за 2007 г. (1)
        196: "2008",  # Становища на КЗЛД за 2008 г. (11)
        200: "2009",  # Становища на КЗЛД за 2009 г. (29)
        225: "2010",  # Становища на КЗЛД за 2010 г. (7)
        137: "2011",  # Становища на КЗЛД за 2011 г. (30)
        240: "2012",  # Становища на КЗЛД за 2012 г. (55)
        244: "2013",  # Становища на КЗЛД за 2013 г. (61)
        223: "2014",  # Становища на КЗЛД за 2014 г. (42)
        270: "2015",  # Становища на КЗЛД за 2015 г. (48)
        269: "2016",  # Становища на КЗЛД за 2016 г. (26)
        257: "2017",  # Становища на КЗЛД за 2017 г. (4)
        234: "2018",  # Становища на КЗЛД за 2018 г. (25)
        283: "2019",  # Становища на КЗЛД за 2019 г. (18)
        290: "2020",  # Становища на КЗЛД за 2020 г. (15)
        281: "2021",  # Становища на КЗЛД за 2021 г. (6)
        279: "2022",  # Становища на КЗЛД за 2022 г. (7)
        295: "2023",  # Становища на КЗЛД за 2023 г. (7)
        300: "2024",  # Становища на КЗЛД за 2024 г. (12)
        304: "2025",  # Становища на КЗЛД за 2025 г. (3)
    }

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # API client - disable SSL verification due to certificate issues
        self.api_client = HttpClient(
            base_url="https://cpdp.bg",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            },
            verify=False,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents by iterating through decision and opinion categories.
        """
        # First fetch all decisions
        logger.info("Fetching CPDP decisions...")
        for cat_id, year in self.DECISION_CATEGORIES.items():
            logger.info(f"Fetching decisions for year {year} (category {cat_id})")
            yield from self._fetch_category(cat_id, "decision", year)

        # Then fetch all opinions
        logger.info("Fetching CPDP opinions...")
        for cat_id, year in self.OPINION_CATEGORIES.items():
            logger.info(f"Fetching opinions for year {year} (category {cat_id})")
            yield from self._fetch_category(cat_id, "opinion", year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        Uses the WordPress API's after parameter.
        """
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")

        # Fetch recent decisions
        for cat_id, year in self.DECISION_CATEGORIES.items():
            yield from self._fetch_category(cat_id, "decision", year, after=since_iso)

        # Fetch recent opinions
        for cat_id, year in self.OPINION_CATEGORIES.items():
            yield from self._fetch_category(cat_id, "opinion", year, after=since_iso)

    def _fetch_category(
        self,
        category_id: int,
        doc_type: str,
        year: str,
        after: str = None,
    ) -> Generator[dict, None, None]:
        """
        Fetch all posts from a specific WordPress category.
        Uses pagination to get all posts.
        """
        page = 1
        per_page = 100
        total_fetched = 0

        while True:
            try:
                self.rate_limiter.wait()

                params = {
                    "categories": category_id,
                    "per_page": per_page,
                    "page": page,
                    "_fields": "id,date,title,content,link,categories",
                }
                if after:
                    params["after"] = after

                url = f"/wp-json/wp/v2/posts?{self._build_query(params)}"
                resp = self.api_client.get(url)

                # Check if response is JSON
                try:
                    posts = resp.json()
                except Exception:
                    logger.error(f"Failed to parse JSON for category {category_id} page {page}")
                    break

                if not posts or not isinstance(posts, list):
                    break

                for post in posts:
                    raw_doc = self._extract_post_data(post, doc_type, year)
                    if raw_doc and raw_doc.get("full_text"):
                        yield raw_doc
                        total_fetched += 1

                # Check if we got a full page (more pages might exist)
                if len(posts) < per_page:
                    break

                page += 1

                # Safety limit
                if page > 50:
                    logger.warning(f"Reached page limit for category {category_id}")
                    break

            except Exception as e:
                logger.error(f"Failed to fetch category {category_id} page {page}: {e}")
                break

        logger.info(f"Category {category_id} ({year}): fetched {total_fetched} documents")

    def _build_query(self, params: dict) -> str:
        """Build URL query string from params dict."""
        from urllib.parse import urlencode
        return urlencode(params)

    def _extract_post_data(self, post: dict, doc_type: str, year: str) -> dict:
        """
        Extract structured data from a WordPress post.
        """
        post_id = post.get("id")
        title_raw = post.get("title", {}).get("rendered", "")
        content_raw = post.get("content", {}).get("rendered", "")
        link = post.get("link", "")
        date_str = post.get("date", "")

        # Decode HTML entities in title
        title = unescape(title_raw).strip()

        # Clean HTML from content to get full text
        full_text = self._clean_html(content_raw)

        if not full_text or len(full_text) < 50:
            logger.warning(f"Post {post_id} has insufficient content")
            return None

        # Extract decision number from title if present
        decision_number = self._extract_decision_number(title)

        # Parse date from WordPress format (YYYY-MM-DDTHH:MM:SS)
        date_iso = None
        if date_str:
            try:
                # WordPress dates are in format: 2023-08-25T14:10:03
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        return {
            "post_id": post_id,
            "title": title,
            "full_text": full_text,
            "date": date_iso,
            "url": link,
            "doc_type": doc_type,  # "decision" or "opinion"
            "year": year,
            "decision_number": decision_number,
            "categories": post.get("categories", []),
        }

    def _clean_html(self, html_content: str) -> str:
        """
        Clean HTML content and extract plain text.
        Preserves paragraph structure.
        """
        if not html_content:
            return ""

        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove script and style elements
        for element in soup(["script", "style"]):
            element.decompose()

        # Get text with paragraph separators
        text = soup.get_text(separator="\n", strip=True)

        # Clean up whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Decode HTML entities
        text = unescape(text)

        # Replace special unicode spaces
        text = text.replace("\xa0", " ")
        text = text.replace("\u2003", " ")
        text = text.replace("\u00a0", " ")

        return text.strip()

    def _extract_decision_number(self, title: str) -> str:
        """
        Extract decision/opinion number from title.
        Examples:
        - "Решение по жалба с рег. № ППН-01-481/21.06.2018 г." -> "ППН-01-481/2018"
        - "Становище № 2024-01-01" -> "2024-01-01"
        """
        # Try various patterns
        patterns = [
            r"№\s*([A-ZА-Я]{2,}-\d+-\d+/\d{4})",  # ППН-01-481/2018
            r"№\s*([A-ZА-Я]{2,}-\d+-\d+)",  # ППН-01-481
            r"рег\.\s*№\s*([A-ZА-Я]{2,}-\d+-\d+)",  # rег. № ППН-01-481
            r"№\s*(\d{4}-\d{2}-\d{2})",  # 2024-01-01
            r"№\s*(\d+/\d{4})",  # 123/2024
            r"№\s*(\d+)",  # Just a number
        ]

        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                return match.group(1)

        return ""

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from document content.
        """
        post_id = raw.get("post_id", "")

        # Build unique ID
        doc_type = raw.get("doc_type", "decision")
        _id = f"BG/CPDP/{doc_type}/{post_id}"

        # Get full text
        full_text = raw.get("full_text", "")

        # Determine document type in Bulgarian
        type_bg = "Решение" if doc_type == "decision" else "Становище"

        # Convert year to integer
        year_str = raw.get("year", "")
        year_int = int(year_str) if year_str and year_str.isdigit() else None

        return {
            "_id": _id,
            "_source": "BG/CPDP",
            "_type": "regulation",  # Using "regulation" as closest valid type for DPA decisions
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": raw.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get("date"),
            "url": raw.get("url"),

            # Source-specific fields
            "post_id": post_id,
            "doc_type": doc_type,
            "doc_type_bg": type_bg,
            "year": year_int,
            "decision_number": raw.get("decision_number"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# -- CLI Entry Point -----------------------------------------------

def main():
    scraper = BulgarianCPDPScraper()

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
