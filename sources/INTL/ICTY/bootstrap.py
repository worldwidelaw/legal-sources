#!/usr/bin/env python3
"""
INTL/ICTY -- ICTY Judgments (International Criminal Tribunal for former Yugoslavia)

Fetches full-text judgments from the archived ICTY website (icty.org) and
document metadata from the UCR API (ucr.irmct.org).

Strategy:
  - Scrape case list from icty.org/en/cases (86 case slugs)
  - For cases with HTML judgments: fetch full text from /x/cases/{slug}/tjug/en/
  - Also fetch appeal judgments from /x/cases/{slug}/acjug/en/ where available
  - Cases without HTML have PDF-only documents (text extraction not available)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records (archive is static)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ICTY")

CASES_URL = "https://www.icty.org/en/cases"
JUDGMENT_BASE = "https://www.icty.org/x/cases/{slug}/{jtype}/en/"
UCR_API_URL = "https://ucr.irmct.org/api/Summary/ByCaseDocsByLang"

# Judgment types to fetch from the legacy HTML site
JUDGMENT_TYPES = [
    ("tjug", "Trial Judgment"),
    ("acjug", "Appeal Judgment"),
]


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    if not text:
        return ""
    # Replace block elements with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = html_mod.unescape(text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract date from ICTY judgment filename like tad-tj970507e.htm -> 1997-05-07."""
    m = re.search(r'(\d{6})', filename)
    if m:
        digits = m.group(1)
        yy = int(digits[:2])
        mm = int(digits[2:4])
        dd = int(digits[4:6])
        year = 1900 + yy if yy > 50 else 2000 + yy
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{year}-{mm:02d}-{dd:02d}"
    return None


class ICTYScraper(BaseScraper):
    """
    Scraper for INTL/ICTY -- ICTY Judgments.
    Country: INTL
    URL: https://www.icty.org/en/cases

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html, application/json, */*",
        })

    def _get_case_slugs(self) -> list:
        """Get all case slugs from the ICTY cases page."""
        logger.info("Fetching case list from %s", CASES_URL)
        self.rate_limiter.wait()
        resp = self.session.get(CASES_URL, timeout=30)
        resp.raise_for_status()
        slugs = sorted(set(re.findall(r'/en/case/([a-z_-]+)', resp.text)))
        logger.info("Found %d case slugs", len(slugs))
        return slugs

    def _get_judgment_files(self, slug: str, jtype: str) -> list:
        """Get list of judgment HTML files from a case's contents page."""
        url = JUDGMENT_BASE.format(slug=slug, jtype=jtype) + "contents.htm"
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                return []
            # Extract unique .htm file references
            files = set(re.findall(r'([a-zA-Z0-9_.-]+\.htm)', resp.text, re.IGNORECASE))
            files.discard("contents.htm")
            files.discard("foot.htm")
            return sorted(files)
        except Exception as e:
            logger.debug("No %s contents for %s: %s", jtype, slug, e)
            return []

    def _fetch_judgment_text(self, slug: str, jtype: str, files: list) -> str:
        """Fetch and combine text from all judgment HTML files for a case."""
        base_url = JUDGMENT_BASE.format(slug=slug, jtype=jtype)
        all_text = []

        for filename in files:
            url = base_url + filename
            self.rate_limiter.wait()
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    continue
                text = strip_html(resp.text)
                if len(text) > 100:  # Skip trivially short pages
                    all_text.append(text)
            except Exception as e:
                logger.warning("Failed to fetch %s: %s", url, e)

        return "\n\n".join(all_text)

    def _format_case_name(self, slug: str) -> str:
        """Convert slug to a readable case name."""
        name = slug.replace("_old", "").replace("_", " ").title()
        return f"Prosecutor v. {name}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ICTY judgments with full text from HTML sources."""
        slugs = self._get_case_slugs()

        for i, slug in enumerate(slugs):
            logger.info("Processing case %d/%d: %s", i + 1, len(slugs), slug)

            for jtype, jtype_label in JUDGMENT_TYPES:
                files = self._get_judgment_files(slug, jtype)
                if not files:
                    continue

                # Fetch full text
                text = self._fetch_judgment_text(slug, jtype, files)
                if not text or len(text) < 500:
                    logger.debug("Skipping %s/%s: text too short (%d chars)",
                                 slug, jtype, len(text) if text else 0)
                    continue

                # Extract date from first judgment filename
                date = None
                for f in files:
                    date = extract_date_from_filename(f)
                    if date:
                        break

                yield {
                    "slug": slug,
                    "judgment_type": jtype,
                    "judgment_type_label": jtype_label,
                    "case_name": self._format_case_name(slug),
                    "date": date,
                    "text": text,
                    "files": files,
                    "url": JUDGMENT_BASE.format(slug=slug, jtype=jtype),
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """ICTY archive is static (closed 2017), so updates yield nothing."""
        logger.info("ICTY archive is static — no updates since closure in 2017")
        return
        yield  # make it a generator

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        slug = raw["slug"]
        jtype = raw["judgment_type"]
        doc_id = f"ICTY-{slug}-{jtype}"

        return {
            "_id": doc_id,
            "_source": "INTL/ICTY",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{raw['case_name']} - {raw['judgment_type_label']}",
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "case_name": raw["case_name"],
            "judgment_type": raw["judgment_type_label"],
            "slug": slug,
            "tribunal": "ICTY",
            "files": raw.get("files", []),
        }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ICTYScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing ICTY connectivity...")
        resp = requests.get(CASES_URL, timeout=30)
        slugs = sorted(set(re.findall(r'/en/case/([a-z_-]+)', resp.text)))
        print(f"  Cases page: {len(slugs)} case slugs found")

        # Test one HTML judgment
        test_slug = "tadic"
        test_url = JUDGMENT_BASE.format(slug=test_slug, jtype="tjug") + "contents.htm"
        resp2 = requests.get(test_url, timeout=20)
        print(f"  Tadic trial judgment contents: HTTP {resp2.status_code}")

        files = set(re.findall(r'([a-zA-Z0-9_.-]+\.htm)', resp2.text, re.IGNORECASE))
        files.discard("contents.htm")
        files.discard("foot.htm")
        print(f"  Judgment files: {len(files)}")
        print("OK")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
