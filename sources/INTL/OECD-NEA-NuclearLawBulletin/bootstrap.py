#!/usr/bin/env python3
"""
OECD/NEA Nuclear Law Bulletin (NLB) fetcher.

The Nuclear Law Bulletin (ISSN 1609-7378) is the OECD Nuclear Energy Agency's
international nuclear-law journal, published free online twice a year in English
and French. It carries topical articles by legal experts, national
legislative/regulatory digests, case-law notes and international-instrument
updates — a `doctrine` layer complementing OECD legal instruments.

Enumeration: the NLB landing page (`pl_21586`) lists recent issues and links to
the Archive page (`pl_77708`, Nos. 1-94). Both pages carry per-issue jcms pages
(`.../nuclear-law-bulletin-no-N-volume-YYYY/H`); each issue page links to the
free full-text PDF under `/upload/docs/application/pdf/...`. Full text is
extracted from the PDF with the shared common.pdf_extract backend.

License: OECD Open Access / Government of member states — free-to-read journal
published by the OECD Nuclear Energy Agency.

Usage:
  python bootstrap.py test                # verify listing + one PDF download
  python bootstrap.py bootstrap --sample  # fetch 15 sample records
  python bootstrap.py bootstrap           # full run
"""

import hashlib
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

SOURCE_DIR = Path(__file__).parent
sys.path.insert(0, str(SOURCE_DIR.parent.parent.parent))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OECD-NEA-NuclearLawBulletin")

BASE_URL = "https://www.oecd-nea.org"
LANDING_PATH = "/jcms/pl_21586/nuclear-law-bulletin-nlb"
ARCHIVE_PATH = "/jcms/pl_77708/nuclear-law-bulletin-archive"


def _norm_path(url: str) -> str:
    url = url.split("?")[0].replace("&amp;", "&")
    if url.startswith("http"):
        url = re.sub(r"^https?://[^/]+", "", url)
    if not url.startswith("/"):
        url = "/" + url
    return url


class NuclearLawBulletinScraper(BaseScraper):
    """Scraper for INTL/OECD-NEA-NuclearLawBulletin."""

    def __init__(self):
        super().__init__(str(SOURCE_DIR))
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 LegalDataHunter/1.0 (Open Data Research)",
            },
        )

    def _load_issues(self) -> list[dict]:
        """Collect English NLB issue pages from the landing + archive pages."""
        issues: dict[int, dict] = {}
        for path in (LANDING_PATH, ARCHIVE_PATH):
            resp = self.client.get(path)
            resp.raise_for_status()
            html = resp.content.decode("utf-8", errors="replace")
            for m in re.finditer(
                r'href="([^"]*jcms/pl_\d+/nuclear-law-bulletin-no-(\d+)[^"]*)"', html
            ):
                num = int(m.group(2))
                if num in issues:
                    continue
                issue_path = _norm_path(m.group(1))
                # Year: from ...volume-YYYY/H or ...-YYYY
                ym = re.search(r"volume-(\d{4})", issue_path) or re.search(
                    r"-(\d{4})", issue_path
                )
                year = ym.group(1) if ym else None
                # Half-year → month (H1 -> Jun, H2 -> Dec)
                hm = re.search(r"volume-\d{4}/(\d)", issue_path)
                month = "06" if hm and hm.group(1) == "1" else "12"
                date = f"{year}-{month}-01" if year else None
                issues[num] = {
                    "number": num,
                    "issue_path": issue_path,
                    "year": year,
                    "date": date,
                }
        ordered = [issues[n] for n in sorted(issues, reverse=True)]
        logger.info("Collected %d English NLB issue pages", len(ordered))
        return ordered

    def _find_pdf(self, issue_path: str) -> Optional[str]:
        resp = self.client.get(issue_path)
        resp.raise_for_status()
        html = resp.content.decode("utf-8", errors="replace")
        for m in re.finditer(r'href="([^"]*upload/docs[^"]*\.pdf)"', html):
            href = m.group(1)
            if re.search(r"index", href, re.I):
                continue
            return _norm_path(href)
        return None

    def normalize(self, raw: dict) -> dict:
        num = raw["number"]
        return {
            "_id": f"INTL/OECD-NEA-NuclearLawBulletin/nlb-{num}",
            "_source": "INTL/OECD-NEA-NuclearLawBulletin",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Nuclear Law Bulletin No. {num}"
            + (f" (Volume {raw['year']})" if raw.get("year") else ""),
            "text": raw.get("_prefetched_text", ""),
            "date": raw.get("date"),
            "url": BASE_URL + raw["issue_path"],
            "doc_id": f"nlb-{num}",
            "issue_number": num,
            "year": raw.get("year"),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        issues = self._load_issues()
        limit = 15 if sample else None
        count = 0

        for issue in issues:
            if limit and count >= limit:
                break
            try:
                self.rate_limiter.wait()
                pdf_path = self._find_pdf(issue["issue_path"])
                if not pdf_path:
                    logger.warning("  No PDF on issue page %s", issue["issue_path"])
                    continue
                issue["pdf_url"] = BASE_URL + pdf_path
                self.rate_limiter.wait()
                resp = self.client.get(pdf_path)
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning("  Failed NLB No. %s: %s", issue["number"], e)
                continue

            if not pdf_bytes or len(pdf_bytes) < 1000:
                logger.warning("  Tiny/empty PDF for NLB No. %s", issue["number"])
                continue

            text = (
                extract_pdf_markdown(
                    source="INTL/OECD-NEA-NuclearLawBulletin",
                    source_id=f"nlb-{issue['number']}",
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                )
                or ""
            )
            if not text or len(text) < 500:
                logger.warning(
                    "  Skipping NLB No. %s — no/short text (%d chars)",
                    issue["number"],
                    len(text),
                )
                continue

            issue["_prefetched_text"] = text
            yield issue
            count += 1
            logger.info("  [%d] NLB No. %s (%d chars)", count, issue["number"], len(text))

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = NuclearLawBulletinScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        issues = scraper._load_issues()
        if not issues:
            print("FAILED - no issues found")
            sys.exit(1)
        print(f"Loaded {len(issues)} NLB issues.")
        pdf = scraper._find_pdf(issues[0]["issue_path"])
        print(f"  First issue No. {issues[0]['number']} PDF: {pdf}")
        resp = scraper.client.get(pdf)
        resp.raise_for_status()
        print(f"  Download OK: {len(resp.content)} bytes")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
