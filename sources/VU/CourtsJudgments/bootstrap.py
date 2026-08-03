#!/usr/bin/env python3
"""
VU/CourtsJudgments -- Vanuatu Judiciary Judgments Database Fetcher

Fetches judgments from courts.gov.vu with full text.

Strategy:
  - Paginate the judgments listing table (25 per page, ~108 pages)
  - Extract metadata from table rows (court, case type, case number, judges, dates)
  - Fetch each judgment detail page for full text from div.cck-line-body
  - 1-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Fetch all judgments
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
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

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VU.CourtsJudgments")

BASE_URL = "https://courts.gov.vu"
LISTING_URL = f"{BASE_URL}/court-activity/judgments"
ITEMS_PER_PAGE = 25
MAX_PAGES = 120  # safety cap (~3000 judgments)


class VUCourtsJudgmentsScraper(BaseScraper):
    """Scraper for VU/CourtsJudgments -- Vanuatu judiciary judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with 1-second delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(1)
                try:
                    resp = self.session.get(url, timeout=timeout)
                except requests.exceptions.SSLError as ssl_exc:
                    # courts.gov.vu omits its intermediate CA cert → AIA-fetch it
                    # and retry with an augmented bundle rather than dropping
                    # TLS verification (issue #1161).
                    from common.ssl_aia import is_missing_issuer_error, ca_bundle_for
                    bundle = ca_bundle_for(url) if is_missing_issuer_error(ssl_exc) else None
                    if not bundle:
                        raise
                    logger.warning("Retrying with AIA-augmented CA bundle: %s", url)
                    resp = self.session.get(url, timeout=timeout, verify=bundle)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a judgments listing page for document links and metadata."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []

        table = soup.find("table", class_="table-striped")
        if not table:
            return documents

        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header
            tds = row.find_all("td")
            if len(tds) < 7:
                continue

            link = tds[3].find("a")
            if not link:
                continue

            href = link.get("href", "")
            if not href or "/judgments/" not in href:
                continue

            full_url = href if href.startswith("http") else BASE_URL + href

            # Extract numeric ID from URL
            id_match = re.match(r".*/judgments/(\d+)-", href)
            judgment_id = id_match.group(1) if id_match else href.split("/")[-1]

            documents.append({
                "judgment_id": judgment_id,
                "url": full_url,
                "href": href,
                "case_name": link.get_text(strip=True),
                "jurisdiction": tds[0].get_text(strip=True),
                "case_type": tds[1].get_text(strip=True),
                "case_number": tds[2].get_text(strip=True),
                "judge": tds[4].get_text(strip=True),
                "hearing_date": tds[5].get_text(strip=True),
                "decision_date": tds[6].get_text(strip=True),
            })

        return documents

    def _parse_date(self, date_str: str) -> str:
        """Parse date string like '05 Mar 2026' to ISO 8601."""
        if not date_str:
            return ""
        try:
            dt = datetime.strptime(date_str.strip(), "%d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return ""

    def _extract_full_text(self, html: str) -> Dict[str, str]:
        """Extract full judgment text and metadata from a detail page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "title": "", "judges": "", "parties": ""}

        # The judiciary site was redesigned (2026): the article is now
        # <article class="judgement-single"> with the title in an <h1> and the
        # full text in <div class="judgement-single__content-body">. Fall back to
        # the old cck-* selectors for any un-migrated pages.
        article = soup.find("article", class_="judgement-single")

        # Title from the article header <h1> (new) or old article-header block.
        if article:
            h_tag = article.find(["h1", "h2"])
            if h_tag:
                result["title"] = h_tag.get_text(strip=True)
        if not result["title"]:
            header = soup.find("div", class_="article-header")
            if header:
                h_tag = header.find(["h1", "h2"])
                if h_tag:
                    result["title"] = h_tag.get_text(strip=True)

        # Full text: new content-body container, else legacy cck-line-body.
        body = soup.find("div", class_="judgement-single__content-body")
        if body is None:
            body = soup.find("div", class_="cck-line-body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            # Clean up excessive whitespace
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            result["text"] = text.strip()

        # Extra metadata from cck-line-top (legacy layout only)
        top = soup.find("div", class_="cck-line-top")
        if top:
            top_text = top.get_text(separator="\n", strip=True)
            # Extract judge(s)
            judge_match = re.search(r"Judge\(s\):\s*(.+?)(?:\n|Prosecutor:|$)", top_text)
            if judge_match:
                result["judges"] = judge_match.group(1).strip()
            # Extract parties info
            parts = []
            for label in ["Prosecutor:", "Defendant(s):", "Appellant:", "Respondent:",
                          "Claimant:", "Applicant:", "Plaintiff:"]:
                m = re.search(rf"{re.escape(label)}\s*(.+?)(?:\n|$)", top_text)
                if m:
                    parts.append(f"{label} {m.group(1).strip()}")
            result["parties"] = "; ".join(parts)

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": f"VU-{raw['judgment_id']}",
            "_source": "VU/CourtsJudgments",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or raw.get("case_name", ""),
            "text": raw.get("text", ""),
            "date": self._parse_date(raw.get("decision_date", "")),
            "url": raw.get("url", ""),
            "jurisdiction": raw.get("jurisdiction", ""),
            "case_type": raw.get("case_type", ""),
            "case_number": raw.get("case_number", ""),
            "judge": raw.get("judge", ""),
            "hearing_date": self._parse_date(raw.get("hearing_date", "")),
            "parties": raw.get("parties", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all judgments from paginated listing."""
        count = 0
        seen_ids = set()

        for page_num in range(MAX_PAGES):
            offset = page_num * ITEMS_PER_PAGE
            url = f"{LISTING_URL}?start={offset}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch page at offset {offset}")
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents at offset {offset}, stopping")
                break

            logger.info(f"Page {page_num+1} (offset={offset}): {len(docs)} judgments")

            for doc in docs:
                jid = doc["judgment_id"]
                if jid in seen_ids:
                    continue
                seen_ids.add(jid)

                # Fetch detail page
                doc_resp = self._request(doc["url"])
                if doc_resp is None:
                    logger.warning(f"Failed to fetch: {doc['case_name'][:60]}")
                    continue

                extracted = self._extract_full_text(doc_resp.text)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    logger.warning(f"Insufficient text ({len(extracted['text'])} chars): {doc['case_name'][:60]}")
                    continue

                raw = {
                    **doc,
                    "title": extracted["title"] or doc["case_name"],
                    "text": extracted["text"],
                    "parties": extracted["parties"],
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} judgments fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent judgments (first 4 pages = 100 most recent)."""
        count = 0
        for page_num in range(4):
            offset = page_num * ITEMS_PER_PAGE
            url = f"{LISTING_URL}?start={offset}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_listing_page(resp.text)
            for doc in docs:
                doc_resp = self._request(doc["url"])
                if doc_resp is None:
                    continue

                extracted = self._extract_full_text(doc_resp.text)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    continue

                raw = {
                    **doc,
                    "title": extracted["title"] or doc["case_name"],
                    "text": extracted["text"],
                    "parties": extracted["parties"],
                }
                count += 1
                yield raw

        logger.info(f"Updates: {count} judgments fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{LISTING_URL}?start=0")
        if resp is None:
            logger.error("Cannot reach courts.gov.vu judgments listing")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No judgments found on listing page")
            return False

        logger.info(f"Listing OK: {len(docs)} judgments on page 1")

        # Test one document detail page
        doc_resp = self._request(docs[0]["url"])
        if doc_resp:
            extracted = self._extract_full_text(doc_resp.text)
            logger.info(f"Detail OK: {docs[0]['case_name'][:60]} ({len(extracted['text'])} chars)")
            return len(extracted["text"]) > 100

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="VU/CourtsJudgments data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VUCourtsJudgmentsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        # bootstrap-fast is the VPS fleet entrypoint; it must run the FULL
        # corpus (streamed to data/records.jsonl by BaseScraper), never the
        # 15-record sample path.
        sample_mode = args.sample and args.command == "bootstrap"
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
