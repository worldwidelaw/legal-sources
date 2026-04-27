#!/usr/bin/env python3
"""
MU/CompetitionCommission -- Mauritius Competition Commission Decisions

Fetches competition enforcement decisions (mergers, collusive agreements,
abuse of monopoly) from the Competition Commission of Mauritius (CCM).

Strategy:
  - Scrapes the main decisions index at /commission-decision/ (table with ~49 rows)
  - Scrapes the RPM sub-decisions umbrella page at
    /commission-decision/decisions-resale-price-maintenance-rpm/ (~71 sub-decisions)
  - Visits each detail page to find decision PDF links (marked with fa-file-pdf-o)
  - Filters out shared sidebar PDFs (Competition Act, Guidelines)
  - Follows internal page links when decision PDF is on a sub-page
  - Downloads PDFs and extracts full text via common.pdf_extract

Data:
  - Merger decisions (~17)
  - Collusive agreement investigations (~30)
  - Abuse of monopoly decisions (~29)
  - Resale Price Maintenance (RPM) sub-decisions (~71)

License: Public regulatory data (Mauritius Competition Act 2007, Section 30(e))

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MU.CompetitionCommission")

BASE_URL = "https://competitioncommission.mu"

DECISIONS_INDEX = "/commission-decision/"
RPM_INDEX = "/commission-decision/decisions-resale-price-maintenance-rpm/"

# Shared sidebar/footer PDFs to skip (not decision-specific)
SHARED_PDF_KEYWORDS = [
    "Competition-Act",
    "Guidelines-market",
    "Competition_Amd",
    "CC8-Guidelines",
]

# Month mapping for date parsing
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities."""
    import html as html_module
    text = re.sub(r'<br\s*/?\s*>', '\n', html_text)
    text = re.sub(r'<p[^>]*>', '\n\n', text)
    text = re.sub(r'</p>', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dates like '11-Feb-2026', '15 March 2020', etc."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Remove timestamps like 1770768000
    date_str = re.sub(r'^\d{10,}', '', date_str).strip()

    m = re.match(r'(\d{1,2})[\s\-]+(\w+)[\s\-]+(\d{4})', date_str)
    if m:
        day, mon_str, year = m.groups()
        mon = MONTH_MAP.get(mon_str.lower())
        if mon:
            try:
                return datetime(int(year), mon, int(day)).strftime("%Y-%m-%d")
            except ValueError:
                pass

    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if m:
        return date_str[:10]

    return None


def _is_shared_pdf(url: str) -> bool:
    """Check if a PDF URL is a shared sidebar/footer PDF."""
    return any(kw in url for kw in SHARED_PDF_KEYWORDS)


class CompetitionCommissionScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )

    def _extract_decisions_from_index(self, html: str) -> List[Dict[str, str]]:
        """Extract decision entries from the main index table."""
        decisions = []
        row_re = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL)

        for row_match in row_re.finditer(html):
            row = row_match.group(1)
            if '<th' in row:
                continue

            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) < 3:
                continue

            # Date from first cell — contains Unix timestamp + human date
            # e.g., "177076800011-Feb-2026" → timestamp 1770768000 + "11-Feb-2026"
            date_raw = _strip_html(cells[0]).strip()
            # Try to extract the human-readable date part
            date_match = re.search(r'(\d{1,2}[\s\-]+\w{3,}[\s\-]+\d{4})', date_raw)
            date_str = date_match.group(1) if date_match else date_raw

            # Link and title
            link_match = re.search(
                r'href=["\']([^"\']+commission-decision/[^"\']+)["\']',
                row, re.DOTALL
            )
            if not link_match:
                continue
            href = link_match.group(1)
            title = _strip_html(cells[2]).strip()

            # Category and investigation ref
            category = _strip_html(cells[-1]).strip() if len(cells) >= 4 else ""
            inv_ref = _strip_html(cells[1]).strip() if len(cells) >= 3 else ""

            if not href or not title:
                continue
            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)

            decisions.append({
                "url": href,
                "title": title,
                "date_str": date_str,
                "category": category,
                "inv_ref": inv_ref,
            })

        return decisions

    def _extract_rpm_decisions(self, html: str) -> List[Dict[str, str]]:
        """Extract RPM sub-decision entries from the umbrella page."""
        decisions = []
        seen = set()

        # RPM decisions are listed as links with inv042/RPM/resale-price in URL
        for m in re.finditer(r'href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, re.DOTALL):
            href = m.group(1)
            title = _strip_html(m.group(2)).strip()
            if not title or len(title) < 5:
                continue
            if title.lower() in ("read more", "more", "next", "previous", "back",
                                  "home", "about", "contact"):
                continue

            is_rpm = (
                "rpm" in href.lower()
                or "inv042" in href.lower()
                or "inv-042" in href.lower()
                or "resale-price" in href.lower()
            )
            if not is_rpm:
                continue

            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)
            if href in seen:
                continue
            seen.add(href)

            decisions.append({
                "url": href,
                "title": title,
                "date_str": "",
                "category": "Resale Price Maintenance",
                "inv_ref": "INV 042",
            })

        return decisions

    def _find_all_decision_pdfs(self, page_url: str, html: str) -> List[str]:
        """Find ALL decision-related PDF URLs from a detail page.

        Returns a list ordered by preference: Decision PDFs first, then
        Media Release PDFs, then any other non-shared PDFs.
        """
        decision_pdfs: List[str] = []
        other_pdfs: List[str] = []
        seen: set = set()

        # Find all fa-file-pdf-o icon links
        icon_links: List[Tuple[str, str]] = []
        for m in re.finditer(
            r'fa-file-pdf-o[^<]*</i>\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
            html, re.DOTALL
        ):
            href = m.group(1)
            text = _strip_html(m.group(2)).strip()
            if not _is_shared_pdf(href):
                icon_links.append((href, text))

        # Collect Decision links first, then others
        for href, text in icon_links:
            is_decision = bool(re.search(r'decision', text, re.IGNORECASE))
            if href.lower().endswith('.pdf'):
                url = href if href.startswith('http') else urljoin(BASE_URL, href)
                if url not in seen:
                    seen.add(url)
                    (decision_pdfs if is_decision else other_pdfs).append(url)
            else:
                # Internal page link — follow it to find PDFs
                internal_url = href if href.startswith('http') else urljoin(BASE_URL, href)
                pdf_from_sub = self._find_pdf_on_subpage(internal_url)
                if pdf_from_sub and pdf_from_sub not in seen:
                    seen.add(pdf_from_sub)
                    (decision_pdfs if is_decision else other_pdfs).append(pdf_from_sub)

        # Scan all wp-content/uploads PDFs as last-resort candidates
        for m in re.finditer(r'href="([^"]*wp-content/uploads/[^"]*\.pdf)"', html, re.IGNORECASE):
            href = m.group(1)
            if not _is_shared_pdf(href):
                url = href if href.startswith('http') else urljoin(BASE_URL, href)
                if url not in seen:
                    seen.add(url)
                    other_pdfs.append(url)

        return decision_pdfs + other_pdfs

    def _find_pdf_on_subpage(self, url: str) -> Optional[str]:
        """Follow an internal link to find a PDF on a sub-page."""
        logger.info(f"  Following internal link: {url}")
        resp = self.client.get(url)
        if not resp or resp.status_code != 200:
            return None

        # If the sub-page itself serves PDF content, use its URL directly
        content_type = resp.headers.get("content-type", "")
        if "application/pdf" in content_type:
            return url

        html = resp.text

        # Look for PDF download links on this sub-page
        for m in re.finditer(r'href="([^"]*\.pdf)"', html, re.IGNORECASE):
            href = m.group(1)
            if not _is_shared_pdf(href):
                return href if href.startswith('http') else urljoin(BASE_URL, href)

        return None

    def _get_all_decisions(self) -> List[Dict[str, str]]:
        """Fetch all decision entries from index and RPM pages."""
        all_decisions = []
        seen_urls = set()

        # 1. Main index
        logger.info("Fetching main decisions index...")
        resp = self.client.get(DECISIONS_INDEX)
        if resp and resp.status_code == 200:
            main_decisions = self._extract_decisions_from_index(resp.text)
            for d in main_decisions:
                if d["url"] not in seen_urls:
                    if "decisions-resale-price-maintenance" in d["url"]:
                        continue
                    seen_urls.add(d["url"])
                    all_decisions.append(d)
            logger.info(f"  Found {len(all_decisions)} main decisions")
        else:
            logger.error("Failed to fetch main decisions index")
        time.sleep(1)

        # 2. RPM sub-decisions
        logger.info("Fetching RPM sub-decisions...")
        resp = self.client.get(RPM_INDEX)
        if resp and resp.status_code == 200:
            rpm_decisions = self._extract_rpm_decisions(resp.text)
            rpm_count = 0
            for d in rpm_decisions:
                if d["url"] not in seen_urls:
                    seen_urls.add(d["url"])
                    all_decisions.append(d)
                    rpm_count += 1
            logger.info(f"  Found {rpm_count} RPM sub-decisions")
        else:
            logger.warning("Failed to fetch RPM sub-decisions page")
        time.sleep(1)

        logger.info(f"Total decisions to process: {len(all_decisions)}")
        return all_decisions

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all decision documents with full text from PDFs."""
        decisions = self._get_all_decisions()

        for i, decision in enumerate(decisions):
            url = decision["url"]
            title = decision["title"]
            logger.info(f"[{i+1}/{len(decisions)}] {title[:60]}...")

            # If the URL is a direct PDF link (e.g., some RPM sub-decisions)
            if url.lower().endswith(".pdf"):
                pdf_urls = [url]
            else:
                # Visit detail page — check if it serves PDF directly
                resp = self.client.get(url)
                if not resp or resp.status_code != 200:
                    logger.warning(f"  Failed to fetch detail page: {url}")
                    continue

                content_type = resp.headers.get("content-type", "")
                if "application/pdf" in content_type:
                    pdf_urls = [url]
                else:
                    pdf_urls = self._find_all_decision_pdfs(url, resp.text)
                time.sleep(1)

            if not pdf_urls:
                logger.warning(f"  No decision PDF found for: {title}")
                continue

            # Try each PDF until one yields text
            source_id = hashlib.md5(url.encode()).hexdigest()
            full_text = None
            used_pdf_url = None
            for pdf_url in pdf_urls:
                try:
                    text = extract_pdf_markdown(
                        source="MU/CompetitionCommission",
                        source_id=source_id,
                        pdf_url=pdf_url,
                        table="case_law",
                    )
                except Exception as e:
                    logger.warning(f"  PDF extraction failed for {pdf_url}: {e}")
                    continue
                if text and len(text.strip()) >= 50:
                    full_text = text
                    used_pdf_url = pdf_url
                    break
                elif len(pdf_urls) > 1:
                    logger.info(f"  PDF {pdf_url} empty, trying next...")

            if not full_text:
                logger.warning(f"  Insufficient text ({len(full_text) if full_text else 0} chars)")
                continue

            yield self.normalize({
                "url": url,
                "title": title,
                "date": _parse_date(decision["date_str"]),
                "text": full_text,
                "category": decision.get("category", ""),
                "inv_ref": decision.get("inv_ref", ""),
                "pdf_url": used_pdf_url or pdf_urls[0],
            })

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        url = raw["url"]
        if not url.startswith("http"):
            url = urljoin(BASE_URL, url)

        doc_id = hashlib.md5(raw["url"].encode()).hexdigest()

        return {
            "_id": f"MU/CompetitionCommission/{doc_id}",
            "_source": "MU/CompetitionCommission",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": url,
            "category": raw.get("category"),
            "investigation_ref": raw.get("inv_ref"),
            "pdf_url": raw.get("pdf_url"),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="MU/CompetitionCommission scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    scraper = CompetitionCommissionScraper()

    if args.command == "test":
        logger.info("Testing connectivity to competitioncommission.mu...")
        resp = scraper.client.get(DECISIONS_INDEX)
        if resp and resp.status_code == 200:
            logger.info(f"OK — got {len(resp.text)} bytes from decisions index")
        else:
            logger.error(f"FAIL — status {resp.status_code if resp else 'no response'}")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for doc in scraper.fetch_all():
            count += 1
            text_len = len(doc.get("text", ""))
            logger.info(
                f"  #{count} {doc['title'][:50]}... "
                f"({text_len} chars)"
            )

            # Save sample
            if count <= 20:
                fname = re.sub(r'[^\w\-]', '_', doc["_id"])[:80] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)

            if count >= max_records:
                break

        logger.info(f"Done — {count} records fetched")


if __name__ == "__main__":
    main()
