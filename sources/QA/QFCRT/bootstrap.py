#!/usr/bin/env python3
"""
QA/QFCRT -- Qatar International Court and Dispute Resolution Centre (QICDRC)

Fetches full-text judgments from qicdrc.gov.qa. Judgment URLs are discovered
via the sitemap (sitemap.xml?page=1). Each judgment page provides structured
metadata (title, case number, neutral citation, date, court type, judges,
keywords) and links to downloadable PDF decisions. Full text is extracted
from PDFs via common.pdf_extract.

Data access:
  - Sitemap: /sitemap.xml?page=1 → all /judgments/... URLs
  - Judgment detail: /judgments/<slug>
  - PDFs: /sites/default/files/<path>.pdf
  - Full text extracted from PDFs via common.pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
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
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.QA.QFCRT")

BASE_URL = "https://www.qicdrc.gov.qa"
DELAY = 2.0


def _clean_text(html_fragment: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r"<[^>]+>", " ", html_fragment)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse QICDRC date formats like '29 July 2025' to ISO 8601."""
    if not date_str:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class QFCRTScraper(BaseScraper):
    """Scraper for QICDRC judgments (Civil & Commercial Court, Regulatory Tribunal)."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def _get_judgment_urls(self) -> List[str]:
        """Extract all English judgment URLs from the sitemap."""
        urls = []
        for page in range(1, 10):  # check up to 10 pages
            sitemap_url = f"{BASE_URL}/sitemap.xml?page={page}"
            time.sleep(1)
            resp = self.http.get(sitemap_url)
            if resp.status_code != 200:
                break
            # Extract URLs matching /judgments/ (English only, not /ar/)
            page_urls = re.findall(
                r"<loc>(https://www\.qicdrc\.gov\.qa/judgments/[^<]+)</loc>",
                resp.text,
            )
            # Filter out Arabic URLs
            page_urls = [u for u in page_urls if "/ar/" not in u]
            urls.extend(page_urls)
            if not page_urls:
                break
        logger.info("Found %d judgment URLs from sitemap", len(urls))
        return list(dict.fromkeys(urls))  # deduplicate preserving order

    def _parse_judgment_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a judgment detail page."""
        time.sleep(DELAY)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch %s: %s", url, resp.status_code)
            return None

        html = resp.text
        meta: Dict[str, Any] = {"url": url}

        # Title
        title_match = re.search(r'<h2\s+class="page-title">(.*?)</h2>', html, re.DOTALL)
        if title_match:
            meta["title"] = _clean_text(title_match.group(1))

        # Court type
        court_match = re.search(
            r'class="judgement-button-link">(.*?)</span>', html, re.DOTALL
        )
        if court_match:
            meta["court_type"] = _clean_text(court_match.group(1))

        # Case number
        case_match = re.search(
            r'class="judgement-body-case-info">(.*?)</div>', html, re.DOTALL
        )
        if case_match:
            meta["case_number"] = _clean_text(case_match.group(1))

        # Neutral citation
        citation_match = re.search(
            r'judgement-body-citation-info">(.*?)</div>', html, re.DOTALL
        )
        if citation_match:
            meta["neutral_citation"] = _clean_text(citation_match.group(1))

        # Date
        date_match = re.search(
            r'judgement-body-date-info">(.*?)</div>', html, re.DOTALL
        )
        if date_match:
            meta["date_raw"] = _clean_text(date_match.group(1))

        # Keywords
        keywords = re.findall(
            r'class="judgement-keywords-button[^"]*">(.*?)</a>', html, re.DOTALL
        )
        if keywords:
            meta["keywords"] = [_clean_text(k) for k in keywords]

        # Judges
        judges = re.findall(r'class="judge--name">(.*?)</div>', html, re.DOTALL)
        if judges:
            meta["judges"] = [_clean_text(j) for j in judges]

        # Summary
        summary_match = re.search(
            r'judgement-body-summary-label">.*?</div>\s*'
            r'<div class="judgement-item-body-cell judgement-item-body-info[^"]*">(.*?)</div>',
            html,
            re.DOTALL,
        )
        if summary_match:
            meta["summary"] = _clean_text(summary_match.group(1))

        # PDF download URL (prefer English)
        en_pdf_match = re.search(
            r'judgement-download-en-wrapper.*?<a\s+download\s+href="([^"]+)"',
            html,
            re.DOTALL,
        )
        if en_pdf_match:
            pdf_path = en_pdf_match.group(1)
            meta["pdf_url"] = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
        else:
            # Fallback: any PDF download link
            any_pdf = re.search(
                r'<a\s+download\s+href="([^"]+\.pdf[^"]*)"', html
            )
            if any_pdf:
                pdf_path = any_pdf.group(1)
                meta["pdf_url"] = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"

        return meta

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract text."""
        time.sleep(DELAY)
        try:
            resp = self.http.get(pdf_url)
            if resp.status_code != 200:
                logger.warning("PDF download failed %s: %s", pdf_url, resp.status_code)
                return ""
            return extract_pdf_markdown(resp.content) or ""
        except Exception as e:
            logger.warning("PDF extraction error for %s: %s", pdf_url, e)
            return ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all judgments from QICDRC."""
        urls = self._get_judgment_urls()
        for i, url in enumerate(urls):
            logger.info("Processing judgment %d/%d: %s", i + 1, len(urls), url)
            meta = self._parse_judgment_page(url)
            if not meta:
                continue

            # Extract full text from PDF
            pdf_url = meta.get("pdf_url")
            if pdf_url:
                text = self._extract_pdf_text(pdf_url)
                meta["text"] = text
            else:
                logger.warning("No PDF found for %s", url)
                meta["text"] = meta.get("summary", "")

            yield meta

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Yield judgments updated since the given date."""
        for raw in self.fetch_all():
            date_str = _parse_date(raw.get("date_raw", ""))
            if date_str and date_str >= since.strftime("%Y-%m-%d"):
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a raw judgment into standard schema."""
        title = raw.get("title", "")
        text = raw.get("text", "")

        if not text or len(text) < 100:
            logger.debug("Skipping %s — insufficient text (%d chars)", raw.get("url"), len(text))
            return None

        case_number = raw.get("case_number", "")
        date = _parse_date(raw.get("date_raw", ""))

        # Build document ID from case number or URL slug
        if case_number:
            doc_id = re.sub(r"[/\s]+", "-", case_number)
        else:
            doc_id = raw["url"].rstrip("/").split("/")[-1]

        judges = raw.get("judges", [])
        keywords = raw.get("keywords", [])

        return {
            "_id": f"QA-QFCRT-{doc_id}",
            "_source": "QA/QFCRT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title if title else f"QICDRC {case_number}",
            "text": text,
            "date": date,
            "case_number": case_number,
            "neutral_citation": raw.get("neutral_citation", ""),
            "court_type": raw.get("court_type", ""),
            "judges": ", ".join(judges) if judges else "",
            "keywords": ", ".join(keywords) if keywords else "",
            "summary": raw.get("summary", ""),
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="QA/QFCRT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=12, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = QFCRTScraper()

    if args.command == "test":
        urls = scraper._get_judgment_urls()
        print(f"OK — found {len(urls)} judgment URLs in sitemap")
        if urls:
            print(f"First: {urls[0]}")
            print(f"Last:  {urls[-1]}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
