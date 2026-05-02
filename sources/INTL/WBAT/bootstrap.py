#!/usr/bin/env python3
"""
INTL/WBAT -- World Bank Administrative Tribunal Judgments

Fetches judgments and orders from the World Bank Administrative Tribunal.

Strategy:
  - Scrape the all-judgments HTML page at tribunal.worldbank.org/judgments-orders-all
  - Extract metadata (title, number, date, PDF URL) from the HTML table
  - Download judgment PDFs and extract full text via common/pdf_extract

Data Coverage:
  - ~798 decisions from Decision No. 1 (1981) to present
  - Employment disputes between World Bank Group and staff
  - Judgments, orders, preliminary objections

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WBAT")

BASE_URL = "https://tribunal.worldbank.org"
ALL_JUDGMENTS_URL = f"{BASE_URL}/judgments-orders-all"
MAX_PDF_BYTES = 50 * 1024 * 1024


class WBATScraper(BaseScraper):
    """Scraper for World Bank Administrative Tribunal judgments."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en",
        })

    def _parse_judgments_page(self) -> list[dict]:
        """Parse the all-judgments HTML page to extract metadata."""
        resp = self.session.get(ALL_JUDGMENTS_URL, timeout=60)
        resp.raise_for_status()
        html = resp.text

        titles = re.findall(
            r'class="views-field views-field-title">(.*?)</td>', html, re.DOTALL
        )
        numbers = re.findall(
            r'class="views-field views-field-field-number">(.*?)</td>', html, re.DOTALL
        )
        dates = re.findall(
            r'class="views-field views-field-field-date">(.*?)</td>', html, re.DOTALL
        )
        judgments = re.findall(
            r'class="views-field views-field-field-judgment-order">(.*?)</td>',
            html,
            re.DOTALL,
        )
        summaries_raw = re.findall(
            r'class="views-field views-field-field-summary">(.*?)</td>',
            html,
            re.DOTALL,
        )

        entries = []
        for i in range(len(titles)):
            title = re.sub(r"<[^>]+>", "", titles[i]).strip()
            number = re.sub(r"<[^>]+>", "", numbers[i]).strip() if i < len(numbers) else ""
            date_str = re.sub(r"<[^>]+>", "", dates[i]).strip() if i < len(dates) else ""
            pdf_links = (
                re.findall(r'href="([^"]+)"', judgments[i]) if i < len(judgments) else []
            )
            summary_links = (
                re.findall(r'href="([^"]+)"', summaries_raw[i])
                if i < len(summaries_raw)
                else []
            )

            pdf_url = pdf_links[0] if pdf_links else ""
            if pdf_url and not pdf_url.startswith("http"):
                pdf_url = f"{BASE_URL}{pdf_url}"

            summary_url = summary_links[0] if summary_links else ""
            if summary_url and not summary_url.startswith("http"):
                summary_url = f"{BASE_URL}{summary_url}"

            entries.append({
                "title": title,
                "number": number,
                "date_str": date_str,
                "pdf_url": pdf_url,
                "summary_url": summary_url,
            })

        logger.info(f"Parsed {len(entries)} judgment entries from HTML page")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF from the WBAT site."""
        if not url:
            return None
        try:
            time.sleep(1)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes), likely error")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        """Extract text from PDF bytes."""
        text = extract_pdf_markdown(
            source="INTL/WBAT",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text

        import io
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p)
                if text and len(text.strip()) >= 100:
                    return text
        except Exception:
            pass
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            if text and len(text.strip()) >= 100:
                return text
        except Exception:
            pass
        return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse MM/DD/YYYY date to ISO 8601."""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str.strip(), "%m/%d/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments with full text from PDFs."""
        entries = self._parse_judgments_page()
        logger.info(f"Total entries to process: {len(entries)}")

        for i, entry in enumerate(entries):
            try:
                title = entry["title"]
                pdf_url = entry["pdf_url"]
                source_id = entry["number"] or str(i)

                logger.info(f"[{i+1}/{len(entries)}] Processing #{source_id}: {title[:60]} ...")

                if not pdf_url:
                    logger.warning(f"  No PDF URL, skipping")
                    continue

                pdf_bytes = self._download_pdf(pdf_url)
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, source_id)
                if not text:
                    logger.warning(f"  Insufficient text for #{source_id}, skipping")
                    continue

                entry["_extracted_text"] = text
                yield entry

            except Exception as e:
                logger.error(f"  Error processing entry {i}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments since a given date."""
        entries = self._parse_judgments_page()
        for entry in entries:
            date = self._parse_date(entry.get("date_str", ""))
            if date and date >= since.strftime("%Y-%m-%d"):
                pdf_url = entry["pdf_url"]
                if not pdf_url:
                    continue
                pdf_bytes = self._download_pdf(pdf_url)
                if not pdf_bytes:
                    continue
                source_id = entry["number"] or entry["title"]
                text = self._extract_text(pdf_bytes, source_id)
                if not text:
                    continue
                entry["_extracted_text"] = text
                yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        title = raw.get("title", "")
        number = raw.get("number", "")
        date = self._parse_date(raw.get("date_str", ""))

        uid = number if number else title.lower().replace(" ", "-")[:60]
        uid_slug = str(uid).lower().replace("/", "-").replace(" ", "-")

        return {
            "_id": f"wbat-{uid_slug}",
            "_source": "INTL/WBAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_extracted_text", ""),
            "date": date,
            "url": raw.get("pdf_url", ""),
            "decision_number": number,
            "summary_url": raw.get("summary_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = WBATScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._parse_judgments_page()
        print(f"Total entries: {len(entries)}")
        if entries:
            print(f"First: #{entries[0]['number']} {entries[0]['title'][:60]}")
            print(f"Last:  #{entries[-1]['number']} {entries[-1]['title'][:60]}")
        sys.exit(0)

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
