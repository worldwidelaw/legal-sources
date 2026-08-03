#!/usr/bin/env python3
"""
EE/Konkurentsiamet — Estonian Competition Authority

Fetches merger decisions and competition supervision decisions from
Konkurentsiamet. Both sections publish full-text PDFs.

Strategy:
  1. Merger decisions: parse the table at /en/koondumiste-teated-ja-otsused
     Extract decision PDFs from the "Otsus" column (column 4).
  2. Competition supervision: parse the table at
     /konkurentsijarelevalve-ja-koondumised/konkurentsijarelevalve/juhtumid
     Extract decision PDFs from the title column (column 2).
  3. Download each PDF and extract text with pdfplumber.

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test
"""

import sys
import re
import io
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.Konkurentsiamet")

BASE_URL = "https://www.konkurentsiamet.ee"

MERGER_PAGE = "/en/koondumiste-teated-ja-otsused"
COMPETITION_PAGE = "/konkurentsijarelevalve-ja-koondumised/konkurentsijarelevalve/juhtumid"


def _parse_estonian_date(date_str: str) -> Optional[str]:
    """Parse DD.MM.YYYY to ISO date."""
    if not date_str:
        return None
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    import pdfplumber

    text_parts = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_parts.append(t)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n".join(text_parts)


class KonkurentsiametScraper(BaseScraper):
    """Scraper for EE/Konkurentsiamet — Estonian Competition Authority."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _parse_merger_decisions(self) -> list:
        """Parse merger decisions table, return list of raw records."""
        from bs4 import BeautifulSoup

        logger.info("Fetching merger decisions page...")
        self.rate_limiter.wait()
        resp = self.http.get(MERGER_PAGE)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning("No table found on merger page")
            return []

        rows = table.find_all("tr")
        records = []

        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            # Column 0: notification date
            notif_date = cells[0].get_text(strip=True)

            # Column 1: parties (text of first link or cell text)
            parties_cell = cells[1]
            parties_link = parties_cell.find("a")
            if parties_link:
                parties_text = parties_link.get_text(strip=True)
                # Remove file size info like "| 40.12 KB | pdf"
                parties_text = re.sub(r"\|\s*[\d.]+\s*KB\s*\|\s*pdf\s*$", "", parties_text).strip()
            else:
                parties_text = parties_cell.get_text(strip=True)

            # Column 4: decision (Otsus) — may have PDF link
            decision_cell = cells[4]
            decision_links = decision_cell.find_all("a")
            if not decision_links:
                continue  # No decision PDF, skip

            for link in decision_links:
                href = link.get("href", "")
                if not href:
                    continue
                link_text = link.get_text(strip=True)
                # Remove file size info
                link_text = re.sub(r"\|\s*[\d.]+\s*KB\s*\|\s*pdf\s*$", "", link_text).strip()

                # Extract decision number from link text (e.g., "nr 5-5/2024-015")
                dec_num_match = re.search(r"nr\s*([\d\-/]+)", link_text)
                decision_number = dec_num_match.group(1) if dec_num_match else None

                # Extract date from link text
                date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", link_text)
                decision_date = _parse_estonian_date(date_match.group(1)) if date_match else _parse_estonian_date(notif_date)

                pdf_url = urljoin(BASE_URL, href)

                records.append({
                    "_pdf_url": pdf_url,
                    "_type_tag": "merger",
                    "title": f"Merger decision: {parties_text}",
                    "decision_number": decision_number or href.split("/")[-1].replace(".pdf", ""),
                    "date": decision_date,
                    "parties": parties_text,
                    "url": pdf_url,
                })

        logger.info(f"Found {len(records)} merger decisions with PDFs")
        return records

    def _parse_competition_decisions(self) -> list:
        """Parse competition supervision cases table."""
        from bs4 import BeautifulSoup

        logger.info("Fetching competition supervision cases page...")
        self.rate_limiter.wait()
        resp = self.http.get(COMPETITION_PAGE)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning("No table found on competition page")
            return []

        rows = table.find_all("tr")
        records = []

        # Size/format suffix that the site appends inside the linked cell,
        # e.g. "5-5/2019-013(403.3 KB, PDF)" or "... | 403.3 KB | pdf".
        size_suffix = re.compile(r"\s*[\(|]\s*[\d.]+\s*KB[,\s|]*PDF\s*\)?\s*$", re.I)

        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            # Columns: 0=date (Kuupäev), 1=number (Nr), 2=title (Pealkiri),
            # 3=sector (Tegevusala). The PDF link lives in EITHER the number
            # cell or the title cell depending on the row, so scan for it
            # instead of assuming a fixed column (this dropped ~160 of ~200
            # enforcement decisions — issue #1079).
            link = None
            for cell in cells[1:3]:
                link = cell.find("a", href=lambda h: h and h.lower().endswith(".pdf"))
                if link:
                    break
            if link is None:
                link = row.find("a", href=lambda h: h and h.lower().endswith(".pdf"))
            if link is None:
                continue

            href = link.get("href", "")
            if not href:
                continue

            # Column 0: date
            date_text = cells[0].get_text(strip=True)
            # Column 1: case number (may carry the "(KB, PDF)" suffix)
            case_number = size_suffix.sub("", cells[1].get_text(strip=True)).strip()
            # Column 3: sector
            sector = cells[3].get_text(strip=True)

            # Column 2: title; fall back to the link text if the title cell is empty
            title_text = size_suffix.sub("", cells[2].get_text(strip=True)).strip()
            if not title_text:
                title_text = size_suffix.sub("", link.get_text(strip=True)).strip()

            pdf_url = urljoin(BASE_URL, href)

            records.append({
                "_pdf_url": pdf_url,
                "_type_tag": "competition",
                "title": title_text,
                "decision_number": case_number or href.split("/")[-1].replace(".pdf", ""),
                "date": _parse_estonian_date(date_text),
                "sector": sector,
                "url": pdf_url,
            })

        logger.info(f"Found {len(records)} competition supervision decisions")
        return records

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract its text content."""
        self.rate_limiter.wait()
        try:
            resp = self.http.get(pdf_url, timeout=90)
            resp.raise_for_status()
            return _extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {pdf_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw records from both merger and competition pages."""
        merger_records = self._parse_merger_decisions()
        competition_records = self._parse_competition_decisions()

        all_records = merger_records + competition_records
        logger.info(f"Total records to process: {len(all_records)}")

        for record in all_records:
            yield record

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield records updated since a given date."""
        for record in self.fetch_all():
            if record.get("date") and record["date"] >= since:
                yield record

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download PDF and produce normalized record."""
        pdf_url = raw.get("_pdf_url", "")
        type_tag = raw.pop("_type_tag", "merger")

        text = self._download_pdf_text(pdf_url)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text ({len(text)} chars) for {pdf_url}")
            return None

        return {
            "_id": raw.get("decision_number", ""),
            "_source": "EE/Konkurentsiamet",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "decision_number": raw.get("decision_number", ""),
            "parties": raw.get("parties", ""),
            "sector": raw.get("sector", ""),
            "decision_type": type_tag,
        }


    def test_connection(self):
        """Quick connectivity test."""
        print("Testing EE/Konkurentsiamet endpoints...")

        print("\n1. Fetching merger decisions page...")
        merger_records = self._parse_merger_decisions()
        print(f"   Found {len(merger_records)} merger decisions")

        print("\n2. Fetching competition supervision page...")
        comp_records = self._parse_competition_decisions()
        print(f"   Found {len(comp_records)} competition decisions")

        if merger_records:
            rec = merger_records[0]
            print(f"\n3. Testing PDF download: {rec['_pdf_url'][:80]}...")
            text = self._download_pdf_text(rec["_pdf_url"])
            print(f"   Text: {len(text)} chars")
            print(f"   Preview: {text[:300]}...")

        print("\nTest complete!")


def main():
    scraper = KonkurentsiametScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
