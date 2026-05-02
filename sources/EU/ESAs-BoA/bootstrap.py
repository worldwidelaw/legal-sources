#!/usr/bin/env python3
"""
EU/ESAs-BoA -- Joint Board of Appeal of the European Supervisory Authorities

Fetches decisions from the Joint Board of Appeal (EBA, EIOPA, ESMA).

Strategy:
  - Scrape EIOPA decisions page for all BoA decision entries
  - Each entry has a PDF download link with UUID
  - Download PDF and extract full text via pypdf / pdfplumber
  - Parse decision date and title from the listing entry metadata

Data Coverage:
  - ~26 decisions from 2013 to present
  - Appeals against EBA, EIOPA, ESMA supervisory decisions
  - English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import io
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.ESAs-BoA")

LISTING_URL = "https://www.eiopa.europa.eu/decisions-board-appeal_en"
BASE_URL = "https://www.eiopa.europa.eu"
MAX_PDF_BYTES = 50 * 1024 * 1024

MONTHS = {
    "JANUARY": "01", "FEBRUARY": "02", "MARCH": "03", "APRIL": "04",
    "MAY": "05", "JUNE": "06", "JULY": "07", "AUGUST": "08",
    "SEPTEMBER": "09", "OCTOBER": "10", "NOVEMBER": "11", "DECEMBER": "12",
}


class ESAsBoAScraper(BaseScraper):
    """Scraper for ESAs Joint Board of Appeal decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "en",
        })

    def _parse_pub_date(self, text: str) -> Optional[str]:
        """Parse date like '21 APRIL 2026' to ISO '2026-04-21'."""
        m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text.strip())
        if m:
            day = m.group(1).zfill(2)
            month = MONTHS.get(m.group(2).upper())
            year = m.group(3)
            if month:
                return f"{year}-{month}-{day}"
        return None

    def _parse_decision_date(self, title: str) -> Optional[str]:
        """Extract decision date from title like '2024-07-30 - Decision...' or '21.07.2022 Decision...'."""
        m = re.match(r"(\d{4}-\d{2}-\d{2})", title)
        if m:
            return m.group(1)
        m = re.match(r"[\u200b]*(\d{2})\.(\d{2})\.(\d{4})", title)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    def _clean_title(self, title: str) -> str:
        """Remove date prefix and clean up title text."""
        title = title.replace("\u200b", "").strip()
        title = re.sub(r"^\d{4}-\d{2}-\d{2}\s*[-\u2013\u2014]\s*", "", title)
        title = re.sub(r"^\d{2}\.\d{2}\.\d{4}\s*", "", title)
        title = re.sub(r"\.pdf$", "", title, flags=re.IGNORECASE)
        return title.strip()

    def _extract_reference(self, title: str, filename: str) -> Optional[str]:
        """Extract BoA reference number like 'BoA-D-2025-01' from title or filename."""
        for text in [filename, title]:
            m = re.search(r"BoA[-_]?[DO][-_]?\d{4}[-_]\d{2}", text, re.IGNORECASE)
            if m:
                return m.group(0).replace("_", "-")
        return None

    def _determine_respondent(self, title: str) -> str:
        """Determine which ESA is the respondent from the title."""
        t = title.upper()
        if "EIOPA" in t:
            return "EIOPA"
        elif "ESMA" in t:
            return "ESMA"
        elif "EBA" in t or "BANKING" in t:
            return "EBA"
        return "ESA"

    def _get_decision_entries(self) -> list[dict]:
        """Scrape the EIOPA decisions page for all BoA decision entries."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        entries = []
        seen_uuids = set()

        for file_div in soup.find_all("div", class_="ecl-file"):
            title_el = file_div.find(class_="ecl-file__title")
            if not title_el:
                continue
            raw_title = title_el.get_text(strip=True)

            info_el = file_div.find(class_="ecl-file__info")
            info_text = info_el.get_text(strip=True) if info_el else ""
            pub_date_match = re.match(r"(\d{1,2}\s+\w+\s+\d{4})", info_text)
            pub_date = self._parse_pub_date(pub_date_match.group(1)) if pub_date_match else None

            dl_link = file_div.find("a", href=lambda h: h and "document/download" in h)
            if not dl_link:
                continue
            href = dl_link["href"]
            if not href.startswith("http"):
                href = BASE_URL + href

            uuid_match = re.search(r"/document/download/([a-f0-9-]+)", href)
            if not uuid_match:
                continue
            uuid = uuid_match.group(1)
            if uuid in seen_uuids:
                continue
            seen_uuids.add(uuid)

            filename = ""
            if "filename=" in href:
                filename = urllib.parse.unquote(href.split("filename=")[1])

            decision_date = self._parse_decision_date(raw_title)
            clean_title = self._clean_title(raw_title)
            reference = self._extract_reference(raw_title, filename)
            respondent = self._determine_respondent(raw_title)

            entries.append({
                "uuid": uuid,
                "raw_title": raw_title,
                "title": clean_title,
                "decision_date": decision_date,
                "pub_date": pub_date,
                "reference": reference,
                "respondent_esa": respondent,
                "download_url": href.split("?")[0],
                "filename": filename,
            })

        logger.info(f"Found {len(entries)} decisions on listing page")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF with rate limiting."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes)")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        """Extract text from PDF bytes using multiple fallback methods."""
        text = extract_pdf_markdown(
            source="EU/ESAs-BoA",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text

        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p.strip())
            if text and len(text.strip()) >= 100:
                return text
        except Exception:
            pass

        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p.strip())
                if text and len(text.strip()) >= 100:
                    return text
        except Exception:
            pass

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BoA decisions with full text from PDFs."""
        entries = self._get_decision_entries()
        logger.info(f"Total entries to process: {len(entries)}")

        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] Processing {entry.get('reference', 'N/A')} - "
                    f"{entry['title'][:60]}..."
                )
                pdf_bytes = self._download_pdf(entry["download_url"])
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, entry["uuid"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry.get('reference', entry['uuid'])}, skipping")
                    continue

                entry["_extracted_text"] = text
                yield entry

            except Exception as e:
                logger.error(f"  Error processing {entry.get('reference', entry['uuid'])}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        entries = self._get_decision_entries()
        recent = [e for e in entries if (e.get("pub_date") or "") >= since_str]
        logger.info(f"Found {len(recent)} entries since {since_str}")

        for entry in recent:
            pdf_bytes = self._download_pdf(entry["download_url"])
            if not pdf_bytes:
                continue
            text = self._extract_text(pdf_bytes, entry["uuid"])
            if not text:
                continue
            entry["_extracted_text"] = text
            yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        uuid = raw.get("uuid", "unknown")
        ref = raw.get("reference") or uuid
        uid_slug = re.sub(r"[^a-z0-9]+", "-", ref.lower()).strip("-")

        date = raw.get("decision_date") or raw.get("pub_date")

        return {
            "_id": f"esas-boa-{uid_slug}",
            "_source": "EU/ESAs-BoA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": date,
            "url": raw.get("download_url", ""),
            "reference": raw.get("reference"),
            "respondent_esa": raw.get("respondent_esa"),
            "pub_date": raw.get("pub_date"),
            "decision_date": raw.get("decision_date"),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ESAsBoAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_decision_entries()
        for e in entries[:10]:
            ref = e.get('reference') or 'N/A'
            ddate = e.get('decision_date') or 'N/A'
            print(f"  {ref:20s}  {ddate}  {e['title'][:60]}")
        print(f"\nTotal: {len(entries)} decisions")
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
