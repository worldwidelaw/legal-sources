#!/usr/bin/env python3
"""
Hamburg State Court Decisions Fetcher (OVG & VG)

Fetches court decisions from the Hamburg justice portal (justiz.hamburg.de).
Covers:
- Oberverwaltungsgericht (OVG) — Higher Administrative Court
- Verwaltungsgericht (VG) — Administrative Court

Decisions are published as PDF files linked from court-specific pages.
Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

# PDF extraction
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://justiz.hamburg.de"

COURT_PAGES = {
    "OVG Hamburg": "/gerichte/oberverwaltungsgericht/entscheidungen",
    "VG Hamburg": "/gerichte/verwaltungsgericht-hamburg/rechtsprechung",
}

# German month names for date parsing
GERMAN_MONTHS = {
    "januar": "01", "februar": "02", "märz": "03", "april": "04",
    "mai": "05", "juni": "06", "juli": "07", "august": "08",
    "september": "09", "oktober": "10", "november": "11", "dezember": "12",
}


class HamburgCourtsFetcher:
    """Fetcher for Hamburg state court decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def _parse_german_date(self, date_str: str) -> Optional[str]:
        """Parse German date formats to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()

        # DD.MM.YYYY
        m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

        # DD. Monat YYYY
        m = re.match(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})", date_str)
        if m:
            month = GERMAN_MONTHS.get(m.group(2).lower())
            if month:
                return f"{m.group(3)}-{month}-{m.group(1).zfill(2)}"

        return None

    def _extract_case_number(self, text: str) -> Optional[str]:
        """Extract case number (Aktenzeichen) from text."""
        # Patterns like: 2 Bs 150/25, 1 E 12/22.P, 12 AE 1983/26
        m = re.search(r"\d+\s+[A-Za-z]+\s+\d+/\d+(?:\.\w+)?", text)
        return m.group(0).strip() if m else None

    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Extract date from surrounding text of a decision link."""
        # Look for DD.MM.YYYY pattern
        m = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4})", text)
        if m:
            return self._parse_german_date(m.group(1))

        # Look for date in filename pattern like "beschluss-vom-02-04-2026"
        m = re.search(r"vom[- ](\d{2})[- ](\d{2})[- ](\d{2,4})", text, re.IGNORECASE)
        if m:
            day, month = m.group(1), m.group(2)
            year = m.group(3)
            if len(year) == 2:
                year = "20" + year
            return f"{year}-{month}-{day}"

        return None

    def _scrape_court_page(self, court_name: str, page_path: str) -> List[Dict[str, Any]]:
        """Scrape a court page for PDF decision links."""
        url = BASE_URL + page_path
        logger.info(f"Scraping {court_name}: {url}")

        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        decisions = []

        # Find all PDF links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.endswith("-data.pdf") and ".pdf" not in href:
                continue
            if "/resource/blob/" not in href:
                continue

            pdf_url = href if href.startswith("http") else BASE_URL + href

            # Get surrounding text for metadata
            parent = link.find_parent(["li", "p", "div", "td"])
            context_text = parent.get_text(" ", strip=True) if parent else ""
            link_text = link.get_text(strip=True)

            # Extract case number
            aktenzeichen = self._extract_case_number(link_text)
            if not aktenzeichen:
                aktenzeichen = self._extract_case_number(context_text)
            if not aktenzeichen:
                # Try from filename
                filename = href.split("/")[-1]
                aktenzeichen = self._extract_case_number(filename.replace("-", " "))

            # Extract date
            date = self._extract_date_from_text(context_text)
            if not date:
                date = self._extract_date_from_text(href)

            # Extract description
            description = context_text[:300] if context_text else ""

            # Extract blob ID for dedup
            blob_m = re.search(r"/resource/blob/(\d+)/", href)
            blob_id = blob_m.group(1) if blob_m else None

            decisions.append({
                "court": court_name,
                "aktenzeichen": aktenzeichen or f"unknown-{blob_id}",
                "date": date,
                "description": description,
                "pdf_url": pdf_url,
                "blob_id": blob_id,
                "source_url": url,
            })

        logger.info(f"  Found {len(decisions)} PDF decisions on {court_name}")
        return decisions

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Fetch all decisions from all Hamburg courts."""
        count = 0
        seen_urls = set()

        for court_name, page_path in COURT_PAGES.items():
            try:
                decisions = self._scrape_court_page(court_name, page_path)
            except Exception as e:
                logger.error(f"Failed to scrape {court_name}: {e}")
                continue

            for dec in decisions:
                if limit and count >= limit:
                    return

                if dec["pdf_url"] in seen_urls:
                    continue
                seen_urls.add(dec["pdf_url"])

                # Download PDF and extract text
                logger.info(f"  Downloading PDF: {dec['aktenzeichen']}")
                try:
                    text = extract_pdf_markdown(
                        source="DE/HamburgCourts",
                        source_id=dec.get("blob_id") or dec["aktenzeichen"],
                        pdf_url=dec["pdf_url"],
                    )
                except Exception as e:
                    logger.warning(f"  PDF extraction failed for {dec['aktenzeichen']}: {e}")
                    text = None

                if not text or len(text.strip()) < 100:
                    logger.warning(f"  Skipping {dec['aktenzeichen']} — insufficient text ({len(text or '')} chars)")
                    continue

                dec["text"] = text
                count += 1
                yield dec

                time.sleep(2)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch decisions updated since a given date."""
        since_dt = datetime.fromisoformat(since)
        for doc in self.fetch_all():
            if doc.get("date"):
                try:
                    doc_dt = datetime.fromisoformat(doc["date"])
                    if doc_dt >= since_dt:
                        yield doc
                except ValueError:
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document to standard schema."""
        aktenzeichen = raw_doc.get("aktenzeichen", "unknown")
        court = raw_doc.get("court", "Hamburg")

        doc_id = f"DE/HamburgCourts/{aktenzeichen}"

        # Build title
        title_parts = [court, aktenzeichen]
        if raw_doc.get("date"):
            title_parts.append(raw_doc["date"])
        title = " — ".join(title_parts)

        return {
            "_id": doc_id,
            "_source": "DE/HamburgCourts",
            "_type": "case_law",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "title": title,
            "text": raw_doc.get("text", ""),
            "date": raw_doc.get("date"),
            "url": raw_doc.get("pdf_url", ""),
            "aktenzeichen": aktenzeichen,
            "court": court,
            "description": raw_doc.get("description", ""),
            "language": "de",
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        fetcher = HamburgCourtsFetcher()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        is_sample = "--sample" in sys.argv
        target = 15 if is_sample else None

        logger.info("Starting Hamburg Courts bootstrap...")
        saved = 0

        for raw_doc in fetcher.fetch_all(limit=(target + 5) if target else None):
            if target and saved >= target:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            if text_len < 100:
                continue

            doc_id = normalized["_id"].replace("/", "_").replace(" ", "_")
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"  Saved [{saved+1}]: {normalized['aktenzeichen']} ({text_len:,} chars)")
            saved += 1

        logger.info(f"Bootstrap complete. Saved {saved} documents to {sample_dir}")

        files = list(sample_dir.glob("*.json"))
        total_chars = sum(
            len(json.load(open(f, encoding="utf-8")).get("text", ""))
            for f in files
        )
        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")
    else:
        fetcher = HamburgCourtsFetcher()
        print("Testing Hamburg Courts fetcher...")
        for i, raw_doc in enumerate(fetcher.fetch_all(limit=3)):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {i+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Case: {normalized['aktenzeichen']}")
            print(f"Court: {normalized['court']}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")


if __name__ == "__main__":
    main()
