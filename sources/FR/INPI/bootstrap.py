#!/usr/bin/env python3
"""
FR/INPI -- French IP Office (INPI) Directives & Director General Decisions

Fetches INPI doctrine: directives on IP procedures + DG decisions on trademarks,
patents, designs, geographic indications, and registration formalities.

Strategy:
  - Scrape directives page for directive PDFs
  - Scrape DG decisions page for procedural decisions
  - Download PDFs via /inpi-block/download-document?id=XXXX
  - Extract text via common/pdf_extract.extract_pdf_markdown
  - 2-second crawl delay between requests

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.INPI")

BASE_URL = "https://www.inpi.fr"
DIRECTIVES_URL = f"{BASE_URL}/directives"
DECISIONS_URL = f"{BASE_URL}/ressources/propriete-intellectuelle/decisions-du-directeur-general-de-linpi"
DOWNLOAD_URL = f"{BASE_URL}/inpi-block/download-document"


class INPIScraper(BaseScraper):
    """Scraper for FR/INPI -- French IP Office directives and decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_directives_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse directives page for PDF links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []

        # Find download links with document IDs
        links = soup.find_all("a", href=lambda h: h and "download-document?id=" in str(h))
        for link in links:
            href = link.get("href", "")
            id_match = re.search(r"id=(\d+)", href)
            if not id_match:
                continue
            doc_id = id_match.group(1)

            # Get title from link text or parent context
            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                parent = link.find_parent(["li", "div", "p"])
                if parent:
                    title = parent.get_text(strip=True)[:200]

            # Skip very short titles (likely just icons)
            if not title or len(title) < 3:
                continue

            pdf_url = f"{DOWNLOAD_URL}?id={doc_id}"
            documents.append({
                "doc_id": doc_id,
                "title": title,
                "pdf_url": pdf_url,
                "category": "directive",
                "decision_number": "",
                "date": "",
            })

        return documents

    def _parse_decisions_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse DG decisions page for PDF links with metadata."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen_ids = set()

        # Find all download links
        links = soup.find_all("a", href=lambda h: h and "download-document?id=" in str(h))
        for link in links:
            href = link.get("href", "")
            id_match = re.search(r"id=(\d+)", href)
            if not id_match:
                continue
            doc_id = id_match.group(1)
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            # Get context from parent elements
            title = ""
            parent = link.find_parent(["li", "div", "p", "td"])
            if parent:
                title = parent.get_text(strip=True)[:300]

            # Extract decision number from title
            dec_match = re.search(r"[Dd]écision\s+n[°o]\s*([\d\w-]+)", title)
            decision_number = dec_match.group(1) if dec_match else ""

            # Extract date from title
            date = ""
            date_patterns = [
                r"(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})",
            ]
            months_fr = {
                "janvier": "01", "février": "02", "mars": "03", "avril": "04",
                "mai": "05", "juin": "06", "juillet": "07", "août": "08",
                "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
            }
            for pat in date_patterns:
                m = re.search(pat, title)
                if m:
                    day = m.group(1).zfill(2)
                    month = months_fr.get(m.group(2), "01")
                    year = m.group(3)
                    date = f"{year}-{month}-{day}"
                    break

            # Determine category from surrounding section headers
            category = self._detect_category(link, soup)

            pdf_url = f"{DOWNLOAD_URL}?id={doc_id}"
            documents.append({
                "doc_id": doc_id,
                "title": title[:200] if title else f"INPI-Decision-{doc_id}",
                "pdf_url": pdf_url,
                "category": category,
                "decision_number": decision_number,
                "date": date,
            })

        return documents

    def _detect_category(self, link, soup) -> str:
        """Detect category by looking at preceding headings."""
        # Walk up and back to find a heading
        for parent in link.parents:
            prev = parent.find_previous_sibling(["h2", "h3", "h4"])
            if prev:
                text = prev.get_text(strip=True).lower()
                if "marqu" in text:
                    return "trademarks"
                if "brevet" in text:
                    return "patents"
                if "dessin" in text or "modèle" in text:
                    return "designs"
                if "inscription" in text:
                    return "registrations"
                if "indication" in text or "géograph" in text:
                    return "geographic_indications"
                if "soleau" in text:
                    return "soleau_envelopes"
                if "délégation" in text or "signature" in text:
                    return "delegations"
                if "redevance" in text or "tarif" in text:
                    return "fees"
                if "examen" in text or "qualification" in text:
                    return "qualification_exams"
                if "fermeture" in text:
                    return "closures"
                if "organisation" in text:
                    return "organization"
                return text[:30]
            if parent.name == "body":
                break
        return "general"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw document data to standard schema."""
        doc_id = raw.get("doc_id", "")
        stable_id = f"INPI-{doc_id}"

        return {
            "_id": stable_id,
            "_source": "FR/INPI",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("pdf_url", ""),
            "decision_number": raw.get("decision_number", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all INPI directives and substantive DG decisions."""
        count = 0
        seen_ids = set()

        # 1. Fetch directives
        logger.info("Fetching directives page...")
        resp = self._request(DIRECTIVES_URL)
        if resp:
            docs = self._parse_directives_page(resp.text)
            logger.info(f"Found {len(docs)} directives")
            for doc in docs:
                if doc["doc_id"] in seen_ids:
                    continue
                seen_ids.add(doc["doc_id"])
                doc = self._extract_pdf_text(doc)
                if doc:
                    count += 1
                    yield doc

        # 2. Fetch DG decisions (substantive ones only)
        logger.info("Fetching DG decisions page...")
        resp = self._request(DECISIONS_URL)
        if resp:
            docs = self._parse_decisions_page(resp.text)
            # Filter to substantive categories
            substantive = {"trademarks", "patents", "designs", "registrations",
                          "geographic_indications", "fees", "general"}
            docs = [d for d in docs if d["category"] in substantive]
            logger.info(f"Found {len(docs)} substantive decisions")
            for doc in docs:
                if doc["doc_id"] in seen_ids:
                    continue
                seen_ids.add(doc["doc_id"])
                doc = self._extract_pdf_text(doc)
                if doc:
                    count += 1
                    yield doc

        logger.info(f"Completed: {count} documents fetched")

    def _extract_pdf_text(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download PDF and extract text."""
        stable_id = f"INPI-{doc['doc_id']}"
        try:
            md = extract_pdf_markdown(
                source="FR/INPI",
                source_id=stable_id,
                pdf_url=doc["pdf_url"],
                table="doctrine",
            )
            if md and len(md) >= 100:
                doc["text"] = md
                return doc
            else:
                logger.warning(f"Insufficient text from PDF: {doc['title'][:60]}")
                return None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {doc['title'][:60]}: {e}")
            return None

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (same as fetch_all for this source)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(DIRECTIVES_URL)
        if resp is None:
            logger.error("Cannot reach INPI directives page")
            return False

        docs = self._parse_directives_page(resp.text)
        if not docs:
            logger.error("No directives found on page")
            return False

        logger.info(f"Directives page OK: {len(docs)} documents found")
        doc = docs[0]
        logger.info(f"  Title: {doc['title'][:80]}")
        logger.info(f"  PDF URL: {doc['pdf_url']}")

        # Test PDF download
        pdf_resp = self._request(doc["pdf_url"])
        if pdf_resp and len(pdf_resp.content) > 1000:
            logger.info(f"  PDF download OK: {len(pdf_resp.content)} bytes")
            return True
        else:
            logger.error("  PDF download failed")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="FR/INPI data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = INPIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
