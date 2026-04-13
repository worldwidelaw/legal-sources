#!/usr/bin/env python3
"""
US/LA-Legislation -- Louisiana Revised Statutes (legis.la.gov)

Fetches Louisiana Revised Statutes with full text from the official
Louisiana Legislature website.

Strategy:
  1. For each RS title folder (77-130), fetch Laws_Toc.aspx?folder=N
     to discover all section document IDs (Law.aspx?d=NNNNN links)
  2. For each section, fetch LawPrint.aspx?d=NNNNN for clean HTML
  3. Extract citation from <span id="LabelName"> and full text from
     <span id="LabelDocument">, strip HTML tags
  4. Normalize into standard schema

Data: Public domain (Louisiana government works). No auth required.
Rate limit: 1 req / 1 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull (all titles)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.LA-Legislation")

BASE_URL = "https://legis.la.gov/legis"

# RS title folders (77-130) mapped to title numbers
RS_TITLE_FOLDERS = [
    (77, "1", "General Provisions"),
    (78, "2", "Aeronautics"),
    (79, "3", "Agriculture and Forestry"),
    (80, "4", "Amusements and Sports"),
    (81, "6", "Banks and Banking"),
    (82, "8", "Cemeteries"),
    (83, "9", "Civil Code-Ancillaries"),
    (84, "10", "Commercial Laws"),
    (85, "11", "Consolidated Public Retirement"),
    (86, "12", "Corporations and Associations"),
    (87, "13", "Courts and Judicial Procedure"),
    (88, "14", "Criminal Law"),
    (89, "15", "Criminal Procedure"),
    (90, "16", "District Attorneys"),
    (91, "17", "Education"),
    (92, "18", "Louisiana Election Code"),
    (93, "19", "Expropriation"),
    (94, "20", "Homesteads and Exemptions"),
    (95, "21", "Hotels and Lodging Houses"),
    (96, "22", "Insurance"),
    (97, "23", "Labor and Workers Compensation"),
    (98, "24", "Legislature and Laws"),
    (99, "25", "Libraries, Museums, and Other Scientific"),
    (100, "26", "Liquors-Alcoholic Beverages"),
    (101, "27", "Louisiana Gaming Control Law"),
    (102, "28", "Behavioral Health"),
    (103, "29", "Military, Naval, and Veterans Affairs"),
    (104, "30", "Minerals, Oil, Gas and Environmental Quality"),
    (105, "31", "Mineral Code"),
    (106, "32", "Motor Vehicles and Traffic Regulation"),
    (107, "33", "Municipalities and Parishes"),
    (108, "34", "Navigation and Shipping"),
    (109, "35", "Notaries Public and Commissioners"),
    (110, "36", "Organization of the Executive Branch"),
    (111, "37", "Professions and Occupations"),
    (112, "38", "Public Contracts, Works and Improvements"),
    (113, "39", "Public Finance"),
    (114, "40", "Public Health and Safety"),
    (115, "41", "Public Lands"),
    (116, "42", "Public Officers and Employees"),
    (117, "43", "Public Printing and Advertisements"),
    (118, "44", "Public Records and Recorders"),
    (119, "45", "Public Utilities and Carriers"),
    (120, "46", "Public Welfare and Assistance"),
    (121, "47", "Revenue and Taxation"),
    (122, "48", "Roads, Bridges and Ferries"),
    (123, "49", "State Administration"),
    (124, "50", "Surveys and Surveyors"),
    (125, "51", "Trade and Commerce"),
    (126, "52", "United States"),
    (127, "53", "War Emergency"),
    (128, "54", "Warehouses"),
    (129, "55", "Weights and Measures"),
    (130, "56", "Wildlife and Fisheries"),
]

# Sample folders for --sample mode (spread across titles)
SAMPLE_FOLDERS = [77, 88, 96, 107, 121]


class LALegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html,*/*",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def _strip_html(self, html_text: str) -> str:
        """Strip HTML tags and clean up text."""
        # Remove HTML tags
        text = re.sub(r'<br\s*/?>', '\n', html_text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        # Decode HTML entities
        text = html_module.unescape(text)
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _get_section_ids(self, folder_id: int) -> list:
        """Get all section document IDs from a TOC folder page.
        Returns list of (doc_id, section_text)."""
        url = f"{BASE_URL}/Laws_Toc.aspx?folder={folder_id}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch TOC folder {folder_id}: {e}")
            return []

        # Extract Law.aspx?d=NNNNN links and their text
        matches = re.findall(
            r'Law\.aspx\?d=(\d+)[^>]*>([^<]+)',
            html,
        )
        seen = set()
        result = []
        for doc_id, link_text in matches:
            if doc_id not in seen:
                seen.add(doc_id)
                result.append((doc_id, link_text.strip()))
        return result

    def _fetch_section(self, doc_id: str) -> Optional[dict]:
        """Fetch full text for a single section via LawPrint.aspx."""
        url = f"{BASE_URL}/LawPrint.aspx?d={doc_id}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch LawPrint d={doc_id}: {e}")
            return None

        # Extract citation from <span id="LabelName">
        m_name = re.search(
            r'<span\s+id="LabelName"[^>]*>([^<]+)</span>',
            html, re.IGNORECASE,
        )
        citation = m_name.group(1).strip() if m_name else ""

        # Extract full text from <span id="LabelDocument">
        m_doc = re.search(
            r'<span\s+id="LabelDocument">(.*?)</span>\s*</div>',
            html, re.IGNORECASE | re.DOTALL,
        )
        if not m_doc:
            # Try broader match
            m_doc = re.search(
                r'<span\s+id="LabelDocument">(.*?)(?:</span>)',
                html, re.IGNORECASE | re.DOTALL,
            )

        if not m_doc:
            logger.warning(f"No LabelDocument found for d={doc_id}")
            return None

        raw_html = m_doc.group(1)
        text = self._strip_html(raw_html)

        if not text or len(text) < 10:
            logger.warning(f"Text too short for d={doc_id}: {len(text) if text else 0}")
            return None

        return {
            "doc_id": doc_id,
            "citation": citation,
            "text": text,
            "url": f"{BASE_URL}/Law.aspx?d={doc_id}",
        }

    def _parse_citation(self, citation: str) -> dict:
        """Parse a citation like 'RS 1:1' or 'CC 1' into components."""
        parts = {"code": "", "section_num": "", "title_num": ""}
        # Match patterns like "RS 44:1", "CC 100", "CCP 1"
        m = re.match(r'(RS|CC|CCP|CCrP|CE|CHC|CA)\s+(\S+)', citation)
        if m:
            parts["code"] = m.group(1)
            parts["section_num"] = m.group(2)
            # For RS, extract title number (before the colon)
            if ":" in parts["section_num"]:
                parts["title_num"] = parts["section_num"].split(":")[0]
        return parts

    def test_api(self):
        """Test connectivity to legis.la.gov."""
        logger.info("Testing Louisiana Legislature website...")
        try:
            # Test TOC page
            sections = self._get_section_ids(77)
            if not sections:
                logger.error("API test FAILED: no sections found in folder 77")
                return False
            logger.info(f"  TOC folder 77: OK ({len(sections)} sections)")

            # Test LawPrint page — skip title-level entries (e.g., "RS 1")
            doc_id = None
            for sid, lt in sections:
                if ":" in lt:  # actual section like "RS 1:1"
                    doc_id = sid
                    break
            if not doc_id:
                doc_id = sections[1][0] if len(sections) > 1 else sections[0][0]
            result = self._fetch_section(doc_id)
            if result and len(result["text"]) > 50:
                logger.info(f"  LawPrint d={doc_id}: OK ({len(result['text'])} chars)")
                logger.info(f"  Citation: {result['citation']}")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: could not extract text")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict, title_num: str = "", title_name: str = "") -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        citation = raw["citation"]
        parsed = self._parse_citation(citation)

        # Build a clean ID
        safe_citation = re.sub(r'[^A-Za-z0-9:.-]', '_', citation)
        doc_id = f"LA-{safe_citation}" if safe_citation else f"LA-{raw['doc_id']}"

        # Use citation as title, or fallback
        title = citation if citation else f"LA Doc {raw['doc_id']}"

        return {
            "_id": doc_id,
            "_source": "US/LA-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "rs_title": title_num or parsed.get("title_num", ""),
            "rs_title_name": title_name,
            "section_num": parsed.get("section_num", citation),
            "code": parsed.get("code", "RS"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all statute sections across all RS titles."""
        total = 0
        for folder_id, title_num, title_name in RS_TITLE_FOLDERS:
            sections = self._get_section_ids(folder_id)
            logger.info(f"  Title {title_num} ({title_name}): {len(sections)} sections")
            for doc_id, link_text in sections:
                # Skip title-level entries (e.g., "RS 1" with no section)
                if re.match(r'^(RS|TITLE)\s+\d+[A-Z]?\s*$', link_text.strip()):
                    continue
                raw = self._fetch_section(doc_id)
                if raw:
                    yield self.normalize(raw, title_num, title_name)
                    total += 1
                    if total % 100 == 0:
                        logger.info(f"  Progress: {total} sections fetched")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample of sections from selected titles."""
        logger.info("Fetching sample sections from selected titles...")
        # Build lookup
        folder_lookup = {f: (t, n) for f, t, n in RS_TITLE_FOLDERS}

        count = 0
        target = 15
        for folder_id in SAMPLE_FOLDERS:
            if count >= target:
                break
            info = folder_lookup.get(folder_id)
            if not info:
                continue
            title_num, title_name = info
            sections = self._get_section_ids(folder_id)
            logger.info(f"  Title {title_num} ({title_name}): {len(sections)} sections")

            # Pick first 3 actual sections (skip title-level entries)
            picked = 0
            for doc_id, link_text in sections:
                if picked >= 3 or count >= target:
                    break
                # Skip title-level or repealed entries
                lt = link_text.strip()
                if re.match(r'^(RS|TITLE)\s+\d+[A-Z]?\s*$', lt):
                    continue
                if "repealed" in lt.lower():
                    continue
                raw = self._fetch_section(doc_id)
                if raw:
                    yield self.normalize(raw, title_num, title_name)
                    count += 1
                    picked += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/LA-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = LALegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
