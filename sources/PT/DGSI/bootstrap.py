#!/usr/bin/env python3
"""
PT/DGSI -- Portuguese Courts of Appeal & Other DGSI Databases

Fetches decisions from DGSI sub-databases not covered by PT/STA or PT/SupremeCourt:
  - Courts of Appeal: Porto, Lisbon, Coimbra, Guimarães, Évora
  - Central Administrative Courts: South, North
  - Court of Conflicts (Tribunal dos Conflitos)
  - Justices of the Peace (Julgados de Paz)

Strategy:
  - JSON enumeration via Lotus Domino ReadViewEntries endpoint
  - Full text via OpenDocument with ExpandSection=1
  - ISO-8859-1 encoding for HTML pages

Data:
  - ~230,000 decisions across 9 databases
  - Language: Portuguese
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.DGSI")

BASE_URL = "http://www.dgsi.pt"

# DGSI sub-databases: (db_code, court_name, short_prefix)
DATABASES = [
    ("jtrp", "Tribunal da Relação do Porto", "TRP"),
    ("jtrl", "Tribunal da Relação de Lisboa", "TRL"),
    ("jtrc", "Tribunal da Relação de Coimbra", "TRC"),
    ("jtrg", "Tribunal da Relação de Guimarães", "TRG"),
    ("jtre", "Tribunal da Relação de Évora", "TRE"),
    ("jtca", "Tribunal Central Administrativo Sul", "TCAS"),
    ("jtcn", "Tribunal Central Administrativo Norte", "TCAN"),
    ("jcon", "Tribunal dos Conflitos", "TCON"),
    ("cajp", "Julgados de Paz", "JP"),
]

# JSON pagination size
PAGE_SIZE = 500


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    return '\n'.join(lines).strip()


class DGSIScraper(BaseScraper):
    """
    Scraper for PT/DGSI -- Portuguese Courts of Appeal & other DGSI databases.
    Country: PT
    URL: https://www.dgsi.pt

    Data types: case_law
    Auth: none (Public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
            },
            timeout=60,
        )

    def _fetch_json_page(self, db: str, start: int = 1, count: int = PAGE_SIZE) -> Optional[dict]:
        """Fetch a JSON enumeration page from a DGSI database."""
        url = f"/{db}.nsf/Por+Ano?ReadViewEntries&Start={start}&Count={count}&OutputFormat=JSON"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"JSON page {db} start={start}: HTTP {resp.status_code}")
                return None
            text = resp.content.decode("utf-8", errors="replace").lstrip("\ufeff")
            return json.loads(text)
        except Exception as e:
            logger.warning(f"Error fetching JSON {db} start={start}: {e}")
            return None

    def _parse_entries(self, data: dict) -> List[Dict[str, Any]]:
        """Parse ReadViewEntries JSON into a list of entry dicts."""
        entries = []
        for ve in data.get("viewentry", []):
            unid = ve.get("@unid", "")
            if not unid:
                continue

            entry = {"unid": unid}
            for ed in ve.get("entrydata", []):
                name = ed.get("@name", "")
                if name == "$14":  # Date
                    dt = ed.get("datetime", {})
                    val = dt.get("0", "") if isinstance(dt, dict) else ""
                    entry["date_raw"] = val
                elif name == "PROCESSO":  # Case number
                    t = ed.get("text", {})
                    entry["case_number"] = t.get("0", "") if isinstance(t, dict) else ""
                elif name == "RELATOR":  # Rapporteur
                    t = ed.get("text", {})
                    entry["rapporteur"] = t.get("0", "") if isinstance(t, dict) else ""
                elif name == "$13":  # Descriptors
                    tl = ed.get("textlist", {})
                    if tl:
                        texts = tl.get("text", [])
                        entry["descriptors"] = [
                            t.get("0", "") if isinstance(t, dict) else str(t)
                            for t in texts
                        ]
                    else:
                        t = ed.get("text", {})
                        if t:
                            entry["descriptors"] = [t.get("0", "") if isinstance(t, dict) else str(t)]

            entries.append(entry)
        return entries

    def _fetch_document(self, db: str, unid: str) -> Optional[str]:
        """Fetch the full HTML of a decision page."""
        url = f"/{db}.nsf/0/{unid}?OpenDocument&ExpandSection=1"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                return None
            try:
                return resp.content.decode("iso-8859-1")
            except Exception:
                return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Error fetching doc {db}/{unid}: {e}")
            return None

    def _parse_document(self, html_content: str) -> Dict[str, Any]:
        """Parse a DGSI decision HTML page into fields."""
        result = {}

        def extract_field(label: str) -> Optional[str]:
            pattern = label + r'</font></b></td>\s*<td[^>]*><b><font[^>]*>([^<]+)</font>'
            m = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            return m.group(1).strip() if m else None

        result["case_number"] = extract_field(r'Processo:')
        result["date_doc"] = extract_field(r'Data do Acord[^<]*:')
        result["section"] = extract_field(r'Tribunal:')
        result["rapporteur"] = extract_field(r'Relator:')
        result["voting"] = extract_field(r'Vota[^<]*o:')

        # Summary
        sumario_match = re.search(
            r'Sum[^<]*rio:</font></b></td>[^<]*<td[^>]*>(.*?)</td>',
            html_content, re.DOTALL | re.IGNORECASE
        )
        if sumario_match:
            result["summary"] = clean_html(sumario_match.group(1))

        # Descriptors from document page
        desc_match = re.search(
            r'Descritores:</font></b></td><td[^>]*><b><font[^>]*>([^<]*(?:<br>[^<]*)*)</font>',
            html_content, re.DOTALL | re.IGNORECASE
        )
        if desc_match:
            desc_lines = re.split(r'<br\s*/?>', desc_match.group(1), flags=re.IGNORECASE)
            result["descriptors_doc"] = [clean_html(d).strip() for d in desc_lines if clean_html(d).strip()]

        # Full text: try "Decisão Texto Integral:" first (Courts of Appeal),
        # then "Texto Integral:" (Administrative courts)
        texto_match = re.search(
            r'Decis[^<]*Texto Integral:</font></b></td>\s*<td[^>]*>(.*?)</td>\s*</tr>',
            html_content, re.DOTALL | re.IGNORECASE
        )
        if texto_match:
            result["full_text"] = clean_html(texto_match.group(1))
        else:
            # Try plain "Texto Integral:" but skip the "S"/"N" indicator field
            # (We only reach here if "Decisão Texto Integral:" didn't match above)
            texto_match2 = re.search(
                r'Texto Integral:</font></b></td>\s*<td[^>]*>(.*?)</td>\s*</tr>',
                html_content, re.DOTALL | re.IGNORECASE
            )
            if texto_match2:
                candidate = clean_html(texto_match2.group(1))
                # The "Texto Integral:" field may just be "S" or "N" (indicator)
                if len(candidate) > 10:
                    result["full_text"] = candidate

        return result

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert DD/MM/YYYY or YYYYMMDD to ISO format."""
        if not date_str:
            return None
        # YYYYMMDD format from JSON
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        # DD/MM/YYYY format from HTML
        parts = date_str.split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all decisions from all DGSI sub-databases."""
        for db, court_name, prefix in DATABASES:
            logger.info(f"Starting database: {db} ({court_name})")

            # Get total count
            first_page = self._fetch_json_page(db, start=1, count=1)
            if not first_page:
                logger.warning(f"Cannot access {db}, skipping")
                continue
            total = int(first_page.get("@toplevelentries", "0"))
            logger.info(f"{db}: {total} total entries")

            start = 1
            fetched = 0
            while start <= total:
                data = self._fetch_json_page(db, start=start, count=PAGE_SIZE)
                if not data:
                    break

                entries = self._parse_entries(data)
                if not entries:
                    break

                for entry in entries:
                    unid = entry["unid"]
                    html_content = self._fetch_document(db, unid)
                    if not html_content:
                        continue

                    doc = self._parse_document(html_content)
                    doc["unid"] = unid
                    doc["db"] = db
                    doc["court_name"] = court_name
                    doc["prefix"] = prefix
                    doc["listing"] = entry
                    yield doc
                    fetched += 1

                start += PAGE_SIZE
                logger.info(f"{db}: fetched {fetched} documents so far (page start={start})")

            logger.info(f"Completed {db}: {fetched} documents")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions from all databases (first 2 pages each)."""
        for db, court_name, prefix in DATABASES:
            logger.info(f"Checking updates for {db}")
            data = self._fetch_json_page(db, start=1, count=PAGE_SIZE)
            if not data:
                continue

            entries = self._parse_entries(data)
            for entry in entries:
                date_str = self._parse_date(entry.get("date_raw", ""))
                if date_str and date_str < since.strftime("%Y-%m-%d"):
                    break

                unid = entry["unid"]
                html_content = self._fetch_document(db, unid)
                if not html_content:
                    continue

                doc = self._parse_document(html_content)
                doc["unid"] = unid
                doc["db"] = db
                doc["court_name"] = court_name
                doc["prefix"] = prefix
                doc["listing"] = entry
                yield doc

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw decision data into standard schema."""
        full_text = raw.get("full_text", "")
        if not full_text or len(full_text.strip()) < 100:
            return None

        prefix = raw.get("prefix", "DGSI")
        db = raw.get("db", "")
        listing = raw.get("listing", {})
        unid = raw.get("unid", "")

        case_number = raw.get("case_number") or listing.get("case_number", "")
        if case_number:
            id_str = f"{prefix}-{case_number.replace('/', '-').replace(' ', '')}"
        else:
            id_str = f"{prefix}-{unid[:16]}"

        date_str = raw.get("date_doc") or listing.get("date_raw", "")
        iso_date = self._parse_date(date_str)

        court_name = raw.get("court_name", "")
        title = f"Acórdão {prefix} {case_number}" if case_number else f"Acórdão {prefix} {unid[:12]}"

        descriptors = raw.get("descriptors_doc") or listing.get("descriptors", [])
        rapporteur = raw.get("rapporteur") or listing.get("rapporteur", "")

        url = f"{BASE_URL}/{db}.nsf/0/{unid}?OpenDocument"

        return {
            "_id": id_str,
            "_source": "PT/DGSI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "summary": raw.get("summary", ""),
            "date": iso_date,
            "url": url,
            "case_number": case_number,
            "rapporteur": rapporteur,
            "section": raw.get("section", ""),
            "descriptors": descriptors,
            "voting": raw.get("voting", ""),
            "court": court_name,
            "jurisdiction": "PT",
            "language": "pt",
            "db": db,
            "doc_id": unid,
        }

    def test_connection(self):
        """Quick connectivity test across all databases."""
        print("Testing DGSI sub-databases...")
        for db, court_name, prefix in DATABASES:
            data = self._fetch_json_page(db, start=1, count=1)
            if data:
                total = data.get("@toplevelentries", "?")
                entries = self._parse_entries(data)
                if entries:
                    unid = entries[0]["unid"]
                    html_content = self._fetch_document(db, unid)
                    if html_content:
                        doc = self._parse_document(html_content)
                        text_len = len(doc.get("full_text", ""))
                        print(f"  {db} ({prefix}): {total} entries, text={text_len} chars - OK")
                    else:
                        print(f"  {db} ({prefix}): {total} entries, doc fetch FAILED")
                else:
                    print(f"  {db} ({prefix}): {total} entries, no entries parsed")
            else:
                print(f"  {db} ({prefix}): FAILED")


def main():
    scraper = DGSIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
