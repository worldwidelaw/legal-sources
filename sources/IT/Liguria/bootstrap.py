#!/usr/bin/env python3
"""
IT/Liguria -- Raccolta Normativa Regione Liguria

Fetches regional laws from the Raccolta Normativa database via the official
bulk download endpoint of the Assemblea Legislativa della Liguria.

Strategy:
  - Download TXT ZIP archives per legislature (1-10) from the official endpoint
  - Parse each TXT file: header line (law ref + date), title, publication info, full text
  - Deduplicate by taking the latest version (_sN suffix = amended text, _vN = version)
  - Coverage: 1971-present (~2,400 laws across 10 legislatures)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from recent legislatures
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Liguria")

BASE_URL = "https://lrv.regione.liguria.it/liguriass_prod"
DOWNLOAD_URL = f"{BASE_URL}/class/download.php"

# Legislature numbers 1-10 (I through X)
LEGISLATURES = list(range(1, 11))

# Filename pattern: legge_YYYY_N[_sS]_vV.txt
FILENAME_RE = re.compile(
    r"legge_(\d{4})_(\d+)(?:_s(\d+))?_v(\d+)\.txt"
)

# Header patterns
# Format 1: § X.Y.Z - L.R. DD month YYYY, n. N.
HEADER_LR_RE = re.compile(
    r"L\.R\.\s+(\d{1,2})\s+(\w+)\s+(\d{4}),\s*n\.\s*(\d+)"
)
# Format 2: LEGGE REGIONALE DD MONTH YYYY[,] N. N
HEADER_LEGGE_RE = re.compile(
    r"LEGGE\s+REGIONALE\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*N\.\s*(\d+)",
    re.IGNORECASE,
)

# Publication line — must be at start of line (not inside article text)
PUBLICATION_RE = re.compile(
    r"^Bollettino\s+Ufficiale\s+n\.\s*(\d+)",
)

ITALIAN_MONTHS = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
}


def _parse_italian_date(day: str, month_name: str, year: str) -> Optional[str]:
    """Parse Italian date components to ISO 8601."""
    month_num = ITALIAN_MONTHS.get(month_name.lower())
    if not month_num:
        return None
    return f"{year}-{month_num}-{int(day):02d}"


class LiguriaScraper(BaseScraper):
    SOURCE_ID = "IT/Liguria"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "*/*",
        })

    def _download_legislature(self, leg_num: int) -> Optional[bytes]:
        """Download ZIP of TXT files for a legislature."""
        params = {
            "type": "zip",
            "formato": "txt",
            "tipo": "legge",
            "metaleg": str(leg_num),
        }
        for attempt in range(3):
            try:
                resp = self.session.get(DOWNLOAD_URL, params=params, timeout=120)
                resp.raise_for_status()
                if len(resp.content) < 50:
                    logger.warning(
                        "Legislature %d returned only %d bytes: %s",
                        leg_num, len(resp.content), resp.text[:100],
                    )
                    return None
                return resp.content
            except requests.RequestException as e:
                if attempt == 2:
                    logger.error("Failed to download legislature %d: %s", leg_num, e)
                    return None
                logger.warning("Download attempt %d failed for leg %d: %s", attempt + 1, leg_num, e)
                time.sleep(2 ** attempt)
        return None

    def _parse_txt_file(self, filename: str, content: str) -> Optional[Dict[str, Any]]:
        """Parse a single TXT file into a raw record."""
        # Strip BOM and normalize
        content = content.lstrip("\ufeff")
        # Split and remove leading/trailing blank lines
        raw_lines = content.split("\n")
        lines = []
        started = False
        for l in raw_lines:
            if not started and not l.strip():
                continue
            started = True
            lines.append(l)
        # Remove trailing blanks
        while lines and not lines[-1].strip():
            lines.pop()

        if len(lines) < 3:
            return None

        # Find the header line: either "§ ... L.R." or "LEGGE REGIONALE"
        header_idx = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if "L.R." in stripped or "l.r." in stripped:
                header_idx = i
                break
            if re.match(r"LEGGE\s+REGIONALE", stripped, re.IGNORECASE):
                header_idx = i
                break

        header_line = lines[header_idx].strip()

        # Find the Bollettino line (must start with "Bollettino", not inside text)
        bollettino_idx = None
        for i, line in enumerate(lines[header_idx + 1:], header_idx + 1):
            stripped = line.strip()
            if PUBLICATION_RE.match(stripped):
                bollettino_idx = i
                break

        if bollettino_idx is None:
            title = " ".join(
                l.strip() for l in lines[header_idx + 1:header_idx + 3]
            ).strip()
            text_start = header_idx + 2
            publication = ""
        else:
            title = " ".join(
                l.strip() for l in lines[header_idx + 1:bollettino_idx]
            ).strip()
            publication = lines[bollettino_idx].strip()
            text_start = bollettino_idx + 1

        # Parse date from header (try both formats)
        date_iso = None
        year = None
        number = None
        header_match = HEADER_LR_RE.search(header_line)
        if not header_match:
            header_match = HEADER_LEGGE_RE.search(header_line)
        if header_match:
            day, month_name, yr, num = header_match.groups()
            year = int(yr)
            number = int(num)
            date_iso = _parse_italian_date(day, month_name, yr)

        # Fallback: extract year/number from filename
        if year is None or number is None:
            fm = FILENAME_RE.match(filename)
            if fm:
                year = int(fm.group(1))
                number = int(fm.group(2))

        if year is None or number is None:
            logger.warning("Could not extract year/number from %s", filename)
            return None

        # Full text: everything after the publication line
        text_lines = lines[text_start:]
        # Clean article markers like [art1], [art1-com1]
        cleaned = []
        for line in text_lines:
            line = re.sub(r"\[art\d+(?:-com\d+)?\]\s*", "", line)
            cleaned.append(line)
        text = "\n".join(cleaned).strip()

        if len(text) < 20:
            logger.warning("Short text for law %d/%d (%d chars)", year, number, len(text))
            return None

        # Build URL to online view
        url = (
            f"{BASE_URL}/articolo?urndoc="
            f"urn:nir:regione.liguria:legge:{date_iso};{number}"
        ) if date_iso else f"{BASE_URL}/documenti-tree/leggi/"

        return {
            "year": year,
            "number": number,
            "title": title,
            "date": date_iso,
            "text": text,
            "url": url,
            "publication": publication,
        }

    def _select_latest_versions(
        self, files: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """Given (filename, content) pairs, keep only the latest version per law.

        Versioning: _sN = amendment sequence, _vN = internal version.
        For each (year, number), keep the file with the highest _s, then highest _v.
        If no _s suffix, treat s=0.
        """
        best: Dict[Tuple[int, int], Tuple[int, int, str, str]] = {}
        for filename, content in files:
            m = FILENAME_RE.match(filename)
            if not m:
                continue
            year, num = int(m.group(1)), int(m.group(2))
            s_num = int(m.group(3)) if m.group(3) else 0
            v_num = int(m.group(4))
            key = (year, num)
            if key not in best or (s_num, v_num) > (best[key][0], best[key][1]):
                best[key] = (s_num, v_num, filename, content)

        return [(entry[2], entry[3]) for entry in best.values()]

    def _process_legislature(self, leg_num: int) -> Generator[Dict[str, Any], None, None]:
        """Download and process all laws from a single legislature."""
        logger.info("Downloading legislature %d...", leg_num)
        zip_data = self._download_legislature(leg_num)
        if not zip_data:
            return

        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_data))
        except zipfile.BadZipFile:
            logger.error("Invalid ZIP for legislature %d", leg_num)
            return

        # Read all TXT files
        all_files = []
        for name in zf.namelist():
            if not name.endswith(".txt"):
                continue
            try:
                raw = zf.read(name)
                # Try UTF-8 first, fall back to latin-1
                try:
                    content = raw.decode("utf-8")
                except UnicodeDecodeError:
                    content = raw.decode("latin-1")
                all_files.append((Path(name).name, content))
            except Exception as e:
                logger.warning("Failed to read %s: %s", name, e)

        # Deduplicate to latest versions
        latest = self._select_latest_versions(all_files)
        logger.info("Legislature %d: %d files, %d unique laws", leg_num, len(all_files), len(latest))

        for filename, content in latest:
            raw = self._parse_txt_file(filename, content)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        year = raw.get("year", "")
        number = raw.get("number", "")
        return {
            "_id": f"IT/Liguria/LR-{year}-{number}",
            "_source": "IT/Liguria",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "law_number": f"LR {number}/{year}" if year and number else "",
            "publication": raw.get("publication", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for leg_num in reversed(LEGISLATURES):
            for raw in self._process_legislature(leg_num):
                yield self.normalize(raw)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # Fetch only the most recent legislatures (9 and 10)
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for leg_num in [10, 9]:
            for raw in self._process_legislature(leg_num):
                record = self.normalize(raw)
                if record.get("date") and record["date"] >= since:
                    yield record

    def test(self) -> bool:
        try:
            resp = self.session.get(
                DOWNLOAD_URL,
                params={"type": "zip", "formato": "txt", "tipo": "legge", "metaleg": "10"},
                timeout=30,
                stream=True,
            )
            resp.raise_for_status()
            # Check that we get a valid ZIP header
            header = resp.raw.read(4)
            resp.close()
            return header[:2] == b"PK"
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = LiguriaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if sample_mode else 999999

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(
                "[%d] %s — %d chars",
                count,
                record.get("law_number", record["_id"]),
                len(record.get("text", "")),
            )
            if count >= max_records:
                break

        logger.info("Done: %d records saved to %s", count, sample_dir)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else str(datetime.now().year - 1)
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
