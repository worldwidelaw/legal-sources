#!/usr/bin/env python3
"""
IT/Puglia -- Legislazione Regionale Puglia

Fetches regional laws and regulations from the Bussola Normativa database
of the Consiglio Regionale della Puglia.

Strategy:
  - Year-by-year search via ASP.NET POST form (RicercaSemplice.aspx)
  - Paginate through results (20 per page) using __doPostBack
  - Fetch each law's full text from LeggeNavscroll.aspx?id=<ID>
  - Coverage: 1972-present (~1500+ laws and regulations)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from recent years
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Puglia")

BASE_URL = "https://bussolanormativa.consiglio.puglia.it/public/Leges"
SEARCH_URL = f"{BASE_URL}/RicercaSemplice.aspx"
DETAIL_URL = f"{BASE_URL}/LeggeNavscroll.aspx"

FIRST_YEAR = 1972
CURRENT_YEAR = datetime.now().year

# Document type codes for the search form
DOC_TYPES = {"L": "Legge Regionale", "R": "Regolamento Regionale"}


class PugliaScraper(BaseScraper):
    SOURCE_ID = "IT/Puglia"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.encoding = "utf-8"
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _post(self, url: str, data: Dict) -> requests.Response:
        for attempt in range(3):
            try:
                resp = self.session.post(url, data=data, timeout=60)
                resp.encoding = "utf-8"
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("POST attempt %d failed: %s", attempt + 1, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _get_hidden_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract ASP.NET hidden form fields (ViewState etc.)."""
        fields = {}
        for inp in soup.find_all("input", type="hidden"):
            name = inp.get("name", "")
            if name:
                fields[name] = inp.get("value", "")
        return fields

    def _search_year(self, year: int, doc_type: str = "L") -> List[Dict[str, Any]]:
        """Search for all laws/regulations in a given year, handling pagination."""
        results = []

        # GET the search page for fresh ViewState
        resp = self._get(SEARCH_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        data = self._get_hidden_fields(soup)

        # Set search parameters
        type_field = "ctl00$ContentPlaceHolder1$Leggi" if doc_type == "L" else "ctl00$ContentPlaceHolder1$Regolamenti"
        data[type_field] = doc_type
        data["ctl00$ContentPlaceHolder1$cmbAnno"] = str(year)
        data["ContentPlaceHolder1_cmbAnno_VI"] = str(year)
        data["ctl00$ContentPlaceHolder1$btnInvia"] = "  Avvia la Ricerca"

        resp = self._post(SEARCH_URL, data)
        page_results, page_count = self._parse_search_results(resp.text, doc_type)
        results.extend(page_results)

        # Handle pagination (page 2, 3, ...)
        for page_num in range(2, page_count + 1):
            time.sleep(1.0)
            soup = BeautifulSoup(resp.text, "html.parser")
            data = self._get_hidden_fields(soup)
            data["__EVENTTARGET"] = f"ctl00$ContentPlaceHolder1$Link{page_num}"
            data["__EVENTARGUMENT"] = ""
            resp = self._post(SEARCH_URL, data)
            page_results, _ = self._parse_search_results(resp.text, doc_type)
            results.extend(page_results)

        return results

    def _parse_search_results(self, html: str, doc_type: str = "L") -> Tuple[List[Dict[str, Any]], int]:
        """Parse search results page. Returns (results, total_pages)."""
        soup = BeautifulSoup(html, "html.parser")
        results = []

        table = soup.find("table")
        if not table:
            return [], 0

        rows = table.find_all("tr")
        if len(rows) < 2:
            return [], 0

        # Build column map from header, then adjust for data rows.
        # Header may have an "Anno" column that is absent from data rows,
        # so we detect the offset by comparing column counts.
        header_cells = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
        first_data_row = rows[1].find_all("td")
        offset = len(header_cells) - len(first_data_row)
        if offset < 0:
            offset = 0

        col_map = {}
        for i, h in enumerate(header_cells):
            idx = i - offset  # adjusted index for data rows
            if idx < 0:
                continue
            if "numero" in h and "number" not in col_map:
                col_map["number"] = idx
            elif "data" in h and "vigenza" not in h and "date" not in col_map:
                col_map["date"] = idx
            elif "genere" in h:
                col_map["genre"] = idx
            elif "titolo" in h:
                col_map["title"] = idx
            elif "abrogat" in h:
                col_map["abrogated"] = idx

        genre_label = "Legge Regionale" if doc_type == "L" else "Regolamento Regionale"

        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            links = row.find_all("a", href=lambda h: h and "Navscroll" in h)
            if not links:
                continue

            href = links[0]["href"]
            match = re.search(r"id=(\d+)", href)
            if not match:
                continue

            doc_id = match.group(1)
            n_cells = len(cells)
            title = cells[col_map["title"]].get_text(strip=True) if "title" in col_map and col_map["title"] < n_cells else ""
            law_num = cells[col_map["number"]].get_text(strip=True) if "number" in col_map and col_map["number"] < n_cells else ""
            date_str = cells[col_map["date"]].get_text(strip=True) if "date" in col_map and col_map["date"] < n_cells else ""
            genre = cells[col_map["genre"]].get_text(strip=True) if "genre" in col_map and col_map["genre"] < n_cells else ""
            abrogated_txt = cells[col_map["abrogated"]].get_text(strip=True) if "abrogated" in col_map and col_map["abrogated"] < n_cells else ""

            if not genre:
                genre = genre_label

            results.append({
                "doc_id": doc_id,
                "title": title,
                "law_number": law_num,
                "date_raw": date_str,
                "genre": genre,
                "doc_type": doc_type,
                "abrogated": abrogated_txt.lower() == "si",
            })

        # Count pagination links
        panel = soup.find(id="ContentPlaceHolder1_panelPagesLink")
        page_count = 0
        if panel:
            page_links = panel.find_all("a")
            page_count = len(page_links)

        return results, page_count

    def _fetch_law_text(self, doc_id: str) -> Dict[str, Any]:
        """Fetch full text and metadata from a law detail page."""
        url = f"{DETAIL_URL}?id={doc_id}"
        resp = self._get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract metadata from the details tab
        metadata = {}
        tab1 = soup.find(id="tab1b")
        if tab1:
            text = tab1.get_text(" ", strip=True)
            # Parse structured fields
            m = re.search(r"Anno\s+(\d{4})", text)
            if m:
                metadata["year"] = m.group(1)
            m = re.search(r"Numero\s+(\d+)", text)
            if m:
                metadata["number"] = m.group(1)
            m = re.search(r"Data\s+(\d{2}/\d{2}/\d{4})", text)
            if m:
                metadata["date"] = m.group(1)
            m = re.search(r"Materia\s+(.+?)(?:Note|$)", text)
            if m:
                metadata["subject"] = m.group(1).strip().rstrip(";")

        # Extract full text from the main content area
        col8 = soup.find("div", class_="it-page-sections-container")
        full_text = ""
        title = ""

        if col8:
            # Get the title from <strong> element
            title_el = col8.find("strong")
            if title_el:
                title = title_el.get_text(" ", strip=True)

            # Remove navigation sidebar
            for nav in col8.find_all("nav"):
                nav.decompose()
            # Remove tab navigation
            for ul in col8.find_all("ul", class_="nav-tabs"):
                ul.decompose()
            # Remove metadata tabs
            for tab_id in ["tab1b", "myTab3Content", "tab2b", "tab4b", "tab5b"]:
                el = col8.find(id=tab_id)
                if el:
                    el.decompose()
            # Remove script tags
            for script in col8.find_all("script"):
                script.decompose()

            # Extract text from remaining content
            full_text = col8.get_text("\n", strip=True)

            # Clean up
            lines = [l.strip() for l in full_text.split("\n") if l.strip()]
            skip_phrases = [
                "Vai alla Ricerca Semplice",
                "Vai alla Ricerca Avanzata",
                "Vai al B.U.R.P",
                "Iter Legis",
                "Back",
                "Articolazione",
                "close",
            ]
            lines = [l for l in lines if not any(s in l for s in skip_phrases)]
            full_text = "\n".join(lines)

        metadata["title"] = title
        metadata["text"] = full_text
        metadata["url"] = url
        return metadata

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert dd/MM/yyyy to ISO 8601."""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        doc_id = raw.get("doc_id", "")
        detail = raw.get("detail", {})

        year = detail.get("year", "")
        number = detail.get("number", "")
        # doc_type is authoritative: the search form is queried separately for
        # Leggi Regionali ("L") and Regolamenti Regionali ("R"). The scraped
        # "genre" table cell is unreliable (column detection can pick up the
        # subject text), so derive the prefix and genre label from doc_type.
        doc_type = raw.get("doc_type", "L")
        genre = "Legge Regionale" if doc_type == "L" else "Regolamento Regionale"
        prefix = "LR" if doc_type == "L" else "RR"
        law_id = f"{prefix}-{year}-{number}" if year and number else f"id-{doc_id}"

        date_str = self._parse_date(detail.get("date", raw.get("date_raw", "")))
        title = detail.get("title", raw.get("title", ""))
        text = detail.get("text", "")

        return {
            "_id": f"IT/Puglia/{law_id}",
            "_source": "IT/Puglia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": detail.get("url", ""),
            "law_number": f"{prefix} {number}/{year}" if year and number else "",
            "subject": detail.get("subject", ""),
            "abrogated": raw.get("abrogated", False),
            "genre": genre,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all laws and regulations, year by year."""
        for year in range(CURRENT_YEAR, FIRST_YEAR - 1, -1):
            for doc_type, type_name in DOC_TYPES.items():
                logger.info("Searching %s for year %d...", type_name, year)
                try:
                    entries = self._search_year(year, doc_type)
                except Exception as e:
                    logger.error("Failed to search %s %d: %s", type_name, year, e)
                    continue

                logger.info("Found %d %s for %d", len(entries), type_name, year)
                for entry in entries:
                    time.sleep(1.5)
                    try:
                        detail = self._fetch_law_text(entry["doc_id"])
                        entry["detail"] = detail
                        # Yield the RAW entry (with detail attached); the framework
                        # calls normalize() itself. Yielding the normalized record
                        # here double-normalized on the ingest host — normalize then
                        # read the absent "detail" key, so every record's text was
                        # empty (issue #933).
                        text = (detail or {}).get("text", "")
                        if len(text) < 50:
                            logger.warning(
                                "Short text for %s (%d chars), skipping",
                                entry["doc_id"],
                                len(text),
                            )
                            continue
                        yield entry
                    except Exception as e:
                        logger.error("Failed to fetch doc %s: %s", entry["doc_id"], e)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch laws from recent years."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_year = int(since[:4])
        except (ValueError, IndexError):
            since_year = CURRENT_YEAR - 1

        for year in range(CURRENT_YEAR, since_year - 1, -1):
            for doc_type, type_name in DOC_TYPES.items():
                logger.info("Updating %s for year %d...", type_name, year)
                try:
                    entries = self._search_year(year, doc_type)
                except Exception as e:
                    logger.error("Failed to search %s %d: %s", type_name, year, e)
                    continue

                for entry in entries:
                    time.sleep(1.5)
                    try:
                        detail = self._fetch_law_text(entry["doc_id"])
                        entry["detail"] = detail
                        if len((detail or {}).get("text", "")) >= 50:
                            yield entry  # RAW — framework normalizes
                    except Exception as e:
                        logger.error("Failed to fetch doc %s: %s", entry["doc_id"], e)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self._get(SEARCH_URL)
            return resp.status_code == 200
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    scraper = PugliaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if sample_mode else 999999

        if sample_mode:
            # In sample mode, fetch a few years to get 15+ records quickly
            for year in [2024, 2023, 2022]:
                for doc_type in ["L", "R"]:
                    if count >= max_records:
                        break
                    logger.info("Sample: searching %s for %d", doc_type, year)
                    try:
                        entries = scraper._search_year(year, doc_type)
                    except Exception as e:
                        logger.error("Search failed: %s", e)
                        continue

                    for entry in entries:
                        if count >= max_records:
                            break
                        time.sleep(1.5)
                        try:
                            detail = scraper._fetch_law_text(entry["doc_id"])
                            entry["detail"] = detail
                            record = scraper.normalize(entry)
                            if len(record.get("text", "")) < 50:
                                logger.warning("Short text for %s, skipping", entry["doc_id"])
                                continue
                            out_path = sample_dir / f"{count:04d}.json"
                            with open(out_path, "w", encoding="utf-8") as f:
                                json.dump(record, f, ensure_ascii=False, indent=2)
                            count += 1
                            logger.info(
                                "[%d] %s — %d chars",
                                count,
                                record.get("law_number", record["_id"]),
                                len(record["text"]),
                            )
                        except Exception as e:
                            logger.error("Failed doc %s: %s", entry["doc_id"], e)
        else:
            # Full run: stream normalized records to data/records.jsonl so the
            # ingest pipeline persists them (previously dumped to sample/ only).
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            jsonl_path = data_dir / "records.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for entry in scraper.fetch_all():
                    record = scraper.normalize(entry)
                    if not record.get("_id") or len(record.get("text", "")) < 50:
                        continue
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 100 == 0:
                        logger.info("Progress: %d records written", count)
            logger.info("Done: %d records -> %s", count, jsonl_path)

        if sample_mode:
            logger.info("Done: %d records saved to %s", count, sample_dir)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else str(CURRENT_YEAR - 1)
        count = 0
        for entry in scraper.fetch_updates(since):
            record = scraper.normalize(entry)
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
