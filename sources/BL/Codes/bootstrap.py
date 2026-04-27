#!/usr/bin/env python3
"""
BL/Codes -- Saint-Barthélemy Codes & Regulations

Fetches legislation from the Collectivité de Saint-Barthélemy.

Two data sources:
  1. Consolidated codes from comstbarth.fr (Code des Contributions, Code de
     l'Urbanisme, etc.) — large PDFs with excellent text.
  2. Official acts (Délibérations CT, Arrêtés) from actes.eservices-comstbarth.fr
     via a session-based pagination API — PDFs, some scanned.

Strategy:
  - Consolidated codes: known PDF URLs, download and extract text, split by
    chapter if desired.
  - Actes portal: POST to jo_consult.php to set session filters, then POST to
    pagination.php for paginated HTML results. Parse rows, download PDFs,
    extract text via common.pdf_extract.

Data Coverage:
  - ~5 consolidated codes (500K+ chars of full text)
  - ~1,600 Délibérations CT (Territorial Council legislative acts)
  - ~5,000 Arrêtés (administrative regulations)
  - Many older acts are scanned PDFs; those with <50 chars of text are skipped.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BL.Codes")

ACTES_BASE = "https://actes.eservices-comstbarth.fr/"
COMSTBARTH_BASE = "https://www.comstbarth.fr/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Known consolidated code PDFs on comstbarth.fr
CONSOLIDATED_CODES = [
    {
        "id": "CODE-CONTRIBUTIONS",
        "title": "Code des Contributions de Saint-Barthélemy",
        "url": (
            "https://www.comstbarth.fr/in/rest/annotationSVC/Attachment/"
            "attach_cmsUpload_a687c0be-3095-4dfc-8921-f4a19be57d68"
        ),
    },
    {
        "id": "CODE-URBANISME",
        "title": "Code de l'Urbanisme, de l'Habitation et de la Construction",
        "url": (
            "https://www.comstbarth.fr/in/rest/annotationSVC/Attachment/"
            "attach_cmsUpload_fabcb0e1-5638-4313-ab87-cfdb7a4b72bd"
        ),
    },
    {
        "id": "REGLEMENT-CARTE-URBANISME",
        "title": "Règlement de la Carte d'Urbanisme de Saint-Barthélemy",
        "url": (
            "https://www.comstbarth.fr/in/rest/annotationSVC/Attachment/"
            "attach_cmsUpload_1ff974ce-75ae-47c4-ad46-f260670a33b1"
        ),
    },
    {
        "id": "CODE-CONTRIBUTIONS-2024",
        "title": "Code des Contributions (annexe délibération 2024-038 CT)",
        "url": (
            "https://actes.eservices-comstbarth.fr/PJ/Deliberation%20CT/"
            "Deliberation%20CT_2024/2024_038ct_annexe.pdf"
        ),
    },
]

# Act types to fetch from the actes portal
ACT_TYPES = [
    {"filter": "Délibération CT", "label": "Délibération CT"},
    {"filter": "Arrêté", "label": "Arrêté"},
]

# Minimum text length to consider a PDF as having extractable content
# Many older acts are scanned PDFs where only dates/stamps are extractable (~50 chars)
MIN_TEXT_CHARS = 100


class BLCodesScraper(BaseScraper):
    """
    Scraper for BL/Codes -- Saint-Barthélemy Codes & Regulations.
    Country: BL
    URL: https://actes.eservices-comstbarth.fr/jo_consult.php

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "fr,en;q=0.9",
        })

    # ── Consolidated code helpers ──────────────────────────────────────

    def _fetch_code_pdf(self, code: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download a consolidated code PDF and extract text."""
        url = code["url"]
        doc_id = code["id"]
        logger.info(f"Fetching consolidated code: {code['title']}")

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
            if not resp.content or resp.content[:4] != b"%PDF":
                logger.warning(f"Invalid PDF from {url}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {url}: {e}")
            return None

        text = extract_pdf_markdown(
            source="BL/Codes",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        )
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for {doc_id}: {len(text or '')} chars")
            return None

        logger.info(f"  {doc_id}: {len(text)} chars extracted")
        return {
            "doc_id": doc_id,
            "title": code["title"],
            "full_text": text,
            "url": url,
            "doc_type": "Code",
            "date_raw": "",
            "reference": doc_id,
        }

    # ── Actes portal helpers ──────────────────────────────────────────

    def _init_actes_session(self, act_type_filter: str, year: str = "") -> bool:
        """Initialize the actes portal session with search filters."""
        try:
            self.rate_limiter.wait()
            self.session.get(f"{ACTES_BASE}jo_consult.php", timeout=30)

            self.rate_limiter.wait()
            resp = self.session.post(
                f"{ACTES_BASE}jo_consult.php",
                data={
                    "annee_select": year,
                    "check_type_actes": act_type_filter,
                    "mot_clef": "",
                    "chercher": "",
                },
                timeout=30,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"Session init failed: {e}")
            return False

    def _fetch_actes_page(self, page_no: int) -> str:
        """Fetch a pagination page from the actes portal."""
        try:
            self.rate_limiter.wait()
            resp = self.session.post(
                f"{ACTES_BASE}pagination.php",
                data={"page_no": str(page_no)},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"Pagination page {page_no} failed: {e}")
        return ""

    def _get_total_results(self, html: str) -> int:
        """Extract total result count from pagination HTML."""
        m = re.search(r'Résultats correspondants à la recherche:\s*(\d+)', html)
        return int(m.group(1)) if m else 0

    def _parse_actes_rows(self, html: str) -> List[Dict[str, Any]]:
        """Parse document rows from pagination HTML."""
        rows = []
        # Split by <tr> tags and process each row
        tr_blocks = re.findall(r"<tr>(.*?)</tr>", html, re.DOTALL)

        for block in tr_blocks:
            # Skip header rows
            if "<th" in block:
                continue

            # Extract all <td> cells (handling attributes like class, title, align)
            cells = re.findall(r"<td[^>]*>(.*?)</td>", block, re.DOTALL)
            if len(cells) < 5:
                continue

            # Cells: [date, type, ref, (annexe), description, file]
            # Some rows have a <!--comment--> between ref and annexe
            # Remove HTML comments from the block
            clean_block = re.sub(r"<!--.*?-->", "", block, flags=re.DOTALL)
            cells = re.findall(r"<td[^>]*>(.*?)</td>", clean_block, re.DOTALL)
            if len(cells) < 5:
                continue

            date_raw = re.sub(r"<[^>]+>", "", cells[0]).strip()
            doc_type = html_module.unescape(re.sub(r"<[^>]+>", "", cells[1]).strip())
            reference = html_module.unescape(re.sub(r"<[^>]+>", "", cells[2]).strip())

            # Skip placeholder rows
            if date_raw == "..." or not reference:
                continue

            annexe_marker = re.sub(r"<[^>]+>", "", cells[3]).strip()
            description = html_module.unescape(
                re.sub(r"<[^>]+>", "", cells[4]).strip()
            )
            file_cell = cells[5] if len(cells) > 5 else ""

            # Extract PDF link
            pdf_match = re.search(r"href='([^']+)'", file_cell)
            pdf_url = ""
            if pdf_match:
                pdf_path = pdf_match.group(1)
                if pdf_path.startswith("./"):
                    pdf_path = pdf_path[2:]
                pdf_url = f"{ACTES_BASE}{pdf_path}"

            is_annexe = "X" in annexe_marker or "annexe" in annexe_marker.lower()

            # Normalize date: convert French month names to DD/MM/YYYY if possible
            date_normalized = self._normalize_date(date_raw)

            rows.append({
                "date_raw": date_normalized,
                "doc_type": doc_type,
                "reference": reference,
                "is_annexe": is_annexe,
                "description": description,
                "pdf_url": pdf_url,
            })

        return rows

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Convert French month+year to a usable date string."""
        MONTHS_FR = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
        }
        # Already DD/MM/YYYY
        if re.match(r"\d{2}/\d{2}/\d{4}", date_str):
            return date_str
        # "month YYYY" format (Arrêtés)
        parts = date_str.lower().split()
        if len(parts) == 2 and parts[0] in MONTHS_FR:
            return f"01/{MONTHS_FR[parts[0]]}/{parts[1]}"
        return date_str

    def _extract_act_pdf(self, pdf_url: str, doc_id: str) -> str:
        """Download an act PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                return ""
            if not resp.content or resp.content[:4] != b"%PDF":
                return ""
        except Exception as e:
            logger.warning(f"PDF download error {pdf_url}: {e}")
            return ""

        text = extract_pdf_markdown(
            source="BL/Codes",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        )
        return text or ""

    def _make_doc_id(self, reference: str, is_annexe: bool, pdf_url: str = "") -> str:
        """Create a clean document ID from the reference."""
        clean = re.sub(r"[^\w\d-]", "_", reference.strip()).strip("_")
        clean = re.sub(r"_+", "_", clean)
        if is_annexe:
            # Extract annexe number from PDF filename to distinguish multiple annexes
            suffix = "annexe"
            if pdf_url:
                fname = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")
                # Look for _annexe_1, _annexe_2, etc.
                m = re.search(r"annexe[_\s]*(\d+)", fname, re.IGNORECASE)
                if m:
                    suffix = f"annexe_{m.group(1)}"
            clean += f"_{suffix}"
        return clean

    # ── Crawl methods ─────────────────────────────────────────────────

    def _crawl_codes(self) -> Generator[Dict[str, Any], None, None]:
        """Yield consolidated code documents."""
        for code in CONSOLIDATED_CODES:
            result = self._fetch_code_pdf(code)
            if result:
                yield result

    def _crawl_actes(
        self, act_type: Dict[str, str], max_pages: int = 9999
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield acts from the actes portal for a given type."""
        label = act_type["label"]
        filter_val = act_type["filter"]

        logger.info(f"Crawling actes portal: {label}")
        if not self._init_actes_session(filter_val):
            logger.error(f"Failed to init session for {label}")
            return

        # Get first page to check total
        first_html = self._fetch_actes_page(1)
        if not first_html:
            logger.error(f"Empty first page for {label}")
            return

        total = self._get_total_results(first_html)
        pages = min((total // 30) + 2, max_pages)
        logger.info(f"{label}: {total} results, ~{pages} pages")

        page = 1
        consecutive_empty = 0
        yielded = 0

        while page <= pages:
            html = first_html if page == 1 else self._fetch_actes_page(page)
            if not html:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            rows = self._parse_actes_rows(html)
            if not rows:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            consecutive_empty = 0

            for row in rows:
                pdf_url = row.get("pdf_url", "")
                if not pdf_url:
                    continue

                doc_id = self._make_doc_id(row["reference"], row["is_annexe"], pdf_url)
                text = self._extract_act_pdf(pdf_url, doc_id)

                if len(text) < MIN_TEXT_CHARS:
                    continue

                row["doc_id"] = doc_id
                row["full_text"] = text
                yielded += 1
                yield row

            page += 1
            if page % 20 == 0:
                logger.info(f"{label}: page {page}/{pages}, {yielded} yielded")

        logger.info(f"{label}: done, {yielded} documents with text")

    # ── BaseScraper interface ─────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents: consolidated codes first, then actes."""
        logger.info("Starting BL/Codes full crawl...")
        yield from self._crawl_codes()
        for act_type in ACT_TYPES:
            yield from self._crawl_actes(act_type)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent documents (first few pages of each actes type)."""
        logger.info(f"Fetching updates since {since.isoformat()}...")
        since_date = since.date()

        # Always re-fetch consolidated codes
        yield from self._crawl_codes()

        # Fetch recent actes (max 5 pages each)
        for act_type in ACT_TYPES:
            if not self._init_actes_session(act_type["filter"]):
                continue
            page = 1
            while page <= 5:
                html = self._fetch_actes_page(page)
                if not html:
                    break
                rows = self._parse_actes_rows(html)
                if not rows:
                    break

                found_old = False
                for row in rows:
                    date_raw = row.get("date_raw", "")
                    if date_raw:
                        try:
                            d = datetime.strptime(date_raw, "%d/%m/%Y").date()
                            if d < since_date:
                                found_old = True
                                continue
                        except ValueError:
                            pass

                    pdf_url = row.get("pdf_url", "")
                    if not pdf_url:
                        continue

                    doc_id = self._make_doc_id(row["reference"], row["is_annexe"], pdf_url)
                    text = self._extract_act_pdf(pdf_url, doc_id)
                    if len(text) >= MIN_TEXT_CHARS:
                        row["doc_id"] = doc_id
                        row["full_text"] = text
                        yield row

                if found_old:
                    break
                page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        doc_id = raw.get("doc_id", "")

        # Parse date
        date_raw = raw.get("date_raw", "")
        date_iso = ""
        if date_raw:
            try:
                dt = datetime.strptime(date_raw, "%d/%m/%Y")
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = date_raw

        # Build title
        description = raw.get("description", "")
        title_from_code = raw.get("title", "")
        reference = raw.get("reference", "")
        doc_type = raw.get("doc_type", "")

        if title_from_code:
            title = title_from_code
        elif description and reference:
            title = f"{reference} : {description}"
        elif description:
            title = description
        elif reference:
            title = reference
        else:
            title = doc_id

        url = raw.get("url", raw.get("pdf_url", ""))

        return {
            "_id": doc_id,
            "_source": "BL/Codes",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date_iso,
            "url": url,
            "doc_type": doc_type,
            "reference": reference,
            "is_annexe": raw.get("is_annexe", False),
            "language": "fr",
        }

    def test_api(self):
        """Quick connectivity and content test."""
        print("Testing BL/Codes sources...\n")

        # Test consolidated codes
        print("=== Consolidated Codes ===")
        for code in CONSOLIDATED_CODES[:2]:
            print(f"\n  {code['title']}:")
            try:
                self.rate_limiter.wait()
                resp = self.session.get(code["url"], timeout=30)
                print(f"    HTTP {resp.status_code}, {len(resp.content)} bytes")
                if resp.content[:4] == b"%PDF":
                    text = extract_pdf_markdown(
                        "BL/Codes", code["id"],
                        pdf_bytes=resp.content, table="legislation"
                    )
                    print(f"    Text: {len(text or '')} chars")
                    if text:
                        print(f"    Preview: {text[:150]}...")
            except Exception as e:
                print(f"    ERROR: {e}")

        # Test actes portal
        print("\n=== Actes Portal ===")
        for act_type in ACT_TYPES:
            label = act_type["label"]
            print(f"\n  {label}:")
            if not self._init_actes_session(act_type["filter"]):
                print("    ERROR: Session init failed")
                continue

            html = self._fetch_actes_page(1)
            total = self._get_total_results(html)
            rows = self._parse_actes_rows(html)
            print(f"    Total: {total}, page 1 rows: {len(rows)}")

            if rows:
                r = rows[0]
                print(f"    First: {r['date_raw']} | {r['reference']} | {r['description'][:60]}")
                if r.get("pdf_url"):
                    doc_id = self._make_doc_id(r["reference"], r["is_annexe"], r.get("pdf_url", ""))
                    text = self._extract_act_pdf(r["pdf_url"], doc_id)
                    print(f"    PDF text: {len(text)} chars")

        print("\nTest complete!")


def main():
    scraper = BLCodesScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
