#!/usr/bin/env python3
"""
TR/TCMB -- Turkish Central Bank Regulations & Circulars

Fetches regulatory documents (teblig, yonetmelik, talimat, genelge) from TCMB
across 9 subject-area sections. All documents are PDFs.

Strategy:
  - For each section page, parse HTML for PDF links
  - Extract UUID from each link path as unique document ID
  - Download PDF and extract text via common/pdf_extract
  - Skip .zip and .docx files (not extractable)

URL patterns:
  - Section page: https://www.tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/{section}/
  - PDF download: https://www.tcmb.gov.tr/wps/wcm/connect/{uuid}/{filename}.pdf?MOD=AJPERES
  - No auth required

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List
from urllib.parse import unquote, urljoin
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TR/TCMB")

BASE_URL = "https://www.tcmb.gov.tr"

# Regulatory sections on the TCMB website
SECTIONS = [
    {
        "key": "bankacilik",
        "name": "Bankacılık",
        "name_en": "Banking",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Bankacilik/",
    },
    {
        "key": "odeme_sistemleri",
        "name": "Ödeme Sistemleri",
        "name_en": "Payment Systems",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Odeme+Sistemleri/",
    },
    {
        "key": "piyasalar",
        "name": "Piyasalar",
        "name_en": "Markets",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Piyasalar/",
    },
    {
        "key": "dis_ticaret",
        "name": "Dış Ticaret",
        "name_en": "Foreign Trade",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Dis+Ticaret/",
    },
    {
        "key": "emisyon",
        "name": "Emisyon",
        "name_en": "Issuance",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Emisyon/",
    },
    {
        "key": "kambiyo",
        "name": "Kambiyo",
        "name_en": "Foreign Exchange",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Kambiyo/",
    },
    {
        "key": "isci_dovizleri",
        "name": "İşçi Dövizleri",
        "name_en": "Worker Remittances",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Isci+Dovizleri/",
    },
    {
        "key": "istatistik",
        "name": "İstatistik",
        "name_en": "Statistics",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Istatistik/",
    },
    {
        "key": "operasyon",
        "name": "Operasyonel İşlemler",
        "name_en": "Operational",
        "path": "/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Banka+Hakkinda/Mevzuat/Operasyon+Mevzuat/",
    },
]


def _extract_pdf_links(html: str) -> List[Dict]:
    """Parse an HTML page for PDF links, returning unique documents."""
    # Pattern: <a href="...uuid...filename.pdf?MOD=AJPERES...">title</a>
    link_pattern = re.compile(
        r'<a[^>]*href="([^"]*\.pdf[^"]*?)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )

    docs = {}  # uuid -> {url, title, filename}
    for match in link_pattern.finditer(html):
        href = match.group(1).replace("&amp;", "&")
        title_raw = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        title = unescape(title_raw)

        # Extract UUID from path: /wps/wcm/connect/{uuid}/filename.pdf
        uuid_match = re.search(r"/wps/wcm/connect/([0-9a-f-]{36})/", href)
        if not uuid_match:
            continue

        uuid = uuid_match.group(1)

        # Extract filename
        fname_match = re.search(r"/([^/]+\.pdf)", href, re.IGNORECASE)
        filename = unquote(fname_match.group(1)) if fname_match else ""

        # Ensure MOD=AJPERES is in the URL for direct download
        if "MOD=AJPERES" not in href:
            if "?" in href:
                href += "&MOD=AJPERES"
            else:
                href += "?MOD=AJPERES"

        # Prefer the link with a title (skip icon-only links)
        if uuid not in docs or (title and not docs[uuid].get("title")):
            docs[uuid] = {
                "uuid": uuid,
                "url": href if href.startswith("http") else BASE_URL + href,
                "title": title,
                "filename": filename,
            }

    return list(docs.values())


def _parse_date_from_title(title: str) -> Optional[str]:
    """Try to extract a date from a document title."""
    # Pattern: DD.MM.YYYY or DD/MM/YYYY
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", title)
    if m:
        try:
            dt = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Pattern: month name YYYY (Turkish months)
    months_tr = {
        "ocak": 1, "şubat": 2, "mart": 3, "nisan": 4,
        "mayıs": 5, "haziran": 6, "temmuz": 7, "ağustos": 8,
        "eylül": 9, "ekim": 10, "kasım": 11, "aralık": 12,
    }
    for month_name, month_num in months_tr.items():
        m = re.search(rf"(\d{{1,2}})\s+{month_name}\s+(\d{{4}})", title, re.IGNORECASE)
        if m:
            try:
                dt = datetime(int(m.group(2)), month_num, int(m.group(1)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        # Also try: month YYYY without day
        m = re.search(rf"{month_name}\s*(\d{{4}})", title, re.IGNORECASE)
        if m:
            try:
                return f"{m.group(1)}-{month_num:02d}-01"
            except ValueError:
                pass

    # Pattern: YYYY from title like "2020/16 Sayılı Tebliğ"
    m = re.search(r"(\d{4})/\d+\s+sayılı", title, re.IGNORECASE)
    if m:
        year = int(m.group(1))
        if 1990 <= year <= 2030:
            return f"{year}-01-01"

    return None


class TCMBScraper(BaseScraper):
    """
    Scraper for: Turkish Central Bank (TCMB) Regulations
    Country: TR
    URL: https://www.tcmb.gov.tr

    Data types: doctrine
    Auth: none

    Crawls regulation section pages, downloads PDFs, extracts text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
            },
        )

    def _fetch_section_docs(self, section: dict) -> List[Dict]:
        """Fetch all PDF document links from a section page."""
        self.rate_limiter.wait()
        resp = self.client.get(section["path"])

        if resp.status_code != 200:
            logger.warning(f"Section {section['name']} returned {resp.status_code}")
            return []

        docs = _extract_pdf_links(resp.text)
        for doc in docs:
            doc["section_key"] = section["key"]
            doc["section_name"] = section["name"]
            doc["section_name_en"] = section["name_en"]

        logger.info(f"Section {section['name']}: {len(docs)} PDF documents")
        return docs

    def _fetch_doc_text(self, doc: dict) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(doc["url"])

            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {doc['filename']}")
                return None

            ct = resp.headers.get("Content-Type", "")
            if "pdf" not in ct and len(resp.content) < 1000:
                logger.warning(f"Not a PDF ({ct}): {doc['filename']}")
                return None

            text = extract_pdf_markdown(
                "TR/TCMB",
                doc["uuid"],
                pdf_bytes=resp.content,
                table="doctrine",
                force=True,
            )
            return text

        except Exception as e:
            logger.warning(f"Failed to extract {doc['filename']}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents from all sections."""
        seen_uuids = set()

        for section in SECTIONS:
            try:
                docs = self._fetch_section_docs(section)
            except Exception as e:
                logger.error(f"Failed section {section['name']}: {e}")
                continue

            for doc in docs:
                if doc["uuid"] in seen_uuids:
                    continue
                seen_uuids.add(doc["uuid"])

                text = self._fetch_doc_text(doc)
                if not text or len(text) < 50:
                    logger.warning(
                        f"Insufficient text for {doc['filename']} "
                        f"({len(text) if text else 0} chars), skipping"
                    )
                    continue

                doc["text"] = text
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (no date filtering available from index pages)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document record into the standard schema."""
        uuid = raw["uuid"]
        title = raw.get("title", "")
        filename = raw.get("filename", "")
        text = raw.get("text", "")

        # Build title from filename if link text was empty
        if not title:
            title = unquote(filename).replace("+", " ").replace(".pdf", "").strip()

        # Try to extract date from title or filename
        date_iso = _parse_date_from_title(title)
        if not date_iso:
            date_iso = _parse_date_from_title(filename)

        section_name = raw.get("section_name", "")
        section_en = raw.get("section_name_en", "")

        return {
            "_id": f"TR/TCMB/{uuid}",
            "_source": "TR/TCMB",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": raw["url"],
            "section": section_name,
            "section_en": section_en,
            "filename": filename,
        }


# ── CLI entry point ─────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="TR/TCMB scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TCMBScraper()

    if args.command == "test":
        # Quick connectivity test
        print("Testing TCMB connection...")
        docs = scraper._fetch_section_docs(SECTIONS[0])
        print(f"Banking section: {len(docs)} documents found")
        if docs:
            print(f"First doc: {docs[0]['title'][:80]}")
            text = scraper._fetch_doc_text(docs[0])
            if text:
                print(f"Text extracted: {len(text)} chars")
                print(text[:200])
            else:
                print("Text extraction failed")
        sys.exit(0)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        seen_uuids = set()

        if args.sample:
            # In sample mode, fetch 2 docs from each section (up to ~18 total)
            for section in SECTIONS:
                section_count = 0
                try:
                    docs = scraper._fetch_section_docs(section)
                except Exception as e:
                    logger.error(f"Failed section {section['name']}: {e}")
                    continue

                for doc in docs:
                    if doc["uuid"] in seen_uuids:
                        continue
                    seen_uuids.add(doc["uuid"])

                    if section_count >= 2:
                        break

                    text = scraper._fetch_doc_text(doc)
                    if not text or len(text) < 50:
                        continue

                    doc["text"] = text
                    normalized = scraper.normalize(doc)

                    safe_name = normalized["_id"].replace("/", "_")
                    out_path = sample_dir / f"{safe_name}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)

                    count += 1
                    section_count += 1
                    text_len = len(normalized.get("text", ""))
                    logger.info(
                        f"[{count}] {section['name_en']}: {normalized['title'][:60]} — "
                        f"{text_len} chars"
                    )

            logger.info(f"Sample bootstrap complete: {count} records")
        else:
            for raw in scraper.fetch_all():
                normalized = scraper.normalize(raw)
                if args.full:
                    safe_name = normalized["_id"].replace("/", "_")
                    out_path = sample_dir / f"{safe_name}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 50 == 0:
                    logger.info(f"Processed {count} records...")

            logger.info(f"Bootstrap complete: {count} records")

    elif args.command == "update":
        since = datetime.now(timezone.utc).replace(day=1)
        count = 0
        for raw in scraper.fetch_updates(since):
            normalized = scraper.normalize(raw)
            count += 1
        logger.info(f"Update complete: {count} records")
