#!/usr/bin/env python3
"""
KN/LawCommission -- St Kitts & Nevis Law Commission

Fetches legislation from lawcommission.gov.kn:
  - Revised Acts of St Kitts and Nevis
  - Revised Ordinances of Nevis
  - Annual Laws (Acts and SROs, 2018-2025)
  - Repealed Acts and Ordinances

PDFs served via EE Simple File List WordPress plugin.
Full text extracted from PDF documents.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote, quote, urljoin
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KN.LawCommission")

BASE_URL = "https://lawcommission.gov.kn"
FILE_LIST_PAGE = "/laws/"
DOCS_BASE = "/wp-content/documents/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Top-level folders to crawl (skip TOC indexes — they're just table of contents)
ROOT_FOLDERS = [
    "Annual-Laws",
    "Revised-Acts-of-St-Kitts-and-Nevis",
    "Revised-Ordinances-of-Nevis",
    "Repealed-Acts-and-Ordinances",
]


def classify_pdf(folder_path: str, filename: str) -> dict:
    """Extract metadata from folder path and filename."""
    meta = {"category": "unknown", "year": None, "act_number": None, "chapter": None}

    if "Annual-Laws" in folder_path:
        # Annual-Laws/2024/ACTs/Act-14-of-2024-Copyright-Act-2024.pdf
        year_match = re.search(r"Annual-Laws/(\d{4})", folder_path)
        if year_match:
            meta["year"] = year_match.group(1)
        else:
            # Try extracting year from filename: "Act-14-of-2024-..."
            fname_year = re.search(r"of[- _](\d{4})", filename)
            if fname_year:
                meta["year"] = fname_year.group(1)

        if "/SRO" in folder_path.upper() or filename.upper().startswith("SRO"):
            meta["category"] = "annual_sro"
            num_match = re.match(r"SRO[- _]?(\d+)", filename, re.IGNORECASE)
            if num_match:
                meta["act_number"] = f"SRO {num_match.group(1)}"
        else:
            meta["category"] = "annual_act"
            num_match = re.match(r"Act[- _]?(\d+)", filename, re.IGNORECASE)
            if num_match:
                meta["act_number"] = f"Act {num_match.group(1)}"

    elif "Revised-Acts" in folder_path:
        meta["category"] = "revised_act"
        # Ch-01_01-West-Indies-Act.pdf
        ch_match = re.match(r"Ch[- _](\d+[_ ]\d+)", filename)
        if ch_match:
            meta["chapter"] = ch_match.group(1).replace("_", ".")
        # Try to get revision year from folder
        year_match = re.search(r"(\d{4})", folder_path.split("Revised-Acts")[-1])
        if year_match:
            meta["year"] = year_match.group(1)

    elif "Revised-Ordinances" in folder_path:
        meta["category"] = "revised_ordinance"
        ch_match = re.match(r"Ch[- _](\d+[_ ]\d+)", filename)
        if ch_match:
            meta["chapter"] = ch_match.group(1).replace("_", ".")
        year_match = re.search(r"(\d{4})", folder_path.split("Revised-Ordinances")[-1])
        if year_match:
            meta["year"] = year_match.group(1)

    elif "Repealed" in folder_path:
        meta["category"] = "repealed"

    return meta


def make_title(filename: str) -> str:
    """Derive a readable title from PDF filename."""
    name = filename.replace(".pdf", "").replace(".PDF", "")
    # Replace hyphens and underscores with spaces
    name = re.sub(r"[-_]+", " ", name)
    # Clean up extra spaces
    name = re.sub(r"\s+", " ", name).strip()
    return name


def make_law_id(folder_path: str, filename: str) -> str:
    """Create a stable ID from folder path and filename."""
    # Use folder + filename to create unique ID
    path_slug = re.sub(r"[^a-z0-9]+", "-", folder_path.lower()).strip("-")
    name_slug = re.sub(r"[^a-z0-9]+", "-", filename.lower().replace(".pdf", "")).strip("-")
    # Keep it reasonably short
    return f"KN-{path_slug[-40:]}-{name_slug[:60]}"


class LawCommissionScraper(BaseScraper):
    """Scraper for St Kitts & Nevis Law Commission legislation."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _list_folder(self, folder_path: str) -> list[dict]:
        """List PDFs and subfolders in a given folder via the file list plugin."""
        url = BASE_URL + FILE_LIST_PAGE
        params = {
            "eeFolder": folder_path,
            "eeFront": "1",
            "eeListID": "1",
            "ee": "1",
        }

        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to list folder {folder_path}: {e}")
            return []

        html = resp.text
        items = []
        seen_urls = set()

        # Find PDF file links: href="/wp-content/documents/path/file.pdf"
        pdf_pattern = re.compile(
            r'href="([^"]*\.pdf)"',
            re.IGNORECASE,
        )
        for match in pdf_pattern.finditer(html):
            href = match.group(1)
            if DOCS_BASE in href or href.endswith(".pdf"):
                full_url = urljoin(BASE_URL, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)
                filename = unquote(href.split("/")[-1])
                items.append({
                    "type": "pdf",
                    "url": full_url,
                    "filename": filename,
                    "folder": folder_path,
                })

        # Find subfolder links: ?eeFolder=path/subfolder
        folder_pattern = re.compile(
            r'eeFolder=([^&"\']+)',
        )
        seen_folders = set()
        for match in folder_pattern.finditer(html):
            subfolder = unquote(match.group(1))
            if subfolder not in seen_folders and subfolder != folder_path:
                # Must be a child of current folder
                if subfolder.startswith(folder_path + "/") or folder_path == "":
                    seen_folders.add(subfolder)
                    items.append({"type": "folder", "path": subfolder})

        return items

    def _crawl_folder(self, folder_path: str, depth: int = 0) -> list[dict]:
        """Recursively crawl folder tree and return all PDF entries."""
        if depth > 5:  # Safety limit
            return []

        time.sleep(0.5)
        items = self._list_folder(folder_path)
        pdfs = [i for i in items if i["type"] == "pdf"]
        subfolders = [i for i in items if i["type"] == "folder"]

        logger.info(f"Folder {folder_path}: {len(pdfs)} PDFs, {len(subfolders)} subfolders")

        for sf in subfolders:
            pdfs.extend(self._crawl_folder(sf["path"], depth + 1))

        return pdfs

    def _enumerate_all_pdfs(self) -> list[dict]:
        """Enumerate all PDFs across all root folders."""
        all_pdfs = []
        for root in ROOT_FOLDERS:
            logger.info(f"Crawling root folder: {root}")
            pdfs = self._crawl_folder(root)
            all_pdfs.extend(pdfs)
            logger.info(f"  → {len(pdfs)} PDFs in {root}")

        logger.info(f"Total PDFs enumerated: {len(all_pdfs)}")
        return all_pdfs

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation with full text from PDFs."""
        all_pdfs = self._enumerate_all_pdfs()

        for i, pdf_info in enumerate(all_pdfs):
            time.sleep(1)

            filename = pdf_info["filename"]
            folder = pdf_info["folder"]
            pdf_url = pdf_info["url"]
            law_id = make_law_id(folder, filename)

            text = extract_pdf_markdown(
                source="KN/LawCommission",
                source_id=law_id,
                pdf_url=pdf_url,
                table="legislation",
            )

            if not text or len(text) < 50:
                logger.warning(f"No text from PDF: {filename}")
                continue

            meta = classify_pdf(folder, filename)
            title = make_title(filename)

            record = {
                "law_id": law_id,
                "title": title,
                "text": text,
                "date": f"{meta['year']}-01-01" if meta.get("year") else None,
                "url": pdf_url,
                "category": meta["category"],
                "chapter": meta.get("chapter"),
                "act_number": meta.get("act_number"),
                "year": meta.get("year"),
                "pdf_filename": filename,
                "language": "eng",
            }

            yield record
            logger.info(f"Processed {i + 1}/{len(all_pdfs)}: {title[:60]}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch all (small enough corpus for full refresh)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": raw["law_id"],
            "_source": "KN/LawCommission",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "law_id": raw["law_id"],
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category"),
            "chapter": raw.get("chapter"),
            "act_number": raw.get("act_number"),
            "year": raw.get("year"),
            "pdf_filename": raw.get("pdf_filename"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to lawcommission.gov.kn."""
        try:
            resp = self.session.get(
                BASE_URL + FILE_LIST_PAGE,
                params={"eeFolder": "Annual-Laws", "eeFront": "1", "eeListID": "1", "ee": "1"},
                timeout=30,
            )
            resp.raise_for_status()
            if ".pdf" in resp.text.lower():
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: no PDF links found")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = LawCommissionScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
