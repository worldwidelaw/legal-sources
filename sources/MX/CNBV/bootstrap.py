#!/usr/bin/env python3
"""
MX/CNBV -- Comisión Nacional Bancaria y de Valores - Normatividad

Fetches regulatory provisions, circulars, laws, and agreements from Mexico's
banking and securities commission (CNBV) with full text extracted from PDFs.

Strategy:
  - List all PDF files in CNBV's Normatividad library via SharePoint REST API.
  - Download each PDF and extract text via pdf_extract.
  - Normalize into standard schema with full text.

Data:
  - ~112 documents covering banking, securities, fintech, payments
  - Full text in Spanish, extracted from PDF
  - Consolidated/compiled regulatory texts

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental (not implemented)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.CNBV")

BASE_URL = "https://www.cnbv.gob.mx"
SP_API_URL = f"{BASE_URL}/_api/web/GetFolderByServerRelativeUrl('/Normatividad')/Files"
DELAY = 2.0


class CNBVScraper(BaseScraper):
    """Scraper for MX/CNBV regulatory documents."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
        })
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _list_files(self) -> List[Dict[str, Any]]:
        """List all PDF files in the CNBV Normatividad library via SharePoint API."""
        logger.info("Fetching file listing from SharePoint REST API...")
        r = self.session.get(
            SP_API_URL,
            headers={"Accept": "application/json;odata=verbose"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        files = []
        for item in data.get("d", {}).get("results", []):
            name = item.get("Name", "")
            if not name.lower().endswith(".pdf"):
                continue
            files.append({
                "name": name,
                "title": item.get("Title") or name.replace(".pdf", ""),
                "url": f"{BASE_URL}/Normatividad/{quote(name)}",
                "size": int(item.get("Length", 0)),
                "modified": item.get("TimeLastModified", ""),
                "created": item.get("TimeCreated", ""),
                "unique_id": item.get("UniqueId", ""),
            })

        logger.info(f"Found {len(files)} PDF files")
        return files

    def _make_doc_id(self, name: str) -> str:
        """Create a stable document ID from the filename."""
        slug = name.replace(".pdf", "")
        slug = re.sub(r'[^\w\s\-]', '', slug)
        slug = re.sub(r'\s+', '-', slug.strip())
        return slug[:120]

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CNBV regulatory documents with full text from PDFs."""
        files = self._list_files()

        for i, finfo in enumerate(files):
            try:
                logger.info(f"[{i+1}/{len(files)}] {finfo['name'][:80]} ({finfo['size']/1024:.0f} KB)")
                time.sleep(DELAY)

                # Download PDF
                try:
                    pdf_resp = self.session.get(finfo["url"], timeout=120)
                    pdf_resp.raise_for_status()
                except requests.RequestException as e:
                    logger.warning(f"  Failed to download: {e}")
                    continue

                pdf_bytes = pdf_resp.content
                if len(pdf_bytes) < 1000:
                    logger.warning(f"  PDF too small ({len(pdf_bytes)} bytes), skipping")
                    continue

                # Extract text
                doc_id = self._make_doc_id(finfo["name"])
                text = extract_pdf_markdown(
                    source="MX/CNBV",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                    force=True,
                )

                if not text or len(text) < 100:
                    logger.warning(f"  Insufficient text ({len(text) if text else 0} chars)")
                    continue

                # Parse modification date
                mod_date = None
                if finfo["modified"]:
                    try:
                        mod_date = finfo["modified"][:10]
                    except (IndexError, ValueError):
                        pass

                yield self.normalize({
                    "doc_id": doc_id,
                    "title": finfo["title"],
                    "text": text,
                    "date": mod_date,
                    "url": finfo["url"],
                    "size": finfo["size"],
                    "filename": finfo["name"],
                })

            except Exception as e:
                logger.error(f"  Error processing {finfo['name'][:60]}: {e}")
                continue

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Incremental update - re-fetch all (small corpus)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": f"MX/CNBV/{raw['doc_id']}",
            "_source": "MX/CNBV",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "filename": raw.get("filename", ""),
            "size_bytes": raw.get("size", 0),
        }


# ── CLI ──────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="MX/CNBV bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = CNBVScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        files = scraper._list_files()
        logger.info(f"Found {len(files)} PDF files")
        if files:
            logger.info(f"First file: {files[0]['name']}")
        logger.info("Test passed!")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else 999999

        for record in scraper.fetch_all():
            text_len = len(record.get("text", ""))
            logger.info(
                f"  => {record['_id'][:60]} | "
                f"text={text_len} chars | date={record.get('date', 'N/A')}"
            )

            safe_name = re.sub(r'[^\w\-]', '_', record["_id"].split("/")[-1])[:80]
            out_path = sample_dir / f"{safe_name}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            count += 1
            if count >= limit:
                break

        logger.info(f"Done. Saved {count} records to {sample_dir}")


if __name__ == "__main__":
    main()
