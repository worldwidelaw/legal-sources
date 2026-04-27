#!/usr/bin/env python3
"""
INTL/STL -- Special Tribunal for Lebanon

Fetches public decisions, orders, judgments, and filings from the STL
court records, archived on the Wayback Machine after the tribunal
closed on 31 December 2023.

Strategy:
  - Use the Wayback Machine CDX API to enumerate all PDF filings from
    the STL Court Record System (stl-tsl.org/crs/assets/Uploads/)
  - Filter for English-language documents (avoid duplicates in FR/AR)
  - Download PDFs via Wayback Machine and extract text via common/pdf_extract
  - Parse metadata (date, filing ID, chamber, doc type) from filenames

Data Coverage:
  - ~200+ public English filings: judgments, decisions, orders, indictments
  - Cases: STL-11-01 (Ayyash/Hariri), STL-14-05, STL-14-06 (contempt),
    STL-17-07 (Merhi/Oneissi), STL-18-10 (Ayyash sentencing)
  - Covers 2009-2023 tribunal lifespan

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (no-op, tribunal closed)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.STL")

CDX_API = "https://web.archive.org/cdx/search/cdx"
WAYBACK_DL = "https://web.archive.org/web/{ts}id_/{url}"
CRS_UPLOADS = "www.stl-tsl.org/crs/assets/Uploads/*"
MAX_PDF_BYTES = 80 * 1024 * 1024  # 80 MB


class STLScraper(BaseScraper):
    """Scraper for Special Tribunal for Lebanon archived court records."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "*/*",
        })

    def _enumerate_pdfs(self) -> list[dict]:
        """Use Wayback CDX API to list all unique STL PDF filings."""
        params = {
            "url": CRS_UPLOADS,
            "output": "json",
            "fl": "original,timestamp,mimetype,statuscode",
            "collapse": "urlkey",
            "limit": "5000",
        }
        resp = self.session.get(CDX_API, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        pdfs = []
        seen_urls = set()
        for row in data[1:]:  # skip header
            url, ts, mime, status = row
            # Only successful PDF responses
            if status != "200":
                continue
            if "pdf" not in mime.lower() and ".pdf" not in url.lower():
                continue

            # Normalize URL (strip query params)
            clean_url = url.split("?")[0]
            if clean_url in seen_urls:
                continue
            seen_urls.add(clean_url)

            pdfs.append({"url": clean_url, "timestamp": ts})

        logger.info(f"CDX enumerated {len(pdfs)} unique PDFs")
        return pdfs

    def _parse_filename_metadata(self, url: str) -> dict:
        """Extract metadata from the STL PDF filename.

        Filename patterns:
          Newer: YYYYMMDD-FILING_ID-PUBLIC-CHAMBER-DocType-LANG-Web.pdf
          Older: YYYYMMDD_PUBLIC_FILING_ID_CHAMBER_DocType_Filed_LANG.pdf
          Early: YY_MM_DD_PTJ_Description_LANG.pdf
        """
        filename = url.split("/")[-1]
        # Remove .pdf extension
        base = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)

        meta = {"filename": filename, "language": "EN"}

        # Try to extract date from start of filename
        # Format: YYYYMMDD or YY_MM_DD
        date_match = re.match(r"^(\d{8})", base)
        if date_match:
            ds = date_match.group(1)
            try:
                dt = datetime.strptime(ds, "%Y%m%d")
                meta["date"] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        else:
            date_match2 = re.match(r"^(\d{2})_(\d{2})_(\d{2})", base)
            if date_match2:
                y, m, d = date_match2.groups()
                year = int(y) + 2000
                try:
                    dt = datetime(year, int(m), int(d))
                    meta["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Extract filing ID (F0001, F0051, etc.)
        filing_match = re.search(r"(F\d{3,4})", base)
        if filing_match:
            meta["filing_id"] = filing_match.group(1)

        # Extract case number
        case_match = re.search(r"(STL-\d{2}-\d{2})", base, re.IGNORECASE)
        if case_match:
            meta["case_number"] = case_match.group(1).upper()

        # Determine language from filename
        if "_FR" in base or "-FR" in base:
            meta["language"] = "FR"
        elif "_AR" in base or "-AR" in base:
            meta["language"] = "AR"
        elif "_EN" in base or "-EN" in base:
            meta["language"] = "EN"

        # Determine document type from filename keywords
        base_upper = base.upper()
        if "JUDGMENT" in base_upper or "JUGEMENT" in base_upper:
            meta["document_type"] = "Judgment"
        elif "SENTENCING" in base_upper:
            meta["document_type"] = "Sentencing Judgment"
        elif "INDICTMENT" in base_upper:
            meta["document_type"] = "Indictment"
        elif "DECISION" in base_upper or "DEC" in base_upper:
            meta["document_type"] = "Decision"
        elif "ORDER" in base_upper or "ORDONNANCE" in base_upper:
            meta["document_type"] = "Order"
        elif "ARREST" in base_upper or "WARRANT" in base_upper:
            meta["document_type"] = "Arrest Warrant"
        elif "SUMMARY" in base_upper:
            meta["document_type"] = "Summary"
        elif "SUBMISSION" in base_upper or "OBSERVATION" in base_upper:
            meta["document_type"] = "Submission"
        elif "CONFIRMATION" in base_upper:
            meta["document_type"] = "Confirmation Decision"
        elif "DESSAISISSEMENT" in base_upper:
            meta["document_type"] = "Order"
        else:
            meta["document_type"] = "Filing"

        # Build a human-readable title from the filename
        # Replace separators, clean up
        title_base = re.sub(r"[-_]+", " ", base)
        # Remove date prefix
        title_base = re.sub(r"^\d{8}\s*", "", title_base)
        title_base = re.sub(r"^\d{2}\s\d{2}\s\d{2}\s*", "", title_base)
        # Clean up common suffixes
        title_base = re.sub(r"\s*(Web|LW|Filed|Locked|Press version)\s*", " ", title_base, flags=re.IGNORECASE)
        title_base = title_base.strip()
        if title_base:
            meta["title"] = title_base

        return meta

    def _download_pdf_via_wayback(self, url: str, timestamp: str) -> Optional[bytes]:
        """Download a PDF from the Wayback Machine."""
        wayback_url = WAYBACK_DL.format(ts=timestamp, url=url)
        try:
            resp = self.session.get(wayback_url, timeout=120, stream=True)
            resp.raise_for_status()

            # Check content length
            content_length = resp.headers.get("Content-Length")
            if content_length and int(content_length) > MAX_PDF_BYTES:
                logger.warning(f"PDF too large ({content_length} bytes): {url}")
                return None

            content = resp.content
            if len(content) > MAX_PDF_BYTES:
                logger.warning(f"PDF too large ({len(content)} bytes): {url}")
                return None

            # Verify it looks like a PDF
            if not content[:5].startswith(b"%PDF"):
                logger.warning(f"Not a valid PDF (bad magic): {url}")
                return None

            return content
        except Exception as e:
            logger.warning(f"Failed to download PDF from Wayback: {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all public STL court records with full text."""
        pdf_list = self._enumerate_pdfs()

        # Filter to English documents to avoid duplicates
        en_pdfs = [p for p in pdf_list if self._is_english(p["url"])]
        logger.info(f"Filtered to {len(en_pdfs)} English PDFs (from {len(pdf_list)} total)")

        for i, pdf_info in enumerate(en_pdfs):
            url = pdf_info["url"]
            ts = pdf_info["timestamp"]
            meta = self._parse_filename_metadata(url)

            logger.info(f"[{i+1}/{len(en_pdfs)}] Processing: {meta.get('filename', url)}")

            time.sleep(1)  # Rate limit

            # Download and extract text
            pdf_bytes = self._download_pdf_via_wayback(url, ts)
            if not pdf_bytes:
                continue

            source_id = meta.get("filing_id", meta.get("filename", "").replace(".pdf", ""))
            text = extract_pdf_markdown(
                source="INTL/STL",
                source_id=source_id,
                pdf_bytes=pdf_bytes,
                table="case_law",
            )
            if not text or len(text.strip()) < 50:
                logger.warning(f"  Insufficient text for {meta.get('filename', '')} ({len(text or '')} chars)")
                continue

            meta["text"] = text
            meta["original_url"] = url
            meta["wayback_url"] = WAYBACK_DL.format(ts=ts, url=url)
            yield meta

    def _is_english(self, url: str) -> bool:
        """Check if a PDF URL is for an English document."""
        filename = url.split("/")[-1].upper()
        # Exclude purely French or Arabic documents
        if ("_FR." in filename or "-FR." in filename or
            "_FR-" in filename or "_FR_" in filename or
            "FILED_FR" in filename or "Filed_FR" in filename):
            # But include bilingual docs (FR_EN, FR-EN)
            if "_EN" in filename or "-EN" in filename:
                return True
            return False
        if ("_AR." in filename or "-AR." in filename or
            "_AR-" in filename or "_AR_" in filename):
            if "_EN" in filename or "-EN" in filename:
                return True
            return False
        # Default: include (many early docs don't have language markers)
        return True

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No-op: STL closed 31 Dec 2023, no new filings."""
        logger.info("STL closed on 31 December 2023. No updates possible.")
        return
        yield  # Make this a generator

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw STL record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        filename = raw.get("filename", "")
        filing_id = raw.get("filing_id", "")
        case_number = raw.get("case_number", "")
        title = raw.get("title", "") or filename
        date_str = raw.get("date")

        # Build a unique ID
        unique_part = filing_id or filename.replace(".pdf", "")
        _id = f"INTL_STL_{unique_part}".replace(" ", "_")

        return {
            "_id": _id,
            "_source": "INTL/STL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": date_str,
            "url": raw.get("wayback_url", raw.get("original_url", "")),
            "case_number": case_number,
            "filing_id": filing_id,
            "document_type": raw.get("document_type", "Filing"),
            "language": raw.get("language", "EN"),
            "court": "Special Tribunal for Lebanon (STL)",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="STL bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = STLScraper()

    if args.command == "test":
        print("Testing STL Wayback Machine access...")
        try:
            pdfs = scraper._enumerate_pdfs()
            en_pdfs = [p for p in pdfs if scraper._is_english(p["url"])]
            print(f"OK: {len(pdfs)} total PDFs, {len(en_pdfs)} English")
            if en_pdfs:
                meta = scraper._parse_filename_metadata(en_pdfs[0]["url"])
                print(f"  First: {meta.get('filename', '?')}")
                print(f"  Date: {meta.get('date', '?')}, Filing: {meta.get('filing_id', '?')}")
                print(f"  Type: {meta.get('document_type', '?')}")
                # Test PDF download
                pdf_bytes = scraper._download_pdf_via_wayback(
                    en_pdfs[0]["url"], en_pdfs[0]["timestamp"]
                )
                if pdf_bytes:
                    print(f"  PDF download: OK ({len(pdf_bytes)} bytes)")
                    text = extract_pdf_markdown(
                        source="INTL/STL",
                        source_id="test",
                        pdf_bytes=pdf_bytes,
                        table="case_law",
                    )
                    if text:
                        print(f"  Text extraction: OK ({len(text)} chars)")
                        print(f"  Preview: {text[:200]}...")
                    else:
                        print("  Text extraction: FAILED")
                else:
                    print("  PDF download: FAILED")
        except Exception as e:
            print(f"FAIL: {e}")
            import traceback
            traceback.print_exc()
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
