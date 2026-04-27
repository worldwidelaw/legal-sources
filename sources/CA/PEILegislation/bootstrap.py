#!/usr/bin/env python3
"""
CA/PEILegislation -- Prince Edward Island Acts & Regulations

Fetches consolidated legislation PDFs from princeedwardisland.ca.
The main site is behind Radware WAF, but static PDF files under
/sites/default/files/legislation/ are served without challenge.

The listing of all PDFs and titles is embedded below (extracted from
a Wayback Machine snapshot of the full legislation listing page).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.PEILegislation")

BASE_URL = "https://www.princeedwardisland.ca"
PDF_BASE = "/sites/default/files/legislation/"

# Wayback Machine URL for the full listing page (fallback for re-scraping)
WB_LISTING_URL = (
    "https://web.archive.org/web/20250525132441/"
    "https://www.princeedwardisland.ca/en/legislation/all/all/all"
)


def _classify_doc(title: str, filename: str) -> str:
    """Classify as act or regulation based on title and filename."""
    lower = title.lower()
    fn_lower = filename.lower()
    if any(kw in lower for kw in ("regulation", "rules", "order", "by-law", "bylaw")):
        return "regulation"
    # Filenames with parent act code + suffix often indicate regulations
    if re.match(r'^[a-z]\d.*g[-_]', fn_lower):
        return "regulation"
    return "act"


def _doc_id_from_url(pdf_url: str) -> str:
    """Generate a stable document ID from the PDF URL."""
    # Use the filename portion as the basis
    filename = pdf_url.rstrip("/").split("/")[-1]
    return hashlib.md5(filename.encode()).hexdigest()[:12]


class PEILegislationScraper(BaseScraper):
    """Scraper for CA/PEILegislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/pdf,*/*",
            },
            timeout=120,
        )

    def _load_pdf_list(self) -> List[Dict[str, Any]]:
        """Load the list of PDF URLs and titles.

        First tries the embedded list file; if not present, falls back
        to fetching the Wayback Machine snapshot and parsing it.
        """
        list_file = Path(__file__).parent / "pdf_list.tsv"
        if list_file.exists():
            return self._parse_list_file(list_file)

        # Fallback: fetch from Wayback Machine
        logger.info("pdf_list.tsv not found, fetching from Wayback Machine...")
        return self._fetch_listing_from_wayback()

    def _parse_list_file(self, list_file: Path) -> List[Dict[str, Any]]:
        """Parse the TSV file of PDF URLs and titles."""
        docs = []
        seen = set()
        for line in list_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            pdf_url, title = parts[0].strip(), parts[1].strip()
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            filename = unquote(pdf_url.rstrip("/").split("/")[-1])
            doc_id = _doc_id_from_url(pdf_url)
            doc_type = _classify_doc(title, filename)

            docs.append({
                "doc_id": doc_id,
                "title": title,
                "doc_type": doc_type,
                "pdf_url": pdf_url,
                "filename": filename,
            })
        logger.info(f"Loaded {len(docs)} documents from pdf_list.tsv")
        return docs

    def _fetch_listing_from_wayback(self) -> List[Dict[str, Any]]:
        """Fetch the full legislation listing from Wayback Machine."""
        wb_client = HttpClient(
            base_url="https://web.archive.org",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,*/*",
            },
            timeout=120,
        )
        resp = wb_client.get(
            "/web/20250525132441/"
            "https://www.princeedwardisland.ca/en/legislation/all/all/all"
        )
        resp.raise_for_status()
        html = resp.text

        # Extract PDF links and their preceding title text
        docs = []
        seen = set()
        # Pattern: links to PDF files under /sites/default/files/legislation/
        for match in re.finditer(
            r'<a[^>]*href="([^"]*?/sites/default/files/legislation/[^"]+\.pdf)"[^>]*>'
            r'(.*?)</a>',
            html,
            re.DOTALL | re.IGNORECASE,
        ):
            href = match.group(1)
            link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

            # Remove Wayback Machine prefix if present
            if "/web/" in href:
                idx = href.find("/sites/default/files/legislation/")
                if idx >= 0:
                    href = href[idx:]

            pdf_url = BASE_URL + href
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            filename = unquote(href.rstrip("/").split("/")[-1])
            title = link_text or filename.replace(".pdf", "").replace("_", " ").replace("-", " ").title()
            doc_id = _doc_id_from_url(pdf_url)
            doc_type = _classify_doc(title, filename)

            docs.append({
                "doc_id": doc_id,
                "title": title,
                "doc_type": doc_type,
                "pdf_url": pdf_url,
                "filename": filename,
            })

        logger.info(f"Fetched {len(docs)} documents from Wayback Machine")

        # Save for future use
        list_file = Path(__file__).parent / "pdf_list.tsv"
        with open(list_file, "w", encoding="utf-8") as f:
            f.write("# PDF URL\tTitle\n")
            for doc in docs:
                f.write(f"{doc['pdf_url']}\t{doc['title']}\n")
        logger.info(f"Saved pdf_list.tsv ({len(docs)} entries)")

        return docs

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"CA/PEILegislation/{raw['doc_id']}",
            "_source": "CA/PEILegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_prefetched_text", ""),
            "date": "",
            "url": raw.get("pdf_url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._load_pdf_list()
        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break

            # Download the PDF
            try:
                self.rate_limiter.wait()
                resp = self.client.get(doc["pdf_url"].replace(BASE_URL, ""))
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning(f"  Failed to download PDF for {doc['title'][:60]}: {e}")
                continue

            if len(pdf_bytes) < 100:
                logger.warning(f"  Skipping {doc['title'][:60]} - tiny PDF ({len(pdf_bytes)} bytes)")
                continue

            # Extract text
            text = extract_pdf_markdown(
                source="CA/PEILegislation",
                source_id=doc["doc_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text) < 50:
                logger.warning(f"  Skipping {doc['title'][:60]} - no/short text ({len(text)} chars)")
                continue

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info(f"  [{count}] {doc['title'][:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = PEILegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing PEI legislation PDF access...")
        docs = scraper._load_pdf_list()
        if docs:
            print(f"Loaded {len(docs)} legislation documents from listing.")
            acts = sum(1 for d in docs if d["doc_type"] == "act")
            regs = sum(1 for d in docs if d["doc_type"] == "regulation")
            print(f"  Acts: {acts}, Regulations: {regs}")
            # Test downloading one PDF
            test_doc = docs[0]
            print(f"  Testing download: {test_doc['title'][:60]}...")
            try:
                resp = scraper.client.get(
                    test_doc["pdf_url"].replace(BASE_URL, "")
                )
                resp.raise_for_status()
                print(f"  Download OK: {len(resp.content)} bytes")
            except Exception as e:
                print(f"  Download FAILED: {e}")
                sys.exit(1)
        else:
            print("FAILED - no documents found")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
