#!/usr/bin/env python3
"""
CL/UAF -- Unidad de Análisis Financiero (Financial Analysis Unit)

Fetches AML/CFT circulars, laws, and policy documents from Chile's UAF.

Strategy:
  - Scrape the circulares page for active circulars (PDF links)
  - Scrape the normativa-derogada pages (paginated) for repealed circulars
  - Scrape the nuestra-ley page for the enabling law (Ley 19.913)
  - Scrape the delitos-base page for the predicate offences catalog
  - Download each PDF and extract full text via common/pdf_extract
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.UAF")

BASE_URL = "https://www.uaf.cl"
CIRCULARES_URL = BASE_URL + "/es-cl/normativa/circulares-uaf"
DEROGADAS_URL = BASE_URL + "/es-cl/normativa/normativa-derogada-el-01062025"
LEY_URL = BASE_URL + "/es-cl/normativa/nuestra-ley"
DELITOS_URL = BASE_URL + "/es-cl/normativa/delitos-base-o-precedentes-de-lavado-de-activos"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"


def _get(url: str, timeout: int = 60) -> str:
    """GET a URL and return decoded text."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    raw = resp.read()
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET a URL and return raw bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=timeout)
    return resp.read()


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    return " ".join(text.split()).strip()


MONTH_MAP = {
    "ene": "01", "feb": "02", "mar": "03", "abr": "04",
    "may": "05", "jun": "06", "jul": "07", "ago": "08",
    "sep": "09", "oct": "10", "nov": "11", "dic": "12",
}


def _parse_date_spanish(date_str: str) -> Optional[str]:
    """Parse Spanish date like '19 Mar 2025' or '03 Sep 2019' to ISO."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Try DD Mon YYYY
    m = re.match(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", date_str, re.IGNORECASE)
    if m:
        day = m.group(1).zfill(2)
        month = MONTH_MAP.get(m.group(2).lower()[:3])
        year = m.group(3)
        if month:
            return f"{year}-{month}-{day}"
    # Try DD/MM/YYYY or DD-MM-YYYY
    m2 = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", date_str)
    if m2:
        return f"{m2.group(3)}-{m2.group(2).zfill(2)}-{m2.group(1).zfill(2)}"
    return None


def _extract_pdf_links(html: str) -> List[Dict[str, str]]:
    """Extract PDF document entries from an HTML page.

    Returns list of dicts with keys: title, pdf_url, date_str.
    """
    results = []
    # Pattern: links to /media/documentos/*.pdf
    # We look for anchors or download buttons containing PDF paths
    # The typical structure is a card/row with title text and a download link

    # Find all PDF links
    pdf_links = re.findall(
        r'href="(/media/documentos/[^"]+\.pdf)"',
        html, re.IGNORECASE
    )

    # For each PDF, try to find surrounding context for title and date
    for pdf_path in pdf_links:
        # Escape the path for regex
        escaped = re.escape(pdf_path)

        # Look for the block containing this PDF link
        # Try to find a containing card/section (look back up to 1000 chars)
        block_match = re.search(
            r'(.{0,1500})' + escaped,
            html, re.DOTALL
        )

        title = ""
        date_str = ""

        if block_match:
            context = block_match.group(1)

            # Extract title: look for heading tags or strong text near the link
            title_patterns = [
                r'<h[2-5][^>]*>([^<]+)</h[2-5]>',
                r'<strong>([^<]+)</strong>',
                r'<b>([^<]+)</b>',
                r'class="[^"]*titulo[^"]*"[^>]*>([^<]+)<',
                r'class="[^"]*title[^"]*"[^>]*>([^<]+)<',
            ]
            for pat in title_patterns:
                matches = re.findall(pat, context, re.IGNORECASE)
                if matches:
                    # Take the last match (closest to the link)
                    title = _clean_html(matches[-1]).strip()
                    if title and len(title) > 5:
                        break

            # Extract date
            date_matches = re.findall(
                r'(\d{1,2}\s+\w{3}\s+\d{4})',
                context
            )
            if date_matches:
                date_str = date_matches[-1]

        if not title:
            # Derive title from filename
            fname = pdf_path.rsplit("/", 1)[-1].replace(".pdf", "")
            title = fname.replace("_", " ")

        results.append({
            "title": title,
            "pdf_url": pdf_path,
            "date_str": date_str,
        })

    return results


def _scrape_circulares_page(url: str) -> List[Dict[str, str]]:
    """Scrape a single page of circulares and return document entries."""
    html = _get(url)
    time.sleep(2)
    return _extract_pdf_links(html)


class UAFScraper(BaseScraper):
    SOURCE_ID = "CL/UAF"

    def _collect_all_documents(self) -> List[Dict[str, Any]]:
        """Collect metadata for all documents across all normativa sections."""
        docs = []
        seen_urls = set()

        def _add_docs(entries: List[Dict[str, str]], category: str, status: str):
            for entry in entries:
                pdf_url = entry["pdf_url"]
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                docs.append({
                    "title": entry["title"],
                    "pdf_url": pdf_url,
                    "date_str": entry.get("date_str", ""),
                    "category": category,
                    "status": status,
                })

        # 1. Active circulars
        logger.info("Fetching active circulars...")
        active = _scrape_circulares_page(CIRCULARES_URL)
        _add_docs(active, "circular", "vigente")
        logger.info(f"  Found {len(active)} active circular PDFs")

        # 2. Derogated circulars (paginated)
        logger.info("Fetching derogated circulars...")
        for page_num in range(1, 10):  # Up to 10 pages, will stop if no results
            if page_num == 1:
                url = DEROGADAS_URL
            else:
                url = f"{DEROGADAS_URL}?page_40812={page_num}"

            try:
                entries = _scrape_circulares_page(url)
            except Exception as e:
                logger.warning(f"Error fetching derogated page {page_num}: {e}")
                break

            new_entries = [e for e in entries if e["pdf_url"] not in seen_urls]
            if not new_entries:
                logger.info(f"  Page {page_num}: no new entries, stopping pagination")
                break

            _add_docs(entries, "circular", "derogada")
            logger.info(f"  Page {page_num}: {len(new_entries)} new derogated PDFs")

        # 3. Ley 19.913 and policy docs
        logger.info("Fetching law and policy documents...")
        try:
            ley_entries = _scrape_circulares_page(LEY_URL)
            _add_docs(ley_entries, "ley", "vigente")
            logger.info(f"  Found {len(ley_entries)} law/policy PDFs")
        except Exception as e:
            logger.warning(f"Error fetching ley page: {e}")

        # 4. Predicate offences catalog
        logger.info("Fetching delitos catalog...")
        try:
            delitos_entries = _scrape_circulares_page(DELITOS_URL)
            _add_docs(delitos_entries, "catalog", "vigente")
            logger.info(f"  Found {len(delitos_entries)} catalog PDFs")
        except Exception as e:
            logger.warning(f"Error fetching delitos page: {e}")

        logger.info(f"Total unique documents: {len(docs)}")
        return docs

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Yield all UAF normativa records with full text."""
        docs = self._collect_all_documents()
        count = 0

        for doc in docs:
            if max_records and count >= max_records:
                return

            record = self._process_document(doc)
            if record:
                yield record
                count += 1
                logger.info(f"[{count}] {record['_id']} ({len(record.get('text', ''))} chars)")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents (small corpus, full re-fetch is fine)."""
        yield from self.fetch_all()

    def _process_document(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download PDF and build normalized record."""
        pdf_url = doc["pdf_url"]
        full_url = BASE_URL + pdf_url if pdf_url.startswith("/") else pdf_url

        # Generate a stable ID from the PDF filename
        fname = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")
        # Clean up random suffixes like _mHMIpGW
        clean_id = re.sub(r'_[A-Za-z0-9]{7}$', '', fname)
        doc_id = clean_id.lower().replace(" ", "-")

        logger.info(f"Downloading PDF: {fname}")
        time.sleep(2)  # Rate limit

        try:
            pdf_data = _get_bytes(full_url, timeout=120)
        except Exception as e:
            logger.warning(f"PDF download failed for {fname}: {e}")
            return None

        if len(pdf_data) < 100:
            logger.warning(f"PDF too small for {fname} ({len(pdf_data)} bytes)")
            return None

        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_data,
            table="legislation",
        )

        if not text or len(text) < 50:
            logger.warning(f"No/insufficient text for {fname} ({len(text) if text else 0} chars)")
            return None

        doc["text"] = text
        doc["doc_id"] = doc_id
        return self.normalize(doc)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        date = _parse_date_spanish(raw.get("date_str", ""))
        pdf_url = raw["pdf_url"]
        full_url = BASE_URL + pdf_url if pdf_url.startswith("/") else pdf_url

        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": date,
            "url": full_url,
            "category": raw.get("category", ""),
            "status": raw.get("status", ""),
        }


# ── CLI entry point ─────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/UAF data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = UAFScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            html = _get(CIRCULARES_URL, timeout=30)
            links = _extract_pdf_links(html)
            logger.info(f"  Found {len(links)} PDF links on circulares page")
            if links:
                logger.info(f"  First: {links[0]['title']}")
            logger.info("Connectivity OK")
        except Exception as e:
            logger.error(f"Test failed: {e}")
            sys.exit(1)
        return

    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)
    count = 0
    limit = 15 if args.sample else 999999

    gen = scraper.fetch_all(max_records=limit) if args.command == "bootstrap" else scraper.fetch_updates()

    for record in gen:
        if count >= limit:
            break
        text_len = len(record.get("text", ""))
        if text_len < 100:
            logger.warning(f"Skipping {record['_id']}: text too short ({text_len} chars)")
            continue

        fname = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"]) + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"[{count}/{limit}] Saved {record['_id']} ({text_len} chars)")

    logger.info(f"Done: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
