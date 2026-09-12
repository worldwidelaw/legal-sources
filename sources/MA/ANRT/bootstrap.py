#!/usr/bin/env python3
"""
MA/ANRT -- Morocco ANRT Telecom Regulatory Decisions

Fetches telecom regulatory documents (decisions, arrêtés, laws, decrees)
from ANRT (Morocco) at anrt.ma.

Strategy:
  - Crawl paginated regulation category pages on anrt.ma
  - Extract PDF download links from each category
  - Download each PDF and extract text with pdfplumber (fallback PyPDF2)
  - Skip scanned-image PDFs without extractable text
  - ~145 PDFs total across 6 categories

Endpoints:
  - Decisions: https://www.anrt.ma/en/reglementation/decisions (11 pages)
  - Arrêtés: https://www.anrt.ma/en/reglementation/arretes
  - Lois telecom: https://www.anrt.ma/en/reglementation/lois/telecommunications
  - Lois autres: https://www.anrt.ma/en/reglementation/lois/autres-lois
  - Décrets telecom: https://www.anrt.ma/en/reglementation/decrets-reglementaires/telecommunications
  - Décrets autres: https://www.anrt.ma/en/reglementation/decrets-reglementaires/autres-decrets

Data:
  - Telecom regulatory decisions, ministerial orders, laws, decrees
  - French language (primary)
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10-15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import hashlib
import html as html_mod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MA.ANRT")

BASE_URL = "https://www.anrt.ma"
SOURCE_ID = "MA/ANRT"

# Regulation category pages: (path, label, max_page)
REGULATION_CATEGORIES = [
    ("/en/reglementation/decisions", "Décisions", 10),
    ("/en/reglementation/arretes", "Arrêtés", 0),
    ("/en/reglementation/lois/telecommunications", "Lois télécommunications", 0),
    ("/en/reglementation/lois/autres-lois", "Autres lois", 0),
    ("/en/reglementation/decrets-reglementaires/telecommunications", "Décrets télécommunications", 1),
    ("/en/reglementation/decrets-reglementaires/autres-decrets", "Autres décrets", 0),
]

MONTHS_FR = {
    "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "décembre": "12", "decembre": "12",
}


def _extract_pdf_entries(html_text: str, category: str) -> List[Dict[str, Any]]:
    """Extract PDF entries (title + URL) from a regulation category page."""
    entries = []
    seen = set()

    for m in re.finditer(r'href="([^"]+\.pdf[^"]*)"[^>]*>(.*?)</a>', html_text, re.DOTALL):
        rel_url = m.group(1)
        link_text = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        abs_url = urljoin(BASE_URL, rel_url)

        if abs_url in seen:
            continue
        seen.add(abs_url)

        title = html_mod.unescape(link_text) if link_text and len(link_text) > 10 else _title_from_url(rel_url)

        entries.append({
            "pdf_url": abs_url,
            "title": title,
            "category": category,
        })

    return entries


def _title_from_url(url: str) -> str:
    """Extract a readable title from a PDF URL."""
    filename = unquote(url.split("/")[-1])
    filename = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
    filename = re.sub(r'[-_]+', ' ', filename).strip()
    return filename if len(filename) > 5 else "Untitled"


def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfplumber, fallback to PyPDF2."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            if pages:
                return "\n\n".join(pages)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        if pages:
            return "\n\n".join(pages)
    except Exception as e:
        logger.debug(f"PyPDF2 failed: {e}")

    return None


def _parse_date(title: str) -> Optional[str]:
    """Extract a date from the document title."""
    month_names = '|'.join(MONTHS_FR.keys())

    # Pattern: "DU DD mois YYYY" or "du DD mois YYYY"
    m = re.search(rf'[Dd][Uu]\s+(\d{{1,2}})\s+({month_names})\s+(\d{{4}})', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = MONTHS_FR.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Pattern: "DD mois YYYY"
    m = re.search(rf'(\d{{1,2}})\s+({month_names})\s+(\d{{4}})', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = MONTHS_FR.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Pattern: year from ANRT decision number "N°XX/YYYY"
    m = re.search(r'N°?\s*\d+/(\d{4})', title)
    if m:
        year = int(m.group(1))
        if 1990 <= year <= 2030:
            return f"{year}-01-01"

    # Pattern: year in folder path (e.g., /2025-11/)
    m = re.search(r'/(\d{4})-(\d{2})/', title)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # Pattern: 4-digit year
    m = re.search(r'\b(20[012]\d|199\d)\b', title)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def _make_id(pdf_url: str) -> str:
    """Create a stable ID from the PDF URL."""
    path = pdf_url.split("anrt.ma")[-1] if "anrt.ma" in pdf_url else pdf_url
    return hashlib.md5(path.encode()).hexdigest()[:16]


class ANRTScraper(BaseScraper):
    SOURCE = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir=source_dir)
        self.client = HttpClient(base_url=BASE_URL)

    def test(self) -> bool:
        try:
            resp = self.client.get(
                f"{BASE_URL}/en/reglementation/decisions",
                verify=False,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Crawl all category pages and collect PDF entries."""
        all_entries = []
        seen_urls = set()

        for path, category, max_page in REGULATION_CATEGORIES:
            for page in range(max_page + 1):
                url = f"{BASE_URL}{path}?page={page}" if page > 0 else f"{BASE_URL}{path}"
                logger.info(f"Crawling: {category} page {page}")
                try:
                    resp = self.client.get(url, verify=False)
                    if resp.status_code != 200:
                        logger.warning(f"HTTP {resp.status_code} for {url}")
                        continue
                    entries = _extract_pdf_entries(resp.text, category)
                    new = 0
                    for entry in entries:
                        if entry["pdf_url"] not in seen_urls:
                            seen_urls.add(entry["pdf_url"])
                            all_entries.append(entry)
                            new += 1
                    logger.info(f"  +{new} PDFs ({len(all_entries)} total)")
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Error crawling {category} page {page}: {e}")
                    continue

        logger.info(f"Total unique PDFs discovered: {len(all_entries)}")
        return all_entries

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        entries = self._discover_documents()

        if sample:
            entries = entries[:40]

        yielded = 0
        skipped_scan = 0

        for i, entry in enumerate(entries):
            if sample and yielded >= 15:
                break

            logger.info(f"[{i+1}/{len(entries)}] Downloading: {entry['title'][:60]}")
            try:
                resp = self.client.get(entry["pdf_url"], timeout=120, verify=False)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code}: {entry['pdf_url']}")
                    continue

                pdf_bytes = resp.content
                if len(pdf_bytes) < 200:
                    logger.warning(f"PDF too small ({len(pdf_bytes)} bytes)")
                    continue

                text = _extract_text_from_pdf(pdf_bytes)
                if not text or len(text.strip()) < 100:
                    skipped_scan += 1
                    logger.info(f"  Skipped (scanned/no text): {entry['title'][:50]}")
                    continue

                doc = self.normalize({
                    "title": entry["title"],
                    "text": text.strip(),
                    "category": entry["category"],
                    "pdf_url": entry["pdf_url"],
                    "pdf_size": len(pdf_bytes),
                })
                yielded += 1
                logger.info(
                    f"  OK: {len(text)} chars, category={entry['category']}"
                )
                yield doc
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error processing {entry['title'][:50]}: {e}")
                continue

        logger.info(
            f"Done: {yielded} documents with text, {skipped_scan} scanned PDFs skipped"
        )

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        logger.info(f"Incremental update since {since} — re-running full fetch")
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw["title"]
        doc_id = _make_id(raw["pdf_url"])
        date = _parse_date(title) or _parse_date(raw["pdf_url"])

        cat_lower = raw.get("category", "").lower()
        if "décision" in cat_lower or "decision" in cat_lower:
            doc_type = "doctrine"
        elif "loi" in cat_lower or "décret" in cat_lower or "arrêté" in cat_lower:
            doc_type = "legislation"
        else:
            doc_type = "doctrine"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": raw["pdf_url"],
            "category": raw.get("category", ""),
            "pdf_size": raw.get("pdf_size"),
        }


# ── CLI entry point ──────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ANRTScraper()
    args = sys.argv[1:]
    cmd = args[0] if args else "test"

    if cmd == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif cmd == "bootstrap":
        sample = "--sample" in args
        out_dir = Path(__file__).resolve().parent / "sample"
        out_dir.mkdir(exist_ok=True)
        count = 0
        for doc in scraper.fetch_all(sample=sample):
            count += 1
            fname = out_dir / f"{doc['_id']}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
        print(f"Saved {count} records to {out_dir}")

    elif cmd == "update":
        since = args[1] if len(args) > 1 else "2024-01-01"
        count = 0
        for doc in scraper.fetch_updates(since):
            count += 1
        print(f"Updated {count} records")

    else:
        print(f"Unknown command: {cmd}")
        print("Usage: bootstrap.py [test|bootstrap [--sample]|update [since]]")
        sys.exit(1)
