#!/usr/bin/env python3
"""
MA/AMMC -- Autorité Marocaine du Marché des Capitaux — Circulars & Regulations

Fetches capital markets regulatory documents (circulars, arrêtés, laws, decrees,
general regulations) from AMMC (Morocco) at ammc.ma.

Strategy:
  - Crawl paginated regulation category pages on ammc.ma
  - Extract PDF download links from each category
  - Download each PDF and extract text with pdfplumber (fallback PyPDF2)
  - Skip scanned-image PDFs without extractable text
  - ~146 PDFs total across 6 categories, ~20% have native text layers

Endpoints:
  - Circulaires: https://www.ammc.ma/fr/reglementations/circulaires
  - Arrêtés: https://www.ammc.ma/fr/reglementations/arretes
  - Dahirs/Lois: https://www.ammc.ma/fr/reglementations/dahirs-lois
  - Décrets: https://www.ammc.ma/fr/reglementations/decrets
  - Règlements: https://www.ammc.ma/fr/reglementations/reglements-generaux
  - Autres: https://www.ammc.ma/fr/reglementations/autres-reglementations

Data:
  - Capital markets circulars, ministerial arrêtés, laws, decrees
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
from urllib.parse import urljoin, unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MA.AMMC")

BASE_URL = "https://www.ammc.ma"
SOURCE_ID = "MA/AMMC"

# Regulation category pages: (path, label, max_page)
REGULATION_CATEGORIES = [
    ("/fr/reglementations/circulaires", "Circulaires", 1),
    ("/fr/reglementations/arretes", "Arrêtés", 7),
    ("/fr/reglementations/dahirs-lois", "Dahirs & Lois", 2),
    ("/fr/reglementations/decrets", "Décrets", 1),
    ("/fr/reglementations/reglements-generaux", "Règlements généraux", 0),
    ("/fr/reglementations/autres-reglementations", "Autres réglementations", 0),
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

    # Split by </li> to find items containing PDFs
    items = html_text.split("</li>")
    for item in items:
        if "field-fichier-joint" not in item and ".pdf" not in item:
            continue

        pdf_match = re.search(r'href="([^"]+\.pdf[^"]*)"', item)
        if not pdf_match:
            continue

        rel_url = pdf_match.group(1)
        abs_url = urljoin(BASE_URL, rel_url)
        if abs_url in seen:
            continue
        seen.add(abs_url)

        # Get title from field-reference-libre or PDF anchor text
        title = None
        ref_match = re.search(
            r'field-field-reference-libre.*?field-content">(.*?)</div>',
            item, re.DOTALL
        )
        if ref_match:
            title = re.sub(r'<[^>]+>', '', ref_match.group(1)).strip()

        if not title or len(title) < 10:
            # Fall back to anchor text (PDF filename)
            anchor_match = re.search(r'>([^<]+\.pdf)</a>', item)
            if anchor_match:
                title = html_mod.unescape(anchor_match.group(1))
                title = re.sub(r'\.pdf$', '', title, flags=re.IGNORECASE).strip()
            else:
                title = _title_from_url(rel_url)

        title = html_mod.unescape(title) if title else "Untitled"

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


def _extract_text_from_pdf(pdf_bytes: bytes, source_id: str) -> Optional[str]:
    """Extract text from PDF bytes via the shared helper.

    Calling pdfplumber/PyPDF2 directly emitted glyphs in the PDF's own content
    stream order, which for the Arabic half of this corpus is *visual* order —
    every line came out character-reversed and unsearchable (issue #1560).
    `extract_pdf_markdown` re-orders RTL clusters by x-geometry, so the fix is
    simply to stop extracting here. `force=True` because the rows being
    replaced are the reversed ones already in Neon, which it otherwise skips.
    """
    try:
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
        return None


def _parse_date(title: str) -> Optional[str]:
    """Extract a date from the document title."""
    month_names = '|'.join(MONTHS_FR.keys())

    # Pattern: "du DD mois YYYY"
    m = re.search(rf'du\s+(\d{{1,2}})\s+({month_names})\s+(\d{{4}})', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = MONTHS_FR.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Pattern: "(DD mois YYYY)"
    m = re.search(rf'\(\s*(\d{{1,2}})\s+({month_names})\s+(\d{{4}})\s*\)', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = MONTHS_FR.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Pattern: "mois YYYY"
    m = re.search(rf'({month_names})\s+(\d{{4}})', title, re.IGNORECASE)
    if m:
        month = MONTHS_FR.get(m.group(1).lower(), "01")
        year = m.group(2)
        if 1980 <= int(year) <= 2030:
            return f"{year}-{month}-01"

    # Pattern: year from number like "n° 02-19" or "02_18" or "03-18"
    m = re.search(r'n°?\s*\d+[-_/](\d{2})\b', title)
    if m:
        yr = int(m.group(1))
        year = 2000 + yr if yr < 50 else 1900 + yr
        if 1980 <= year <= 2030:
            return f"{year}-01-01"

    # Pattern: 4-digit year in title
    m = re.search(r'\b(20[012]\d)\b', title)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def _make_id(pdf_url: str) -> str:
    """Create a stable ID from the PDF URL."""
    path = pdf_url.split("ammc.ma")[-1] if "ammc.ma" in pdf_url else pdf_url
    return hashlib.md5(path.encode()).hexdigest()[:16]


class AMMCScraper(BaseScraper):
    SOURCE = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir=source_dir)
        self.client = HttpClient(base_url=BASE_URL)

    def test(self) -> bool:
        try:
            resp = self.client.get(
                f"{BASE_URL}/fr/reglementations/circulaires",
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
            entries = entries[:80]  # test more since many are scanned

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

                text = _extract_text_from_pdf(pdf_bytes, _make_id(entry["pdf_url"]))
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
        date = _parse_date(title)

        doc_type = "legislation"
        cat_lower = raw.get("category", "").lower()
        if "circulaire" in cat_lower or "autre" in cat_lower:
            doc_type = "doctrine"
        elif "arrêté" in cat_lower or "décret" in cat_lower:
            doc_type = "legislation"

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
    scraper = AMMCScraper()
    args = sys.argv[1:]
    cmd = args[0] if args else "test"

    if cmd == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif cmd == "bootstrap":
        sample = "--sample" in args
        here = Path(__file__).resolve().parent
        count = 0
        if sample:
            out_dir = here / "sample"
            out_dir.mkdir(exist_ok=True)
            for doc in scraper.fetch_all(sample=True):
                count += 1
                with open(out_dir / f"{doc['_id']}.json", "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
            print(f"Saved {count} records to {out_dir}")
        else:
            # A full run must stream to data/records.jsonl — that is what the
            # pipeline ingests. Writing the corpus into sample/ instead left the
            # fleet with nothing to read and it fell back to the 15 bundled
            # samples (issue #798 class).
            data_dir = here / "data"
            data_dir.mkdir(exist_ok=True)
            out_path = data_dir / "records.jsonl"
            with open(out_path, "w", encoding="utf-8") as f:
                for doc in scraper.fetch_all(sample=False):
                    count += 1
                    f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                    f.flush()
            print(f"Wrote {count} records to {out_path}")

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
