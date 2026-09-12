#!/usr/bin/env python3
"""
MA/BankAlMaghrib -- Bank Al-Maghrib Circulars & Regulations

Fetches banking regulations, circulars, directives, and instructions from
Bank Al-Maghrib (Morocco's central bank) at bkam.ma.

Strategy:
  - Crawl all regulation category pages under /Reglementation/
  - Extract PDF download links (/content/download/...)
  - Download each PDF and extract text with pdfplumber (fallback PyPDF2)
  - Skip scanned-image PDFs without extractable text
  - ~268 PDFs total, ~23% have native text layers (~60+ documents)

Endpoints:
  - Regulation hub: https://www.bkam.ma/Trouvez-l-information-concernant/Reglementation
  - PDFs: https://www.bkam.ma/content/download/{id}/{id}/{filename}.pdf

Data:
  - Circulars, directives, arrêtés, instructions, banking laws
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
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MA.BankAlMaghrib")

BASE_URL = "https://www.bkam.ma"
SOURCE_ID = "MA/BankAlMaghrib"

# All regulation category pages to crawl
REGULATION_PAGES = [
    ("/Trouvez-l-information-concernant/Reglementation/Statut-et-missions", "Statut et missions"),
    ("/Trouvez-l-information-concernant/Reglementation/Loi-bancaire", "Loi bancaire"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-fiduciaire/Circulation/Decrets/Circulation", "Activité fiduciaire - Circulation"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-fiduciaire/Retrait/Retrait/Decrets-de-retrait-des-billets-et-des-pieces-en-circulation", "Activité fiduciaire - Retrait"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-fiduciaire/Traitement-des-billets-de-banque-par-les-tiers-prives2/Articles/Traitement-des-billets-de-banque-par-les-tiers-prives", "Activité fiduciaire - Traitement billets"),
    ("/Trouvez-l-information-concernant/Reglementation/Marche-monetaire/Interventions-de-bank-al-maghrib-sur-le-marche-monetaire", "Marché monétaire - Interventions BAM"),
    ("/Trouvez-l-information-concernant/Reglementation/Marche-monetaire/Operations-de-pension/Operations-de-pension/Operations-de-pension", "Marché monétaire - Opérations de pension"),
    ("/Trouvez-l-information-concernant/Reglementation/Marche-monetaire/Adjudications-de-bons-du-tresor/Articles/Adjudications-de-bons-du-tresor", "Marché monétaire - Bons du Trésor"),
    ("/Trouvez-l-information-concernant/Reglementation/Marche-monetaire/Titres-de-creances-negociables/Articles/Titres-de-creances-negociables", "Marché monétaire - TCN"),
    ("/Trouvez-l-information-concernant/Reglementation/Taux-d-interet", "Taux d'intérêt"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-des-etablissements-de-credit-et-assimiles/Conditions-d-exercice-agrement", "Conditions d'exercice - Agrément"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-des-etablissements-de-credit-et-assimiles/Reglementation-comptable", "Réglementation comptable"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-des-etablissements-de-credit-et-assimiles/Reglementation-prudentielle", "Réglementation prudentielle"),
    ("/Trouvez-l-information-concernant/Reglementation/Activite-des-etablissements-de-credit-et-assimiles/Reglementation-des-relations-etablissements-de-credit-clientele", "Relations établissements-clientèle"),
    ("/Trouvez-l-information-concernant/Reglementation/Systemes-et-moyens-de-paiement", "Systèmes et moyens de paiement"),
    ("/Trouvez-l-information-concernant/Reglementation/Marche-de-changes/Circulaires/Marche-de-changes", "Marché de changes"),
    ("/Trouvez-l-information-concernant/Reglementation/Banque-participative", "Banque participative"),
    ("/Trouvez-l-information-concernant/Reglementation/Institution-de-microfinance-imf", "Institution de microfinance"),
]

MONTHS_FR = {
    "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "décembre": "12", "decembre": "12",
}


def _extract_pdf_links(html: str, page_url: str, category: str) -> List[Dict[str, Any]]:
    """Extract PDF download links and their titles from a category page."""
    entries = []
    seen_urls = set()

    pattern = re.compile(
        r'<a[^>]*href="(/content/download/[^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(html):
        rel_url = match.group(1)
        link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

        if rel_url in seen_urls:
            continue
        seen_urls.add(rel_url)

        abs_url = urljoin(page_url, rel_url)
        title = _best_title(link_text, rel_url)

        entries.append({
            "pdf_url": abs_url,
            "title": title,
            "category": category,
            "page_url": page_url,
        })

    return entries


def _best_title(link_text: str, pdf_url: str) -> str:
    """Pick the best title from link text or PDF filename."""
    if link_text and len(link_text) > 15:
        return link_text

    # Fall back to filename
    filename = unquote(pdf_url.split("/")[-1])
    filename = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
    filename = re.sub(r'[-_]', ' ', filename).strip()
    return filename if len(filename) > 5 else link_text or "Untitled"


def _extract_text_from_pdf(pdf_bytes: bytes, doc_id: str) -> Optional[str]:
    """Extract text from PDF bytes via the shared helper.

    This used to call pdfplumber with a PyPDF2 fallback. Both emit glyphs in the
    order the content stream lists them, which for the Arabic circulars here is
    *visual* order, so those lines were stored character-reversed — ``قانون`` as
    ``نوناق`` (issue #1560). Keyword search then matches nothing, and silently:
    the semantic half of a hybrid index still returns something, so the source
    looks healthy while ranking against gibberish.

    ``common.pdf_extract`` routes RTL-heavy output back through
    ``common.arabic_pdf``, which orders glyph clusters by x-geometry rather than
    trusting the emitted sequence. Bank Al-Maghrib publishes mostly in French;
    those documents are not RTL, so the repair leaves them untouched.
    """
    try:
        return extract_pdf_markdown(
            SOURCE_ID,
            doc_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
            force=True,
        )
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
        return None


def _parse_date(title: str, pdf_url: str = "") -> Optional[str]:
    """Extract a date from the document title or URL."""
    month_names = '|'.join(MONTHS_FR.keys())

    # Pattern: "(DD mois YYYY)" — Gregorian date in parentheses after hijri date
    m = re.search(rf'\(\s*(\d{{1,2}})\s+({month_names})\s+(\d{{4}})\s*\)', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = MONTHS_FR.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Pattern: "du DD mois YYYY"
    m = re.search(rf'du\s+(\d{{1,2}})\s+({month_names})\s+(\d{{4}})', title, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = MONTHS_FR.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{day:02d}"

    # Pattern: "mois YYYY" standalone (e.g., "décembre 2016")
    m = re.search(rf'({month_names})\s+(\d{{4}})', title, re.IGNORECASE)
    if m:
        month = MONTHS_FR.get(m.group(1).lower(), "01")
        year = m.group(2)
        if 1980 <= int(year) <= 2030:
            return f"{year}-{month}-01"

    # Pattern: year from regulation number (e.g., "4W2024", "n°2014-03", "n°15/G/2013")
    m = re.search(r'[Ww/](\d{4})\b', title)
    if m:
        year = int(m.group(1))
        if 1980 <= year <= 2030:
            return f"{year}-01-01"

    m = re.search(r'n°?\s*(\d{4})[-/.]', title)
    if m:
        year = int(m.group(1))
        if 1980 <= year <= 2030:
            return f"{year}-01-01"

    m = re.search(r'n°?\s*(\d{1,2})[/-][Gg][/-](\d{2,4})', title)
    if m:
        year_str = m.group(2)
        year = int(year_str)
        if year < 100:
            year = 2000 + year if year < 50 else 1900 + year
        if 1980 <= year <= 2030:
            return f"{year}-01-01"

    # Pattern: "loi n°XX-YY" where YY might encode a year (e.g., "loi n°40-17" → 2017)
    m = re.search(r'n°?\s*\d+-(\d{2})\b', title)
    if m:
        yr = int(m.group(1))
        year = 2000 + yr if yr < 50 else 1900 + yr
        if 1980 <= year <= 2030:
            return f"{year}-01-01"

    return None


def _make_id(pdf_url: str) -> str:
    """Create a stable ID from the PDF URL."""
    path = pdf_url.split("bkam.ma")[-1] if "bkam.ma" in pdf_url else pdf_url
    return hashlib.md5(path.encode()).hexdigest()[:16]


class BankAlMaghribScraper(BaseScraper):
    SOURCE = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir=source_dir)
        self.client = HttpClient(base_url=BASE_URL)

    def test(self) -> bool:
        try:
            resp = self.client.get(
                f"{BASE_URL}/Trouvez-l-information-concernant/Reglementation"
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Crawl all category pages and collect PDF entries."""
        all_entries = []
        seen_urls = set()

        for path, category in REGULATION_PAGES:
            page_url = BASE_URL + path
            logger.info(f"Crawling: {category} ({path.split('/')[-1]})")
            try:
                resp = self.client.get(page_url)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for {page_url}")
                    continue
                entries = _extract_pdf_links(resp.text, page_url, category)
                new = 0
                for entry in entries:
                    if entry["pdf_url"] not in seen_urls:
                        seen_urls.add(entry["pdf_url"])
                        all_entries.append(entry)
                        new += 1
                logger.info(f"  +{new} PDFs ({len(all_entries)} total)")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error crawling {category}: {e}")
                continue

        logger.info(f"Total unique PDFs: {len(all_entries)}")
        return all_entries

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        entries = self._discover_documents()

        if sample:
            entries = entries[:50]  # test more PDFs since many are scanned

        yielded = 0
        skipped_scan = 0

        for i, entry in enumerate(entries):
            if sample and yielded >= 15:
                break

            logger.info(f"[{i+1}/{len(entries)}] Downloading: {entry['title'][:60]}")
            try:
                resp = self.client.get(entry["pdf_url"], timeout=60)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code}: {entry['pdf_url']}")
                    continue

                pdf_bytes = resp.content
                if len(pdf_bytes) < 200:
                    logger.warning(f"PDF too small ({len(pdf_bytes)} bytes)")
                    continue

                doc_id = _make_id(entry["pdf_url"])
                text = _extract_text_from_pdf(pdf_bytes, doc_id)
                if not text or len(text.strip()) < 100:
                    skipped_scan += 1
                    logger.info(f"  Skipped (scanned/no text): {entry['title'][:50]}")
                    continue

                text = text.strip()
                date = _parse_date(entry["title"], entry["pdf_url"])

                title_lower = entry["title"].lower()
                if any(kw in title_lower for kw in ["loi ", "ordonnance ", "décret ", "decret ", "arrêté ", "arrete "]):
                    doc_type = "legislation"
                else:
                    doc_type = "legislation"  # circulars/directives are regulations

                yield {
                    "_id": doc_id,
                    "_source": SOURCE_ID,
                    "_type": doc_type,
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": entry["title"],
                    "text": text,
                    "date": date,
                    "url": entry["pdf_url"],
                    "category": entry["category"],
                    "page_url": entry["page_url"],
                    "pdf_size_bytes": len(pdf_bytes),
                    "text_length": len(text),
                }
                yielded += 1
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error processing: {e}")
                continue

        logger.info(f"Done: {yielded} records yielded, {skipped_scan} scanned PDFs skipped")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MA/BankAlMaghrib data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch 10-15 sample records")
    parser.add_argument("--since", type=str, help="ISO date for incremental updates")
    args = parser.parse_args()

    scraper = BankAlMaghribScraper()

    if args.command == "test":
        ok = scraper.test()
        print(f"Connectivity test: {'PASS' if ok else 'FAIL'}")
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        gen = scraper.fetch_all(sample=args.sample)
    else:
        since = args.since or "2024-01-01"
        gen = scraper.fetch_updates(since)

    count = 0
    for record in gen:
        normalized = scraper.normalize(record)
        out_path = sample_dir / f"{normalized['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        count += 1
        text_len = len(normalized.get("text", ""))
        logger.info(f"Saved: {out_path.name} | {normalized['title'][:60]} | {text_len} chars")

    logger.info(f"Done. Total records: {count}")
    if count == 0:
        logger.error("No records fetched!")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
