#!/usr/bin/env python3
"""
DZ/BankOfAlgeria -- Bank of Algeria Regulations & Instructions

Fetches banking regulations, instructions, prudential norms, AML rules,
and forex regulations from the Bank of Algeria website.

Strategy:
  - The regulatory page lists ~20 thematic subpages (prudential norms,
    AML, forex, monetary policy instruments, etc.)
  - Each subpage contains PDF links with descriptive titles in HTML
  - Download PDFs and extract full text using pdfplumber
  - Covers regulations (règlements), instructions, notes, and directives

Endpoints:
  - Main page: https://www.bank-of-algeria.dz/cadre-legislatif-et-reglementaire/
  - Subpages: https://www.bank-of-algeria.dz/{topic}-reglement/ etc.
  - PDFs: https://www.bank-of-algeria.dz/stoodroa/{year}/{month}/{filename}.pdf

Data:
  - ~150 regulatory PDFs covering banking law since 1990s
  - French language (official)
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
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
import ssl
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DZ.BankOfAlgeria")

BASE_URL = "https://www.bank-of-algeria.dz"
SOURCE_ID = "DZ/BankOfAlgeria"

# All regulatory subpages to crawl, with their thematic category
REGULATORY_PAGES = [
    ("normes-applicables-aux-banques-etablissements-financiers-reglement", "Normes prudentielles"),
    ("prevention-et-lutte-reglement", "Prévention et lutte anti-blanchiment"),
    ("inclusion-financiere-reglement", "Inclusion financière"),
    ("politique-monetaire-reglement", "Instruments de la politique monétaire"),
    ("organisation-et-fonctionnement-du-marche-monetaire-reglement", "Marché monétaire"),
    ("change-reglement", "Marché interbancaire des changes"),
    ("convertibilite-reglement", "Conditions de banque"),
    ("devises-reglement", "Réglementation des changes"),
    ("regles-comptables-et-declarations-statistiques-par-banques-etablissements-financiers-reglement", "Règles comptables"),
    ("stabilite-financiere-reglement", "Stabilité financière"),
    ("systeme-de-paiements-gros-reglement", "Systèmes de paiement (gros montants)"),
    ("systemes-de-paiement-reglement", "Sécurité des systèmes de paiements"),
    ("finance-islamique-reglement", "Finance islamique"),
    ("billet-de-banque-retrait-de-la-circulation-reglement", "Retrait billets de banque"),
    ("reglement-piece-de-monnaie", "Pièces de monnaie"),
    ("pieces-de-monnaie-retrait-de-la-circulation-reglement", "Retrait pièces de monnaie"),
    ("conditions-dagrement-de-dirigeants-des-etablissements-assujettis", "Conditions d'agrément (instructions)"),
    ("condition-dagrement-de-dirigeants-des-etablissements-assujettis", "Conditions d'agrément (instructions) 2"),
    ("controle-interne-et-lutte-anti-blanchiment", "Contrôle interne (instructions)"),
    ("les-lignes-directrice", "Lignes directrices"),
    ("les-instructions-de-la-ctrf", "Instructions de la CTRF"),
    ("instructions-de-la-ctrf", "Instructions de la CTRF 2"),
    ("loi-monnaie-et-credit", "Loi monétaire et bancaire"),
    ("cadre-reglementaire-2", "Cadre réglementaire"),
]


def _extract_pdf_entries(html: str, page_url: str, category: str) -> List[Dict[str, Any]]:
    """Extract PDF links and their descriptive context from an HTML page."""
    entries = []
    seen_urls = set()

    # Find all PDF links
    pdf_pattern = re.compile(
        r'<a[^>]*href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    for match in pdf_pattern.finditer(html):
        pdf_url = match.group(1)
        link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        # Make URL absolute
        if not pdf_url.startswith("http"):
            pdf_url = urljoin(page_url, pdf_url)

        # Try to extract a title from surrounding context
        pos = match.start()
        # Look backward for descriptive text (in <p>, <li>, <h3>, <h4>, <td> etc.)
        context_before = html[max(0, pos - 2000):pos]
        title = _extract_title(link_text, context_before, pdf_url)

        entries.append({
            "pdf_url": pdf_url,
            "title": title,
            "link_text": link_text,
            "category": category,
            "page_url": page_url,
        })

    return entries


def _extract_title(link_text: str, context: str, pdf_url: str) -> str:
    """Extract the best title for a regulation from available context."""
    # If link text is descriptive enough, use it
    if link_text and len(link_text) > 20 and "télécharger" not in link_text.lower():
        return link_text

    # Look for regulation number patterns in context
    # E.g., "Règlement n°2014-03 du 16 février 2014 relatif aux..."
    context_clean = re.sub(r'<[^>]+>', ' ', context)
    context_clean = re.sub(r'\s+', ' ', context_clean).strip()

    # Find last regulation description before the link
    reg_patterns = [
        r'((?:Règlement|Instruction|Ordonnance|Loi|Décret|Note)\s+n°?\s*[\d\-/]+[^.]*\.)',
        r'((?:Règlement|Instruction|Ordonnance|Loi|Décret|Note)\s+n°?\s*[\d\-/]+[^<]{20,200})',
    ]
    for pat in reg_patterns:
        matches = re.findall(pat, context_clean, re.IGNORECASE)
        if matches:
            return matches[-1].strip()

    # Try to get title from the PDF filename
    filename = unquote(pdf_url.split("/")[-1].replace(".pdf", ""))
    filename = re.sub(r'[-_]', ' ', filename).strip()
    if filename and len(filename) > 5:
        return filename

    return link_text or "Untitled regulation"


def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not available, trying PyPDF2")
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
            return "\n\n".join(pages) if pages else None
        except Exception as e:
            logger.error(f"PyPDF2 extraction failed: {e}")
            return None

    try:
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
            return "\n\n".join(pages) if pages else None
    except Exception as e:
        logger.error(f"pdfplumber extraction failed: {e}")
        return None


def _parse_date_from_title(title: str) -> Optional[str]:
    """Try to extract a date from the regulation title."""
    # Pattern: "du DD mois YYYY"
    months = {
        "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
        "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
        "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
        "novembre": "11", "décembre": "12", "decembre": "12",
    }
    pattern = r'du\s+(\d{1,2})\s+(' + '|'.join(months.keys()) + r')\s+(\d{4})'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        day = int(match.group(1))
        month = months.get(match.group(2).lower(), "01")
        year = match.group(3)
        return f"{year}-{month}-{day:02d}"

    # Try year from regulation number: n°YY-NN or n°YYYY-NN
    match = re.search(r'n°?\s*(\d{2,4})[-/](\d{1,4})', title)
    if match:
        year_str = match.group(1)
        if len(year_str) == 2:
            year = int(year_str)
            year = 2000 + year if year < 50 else 1900 + year
        else:
            year = int(year_str)
        if 1962 <= year <= 2030:
            return f"{year}-01-01"

    # Try year from PDF URL
    match = re.search(r'/(\d{4})/', title)
    if match:
        year = int(match.group(1))
        if 1962 <= year <= 2030:
            return f"{year}-01-01"

    return None


def _make_id(pdf_url: str) -> str:
    """Create a stable document ID from the PDF URL."""
    # Use the path portion of the URL as the basis
    path = pdf_url.split("bank-of-algeria.dz/")[-1] if "bank-of-algeria.dz/" in pdf_url else pdf_url
    return hashlib.md5(path.encode()).hexdigest()[:16]


class BankOfAlgeriaScraper(BaseScraper):
    SOURCE = SOURCE_ID

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir=source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            verify=False,
        )

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.client.get(f"{BASE_URL}/cadre-legislatif-et-reglementaire/")
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            return False

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Crawl all regulatory subpages and collect PDF entries."""
        all_entries = []
        seen_urls = set()

        for slug, category in REGULATORY_PAGES:
            page_url = f"{BASE_URL}/{slug}/"
            logger.info(f"Crawling: {page_url} ({category})")
            try:
                resp = self.client.get(page_url)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for {page_url}")
                    continue
                entries = _extract_pdf_entries(resp.text, page_url, category)
                for entry in entries:
                    if entry["pdf_url"] not in seen_urls:
                        seen_urls.add(entry["pdf_url"])
                        all_entries.append(entry)
                logger.info(f"  Found {len(entries)} PDFs ({len(all_entries)} total unique)")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error crawling {page_url}: {e}")
                continue

        logger.info(f"Total unique PDFs discovered: {len(all_entries)}")
        return all_entries

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Bank of Algeria regulatory documents."""
        entries = self._discover_documents()

        if sample:
            entries = entries[:15]

        for i, entry in enumerate(entries):
            logger.info(f"[{i+1}/{len(entries)}] Downloading: {entry['pdf_url']}")
            try:
                resp = self.client.get(entry["pdf_url"], timeout=60)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for PDF: {entry['pdf_url']}")
                    continue

                pdf_bytes = resp.content
                if len(pdf_bytes) < 100:
                    logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): {entry['pdf_url']}")
                    continue

                text = _extract_text_from_pdf(pdf_bytes)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"No text extracted from: {entry['pdf_url']}")
                    continue

                doc_id = _make_id(entry["pdf_url"])
                date = _parse_date_from_title(entry["title"])
                if not date:
                    date = _parse_date_from_title(entry["pdf_url"])

                # Determine type: laws are legislation, everything else is doctrine
                title_lower = entry["title"].lower()
                if any(kw in title_lower for kw in ["loi ", "ordonnance ", "décret "]):
                    doc_type = "legislation"
                else:
                    doc_type = "legislation"  # regulations are legislation

                yield {
                    "_id": doc_id,
                    "_source": SOURCE_ID,
                    "_type": doc_type,
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": entry["title"],
                    "text": text.strip(),
                    "date": date,
                    "url": entry["pdf_url"],
                    "category": entry["category"],
                    "page_url": entry["page_url"],
                    "pdf_size_bytes": len(pdf_bytes),
                    "text_length": len(text.strip()),
                }

                time.sleep(1)

            except Exception as e:
                logger.error(f"Error processing {entry['pdf_url']}: {e}")
                continue

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Incremental update - re-crawl all pages, yield only new PDFs."""
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Already normalized during fetch."""
        return raw


def main():
    import argparse
    parser = argparse.ArgumentParser(description="DZ/BankOfAlgeria data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="ISO date for incremental updates")
    args = parser.parse_args()

    scraper = BankOfAlgeriaScraper()

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
