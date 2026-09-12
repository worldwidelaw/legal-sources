#!/usr/bin/env python3
"""
SN/Primature-Legislation — Senegal PM Office Laws & Regulations

Fetches laws, codes, decrees, and treaties from primature.sn.

Strategy:
  - Enumerate document listings from 5 categories, paginated
  - For each document page, extract body text (HTML)
  - Download PDF attachments and extract text via pdfminer
  - Combine body text + PDF text for full content

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import html as html_mod
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SN.Primature-Legislation")

BASE_URL = "https://primature.sn"

CATEGORIES = [
    ("codes", "Codes"),
    ("lois-et-decrets", "Lois et Décrets"),
    ("constitution-du-senegal", "Constitution"),
    ("conventions-minieres", "Conventions Minières"),
    ("traites-et-accords-internationaux", "Traités et Accords internationaux"),
]

MAX_PAGES_PER_CATEGORY = 20


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(io.BytesIO(pdf_bytes))
        if text and len(text.strip()) > 50:
            return text.strip()
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
    return ""


def extract_date_from_text(text: str) -> Optional[str]:
    """Try to extract a date from document title/text."""
    # Pattern: "du DD mois YYYY" or "n° YYYY-NNN"
    months_fr = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12',
    }
    # Try "DD month YYYY" pattern
    for month_name, month_num in months_fr.items():
        m = re.search(rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})', text, re.IGNORECASE)
        if m:
            day = int(m.group(1))
            year = int(m.group(2))
            if 1900 <= year <= 2030 and 1 <= day <= 31:
                return f"{year}-{month_num}-{day:02d}"

    # Try "n° YYYY-NNN" to at least get the year
    m = re.search(r'n°\s*(\d{4})-', text)
    if m:
        year = int(m.group(1))
        if 1900 <= year <= 2030:
            return f"{year}-01-01"

    return None


class PrimatureLegislationScraper(BaseScraper):
    """Scraper for Senegal PM Office legislation."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        })

    def _get_listing_links(self, category_slug: str, page: int = 0) -> list:
        """Get document links from a category listing page."""
        url = f"{BASE_URL}/publications/lois-et-reglements/{category_slug}"
        if page > 0:
            url += f"?page={page}"

        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch listing {url}: {e}")
            return []

        html = resp.text
        # Find all document links
        pattern = re.compile(
            r'href="(/publications/lois-et-reglements/[^"]+)"'
        )
        links = []
        for m in pattern.finditer(html):
            path = m.group(1)
            # Skip category links and pagination
            if path.endswith(category_slug) or '?page=' in path:
                continue
            # Skip other category slugs
            category_slugs = [c[0] for c in CATEGORIES]
            if any(path.endswith(f"/{cs}") for cs in category_slugs):
                continue
            full_url = urljoin(BASE_URL, path)
            if full_url not in links:
                links.append(full_url)

        return links

    def _fetch_document(self, url: str, category: str) -> Optional[dict]:
        """Fetch a single document page and extract content."""
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch document {url}: {e}")
            return None

        html = resp.text

        # Extract title
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        title = strip_html(title_match.group(1)) if title_match else ""
        if not title:
            title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
            title = strip_html(title_match.group(1)).replace(" | Gouvernement du Sénégal", "") if title_match else url.split("/")[-1]

        # Extract body text
        body_match = re.search(
            r'<div[^>]*class="[^"]*field--name-body[^"]*"[^>]*>(.*?)</div>\s*(?=<div|$)',
            html, re.DOTALL
        )
        body_text = strip_html(body_match.group(1)) if body_match else ""
        # Remove footer text
        body_text = re.sub(r'©\s*Primature.*$', '', body_text, flags=re.DOTALL).strip()

        # Extract PDF links
        pdf_links = re.findall(
            r'href="([^"]*\.pdf)"',
            html, re.IGNORECASE
        )
        pdf_links = [urljoin(BASE_URL, link) for link in pdf_links]

        # Download and extract text from PDFs
        pdf_texts = []
        for pdf_url in pdf_links:
            time.sleep(0.5)
            try:
                pdf_resp = self.session.get(pdf_url, timeout=30)
                if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                    pdf_text = extract_pdf_text(pdf_resp.content)
                    if pdf_text:
                        pdf_name = pdf_url.split("/")[-1]
                        logger.debug(f"  PDF {pdf_name}: {len(pdf_text)} chars")
                        pdf_texts.append(pdf_text)
            except requests.RequestException as e:
                logger.debug(f"Failed to download PDF {pdf_url}: {e}")

        # Combine all text
        all_texts = []
        if body_text:
            all_texts.append(body_text)
        all_texts.extend(pdf_texts)
        full_text = "\n\n".join(all_texts)

        if not full_text.strip():
            logger.warning(f"No text extracted from {url}")
            return None

        # Extract date
        date = extract_date_from_text(title) or extract_date_from_text(body_text)

        # Extract slug from URL
        slug = url.rstrip("/").split("/")[-1]

        return {
            "url": url,
            "slug": slug,
            "title": title,
            "body_text": body_text,
            "pdf_texts": pdf_texts,
            "full_text": full_text,
            "date": date,
            "category": category,
            "pdf_count": len(pdf_links),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all categories."""
        seen_urls = set()
        for cat_slug, cat_name in CATEGORIES:
            logger.info(f"Fetching category: {cat_name} ({cat_slug})")
            for page in range(MAX_PAGES_PER_CATEGORY):
                links = self._get_listing_links(cat_slug, page)
                if not links:
                    break
                logger.info(f"  Page {page}: {len(links)} documents")
                for link in links:
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    time.sleep(1.0)
                    doc = self._fetch_document(link, cat_name)
                    if doc:
                        yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently published documents."""
        # Re-fetch all and filter by date
        for doc in self.fetch_all():
            if doc.get("date"):
                try:
                    doc_date = datetime.fromisoformat(doc["date"])
                    if doc_date >= since.replace(tzinfo=None):
                        yield doc
                except ValueError:
                    yield doc  # If date parse fails, include it

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document into a standardized record."""
        text = raw.get("full_text", "")
        if not text or len(text.strip()) < 50:
            return None

        return {
            "_id": f"sn-primature-{raw['slug']}",
            "_source": "SN/Primature-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw["category"],
            "pdf_count": raw.get("pdf_count", 0),
            "language": "fr",
            "jurisdiction": "Senegal",
        }


# ── CLI ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="Senegal Primature legislation scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap")
    args = parser.parse_args()

    scraper = PrimatureLegislationScraper()

    if args.command == "test":
        print("Testing connectivity to primature.sn...")
        try:
            resp = scraper.session.get(f"{BASE_URL}/publications/lois-et-reglements", timeout=15)
            print(f"OK: HTTP {resp.status_code}, {len(resp.text)} bytes")
            links = scraper._get_listing_links("codes")
            print(f"Found {len(links)} code documents")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            logger.info("Running sample bootstrap...")
            records = []
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                if record and record.get("text"):
                    records.append(record)
                    logger.info(f"  [{len(records)}] {record['title'][:60]}... ({len(record['text'])} chars)")
                    if len(records) >= 15:
                        break

            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            for i, rec in enumerate(records):
                path = sample_dir / f"{i+1:03d}_{rec['_id'][:60]}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")
            for rec in records:
                text_len = len(rec.get("text", ""))
                print(f"  {rec['_id']}: {rec['title'][:70]} ({text_len} chars)")
        else:
            logger.info("Running full bootstrap...")
            stats = scraper.bootstrap(sample_mode=False)
            print(json.dumps(stats, indent=2))

    elif args.command == "update":
        logger.info("Running update (re-fetches all)...")
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2))
