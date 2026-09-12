#!/usr/bin/env python3
"""
CV/TribunalSupremo - Cabo Verde Supreme Court Decisions

Fetches court decisions (acórdãos) from stj.cv. Decisions are published
as PDFs via WordPress Download Manager. Text extracted via pypdf.

Data source: https://www.stj.cv/
License: Open government data (official court decisions)
"""

import argparse
import hashlib
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pypdf
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.stj.cv/"
SOURCE_ID = "CV/TribunalSupremo"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Section pages with direct PDF links
SECTION_PAGES = [
    ("https://www.stj.cv/index.php/1a-sec-civel/", "civil"),
    ("https://www.stj.cv/index.php/3a-sec-adm-fisc-ad/", "administrative_fiscal"),
]

# Main acordaos page with WPDM download links (includes criminal section)
ACORDAOS_PAGE = "https://www.stj.cv/index.php/acordaos/"


class CaboVerdeSTJFetcher:
    """Fetcher for Cabo Verde Supreme Court decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })
        self._seen_urls = set()

    def get_direct_pdf_links(self, section_url: str) -> List[Dict[str, str]]:
        """Get direct PDF links from a section page."""
        try:
            resp = self.session.get(section_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {section_url}: {e}")
            return []

        entries = []
        for match in re.finditer(r'<a[^>]*href="([^"]*\.pdf)"[^>]*>([^<]*)</a>', resp.text):
            pdf_url = match.group(1)
            link_text = match.group(2).strip()

            if pdf_url in self._seen_urls:
                continue
            self._seen_urls.add(pdf_url)

            if link_text:
                title = link_text
            else:
                filename = pdf_url.split('/')[-1].replace('.pdf', '').replace('.PDF', '')
                title = filename.replace('-', ' ').replace('_', ' ')

            if not pdf_url.startswith('http'):
                from urllib.parse import urljoin
                pdf_url = urljoin(section_url, pdf_url)

            entries.append({"url": pdf_url, "title": title, "download_url": pdf_url})

        logger.info(f"Found {len(entries)} direct PDFs from {section_url.split('/')[-2]}")
        return entries

    def get_wpdm_entries(self) -> List[Dict[str, str]]:
        """Get decision entries from the main acordaos page (WPDM download links)."""
        try:
            resp = self.session.get(ACORDAOS_PAGE, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {ACORDAOS_PAGE}: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        entries = []

        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/download/' not in href or 'acordao' not in href.lower():
                continue
            if href == '#':
                continue
            text = a.get_text(strip=True)
            if not text or text == 'Download':
                continue
            if href in self._seen_urls:
                continue
            self._seen_urls.add(href)
            entries.append({"url": href, "title": text, "download_url": None})

        logger.info(f"Found {len(entries)} WPDM entries from acordaos page")
        return entries

    def resolve_wpdm_download(self, page_url: str) -> Optional[str]:
        """Visit a WPDM download page and extract the actual PDF download URL."""
        try:
            resp = self.session.get(page_url, timeout=30)
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, 'html.parser')
            elem = soup.find(attrs={"data-downloadurl": True})
            if elem:
                return elem["data-downloadurl"]
        except requests.RequestException as e:
            logger.warning(f"Failed to resolve WPDM download for {page_url}: {e}")
        return None

    def download_pdf_text(self, url: str) -> Optional[str]:
        """Download PDF and extract text."""
        try:
            resp = self.session.get(url, timeout=120)
            if resp.status_code != 200:
                return None
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type and resp.content[:4] != b'%PDF':
                return None
            if len(resp.content) < 1000:
                return None

            reader = pypdf.PdfReader(io.BytesIO(resp.content))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)

            full_text = "\n\n".join(pages_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            return full_text if len(full_text) > 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def extract_metadata(self, title: str, url: str) -> Dict[str, Any]:
        """Extract year, case number, and section from title/URL."""
        # Year
        year = None
        match = re.search(r'(\d{1,3})[-/](\d{4})', title)
        if match:
            year = int(match.group(2))
        if not year:
            match = re.search(r'(\d{4})', url.split('/')[-1] if '/' in url else url)
            if match:
                y = int(match.group(1))
                if 2000 <= y <= 2030:
                    year = y

        # Case number
        case_num = None
        match = re.search(r'Acord[aã]o[\s\-]*(\d+[-/]\d{4})', title, re.IGNORECASE)
        if match:
            case_num = match.group(1).replace('-', '/')
        elif re.search(r'(\d+)[-/](\d{4})', title):
            m = re.search(r'(\d+)[-/](\d{4})', title)
            case_num = f"{m.group(1)}/{m.group(2)}"

        # Section from title keywords
        section = "general"
        title_lower = title.lower()
        if any(k in title_lower for k in ['penal', 'criminal', 'homicid', 'roubo', 'furto', 'droga']):
            section = "criminal"
        elif any(k in title_lower for k in ['cível', 'civel', 'divórcio', 'divorcio', 'contrato', 'posse', 'alimento']):
            section = "civil"
        elif any(k in title_lower for k in ['admin', 'fiscal', 'tributár', 'tributar', 'suspensão', 'disciplin']):
            section = "administrative_fiscal"

        return {"year": year, "case_number": case_num, "section": section}

    def normalize(self, entry: Dict[str, str], text: str, section_override: Optional[str] = None) -> Dict[str, Any]:
        """Normalize into standard schema."""
        doc_id = hashlib.sha256(entry["url"].encode()).hexdigest()[:16]
        meta = self.extract_metadata(entry["title"], entry["url"])
        if section_override:
            meta["section"] = section_override

        year = meta["year"]
        date = f"{year}-01-01" if year else None

        # Clean HTML entities from title
        title = entry["title"].replace('&#8211;', '–').replace('&#8212;', '—')
        title = re.sub(r'&#\d+;', '', title)

        return {
            "_id": f"CV-STJ-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": entry["url"],
            "case_number": meta["case_number"],
            "section": meta["section"],
            "year": year,
            "country": "CV",
            "language": "pt",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all decisions with full text."""
        # First: section pages (direct PDF links)
        for section_url, section_name in SECTION_PAGES:
            entries = self.get_direct_pdf_links(section_url)
            for i, entry in enumerate(entries):
                logger.info(f"  [{i+1}/{len(entries)}] {entry['title'][:50]}")
                time.sleep(1.5)
                text = self.download_pdf_text(entry["download_url"])
                if text:
                    yield self.normalize(entry, text, section_override=section_name)
                else:
                    logger.warning(f"    No text extracted")

        # Second: main acordaos page (WPDM links, captures criminal etc.)
        entries = self.get_wpdm_entries()
        for i, entry in enumerate(entries):
            logger.info(f"  [WPDM {i+1}/{len(entries)}] {entry['title'][:50]}")
            time.sleep(1.5)
            dl_url = self.resolve_wpdm_download(entry["url"])
            if not dl_url:
                logger.warning(f"    Could not resolve download URL")
                continue
            time.sleep(1.0)
            text = self.download_pdf_text(dl_url)
            if text:
                yield self.normalize(entry, text)
            else:
                logger.warning(f"    No text extracted")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample across all sections."""
        records = []

        # Get some from each section page
        for section_url, section_name in SECTION_PAGES:
            if len(records) >= max_records:
                break
            entries = self.get_direct_pdf_links(section_url)
            for entry in entries[:5]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                text = self.download_pdf_text(entry["download_url"])
                if text:
                    record = self.normalize(entry, text, section_override=section_name)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {entry['title'][:50]} ({len(text)} chars)")

        # Get some from WPDM (criminal section)
        if len(records) < max_records:
            wpdm_entries = self.get_wpdm_entries()
            # Filter for criminal ones
            criminal = [e for e in wpdm_entries if any(k in e['title'].lower() for k in ['penal', 'criminal', 'homicid'])]
            if not criminal:
                criminal = wpdm_entries[:5]
            for entry in criminal[:max_records - len(records)]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                dl_url = self.resolve_wpdm_download(entry["url"])
                if not dl_url:
                    continue
                time.sleep(1.0)
                text = self.download_pdf_text(dl_url)
                if text:
                    record = self.normalize(entry, text)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {entry['title'][:50]} ({len(text)} chars)")

        return records


def bootstrap_sample():
    """Run sample mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = CaboVerdeSTJFetcher()
    records = fetcher.fetch_sample(max_records=15)

    for i, record in enumerate(records):
        outfile = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
    texts = [r.get("text", "") for r in records]
    non_empty = sum(1 for t in texts if len(t) > 100)
    avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
    logger.info(f"Validation: {non_empty}/{len(records)} records have text (avg {avg_len:.0f} chars)")
    return records


def bootstrap_full():
    """Run full mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = CaboVerdeSTJFetcher()
    count = 0
    for record in fetcher.fetch_all():
        outfile = SAMPLE_DIR / f"record_{count+1:04d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
    logger.info(f"Complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="CV/TribunalSupremo Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
