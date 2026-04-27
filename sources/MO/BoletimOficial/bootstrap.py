#!/usr/bin/env python3
"""
MO/BoletimOficial -- Macau Official Gazette (Boletim Oficial)

Fetches legislation from bo.dsaj.gov.mo. The site has a paginated listing of
45,000+ legislation items from 1872-present, with detail pages containing full
text in HTML (Portuguese edition, ISO-8859-1).

Strategy:
  - Paginate through /pt/legis/list/a/?p=N (100 items per page, 455+ pages)
  - Extract link IDs and metadata (title, gazette issue, date) from each page
  - Fetch full text from /pt/bo/a/link/{ID}
  - Detail pages may contain multiple documents from the same gazette issue;
    isolate the specific document by matching title against <h2> headings

Listing HTML structure:
  <tr>
    <td class="col-md-9">
      <a href="/pt/bo/a/link/46557">Despacho do Chefe do Executivo n.º 72/2026</a>,
      Aprova o Regulamento do Parque de Estacionamento...
    </td>
    <td class="col-md-3 text-right text-nowrap">
      <a href="...">B.O. n.º: 15, I Série, 2026/04/13</a>
    </td>
  </tr>

Detail page:
  Multiple <h2> headings per page (one per document in gazette issue).
  Content between consecutive <h2> tags belongs to each document.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MO.BoletimOficial")

BASE_URL = "https://www.bo.dsaj.gov.mo"

# Extract link ID and title from listing rows
LISTING_LINK_RE = re.compile(
    r'<a\s+href="/pt/bo/a/link/(\d+)"[^>]*>([^<]+)</a>\s*,?\s*([^<]*)',
    re.DOTALL,
)

# Date pattern: YYYY/MM/DD in gazette ref
DATE_RE = re.compile(r'(\d{4})/(\d{2})/(\d{2})')

# Extract gazette reference from second cell
GAZETTE_RE = re.compile(
    r'B\.O\.\s*n\.º:\s*(\d+),\s*(I+\s+S[eé]rie),\s*(\d{4}/\d{2}/\d{2})',
    re.IGNORECASE,
)


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def normalize_title(title: str) -> str:
    """Normalize a title for comparison (lowercase, strip accents, collapse spaces)."""
    t = unescape(title).strip().lower()
    t = re.sub(r'\s+', ' ', t)
    t = t.replace('\u00ba', '').replace('\u00aa', '')  # º ª
    t = t.replace('.', '').replace(',', '')
    return t


class MOBoletimOficialScraper(BaseScraper):
    """Scraper for MO/BoletimOficial -- Macau Official Gazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _parse_listing_page(self, page: int) -> List[Dict[str, Any]]:
        """Parse a listing page and extract entries."""
        url = f"/pt/legis/list/a/?p={page}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch listing page {page}: {e}")
            return []

        html = resp.text
        results = []

        # Parse table rows
        rows = re.findall(
            r'<tr[^>]*>\s*<td\s+class="col-md-9">(.*?)</td>\s*'
            r'<td\s[^>]*>(.*?)</td>\s*</tr>',
            html,
            re.DOTALL,
        )

        for col1, col2 in rows:
            link_match = re.search(r'href="/pt/bo/a/link/(\d+)"[^>]*>([^<]+)</a>', col1)
            if not link_match:
                continue

            link_id = link_match.group(1)
            doc_ref = unescape(link_match.group(2)).strip()

            # Description is everything after the </a> tag, cleaned
            desc_part = col1[col1.find('</a>') + 4:] if '</a>' in col1 else ''
            desc = re.sub(r'<[^>]+>', '', desc_part).strip()
            desc = re.sub(r'^,\s*', '', desc).strip()

            entry: Dict[str, Any] = {
                "link_id": link_id,
                "doc_ref": doc_ref,
                "description": desc,
                "title": f"{doc_ref}, {desc}" if desc else doc_ref,
            }

            # Extract gazette info and date from second cell
            gaz_match = GAZETTE_RE.search(col2)
            if gaz_match:
                entry["gazette_number"] = gaz_match.group(1)
                entry["series"] = gaz_match.group(2).strip()
                date_str = gaz_match.group(3)
                dm = DATE_RE.match(date_str)
                if dm:
                    entry["date"] = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                entry["gazette_info"] = f"B.O. n.º {gaz_match.group(1)}, {gaz_match.group(2)}, {date_str}"
            else:
                # Fallback date extraction
                dm = DATE_RE.search(col2)
                if dm:
                    entry["date"] = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                entry["gazette_info"] = re.sub(r'<[^>]+>', '', col2).strip()

            results.append(entry)

        return results

    def _extract_document_text(self, link_id: str, doc_ref: str) -> str:
        """Fetch a detail page and extract the specific document's full text.

        Detail pages may contain multiple documents from the same gazette issue.
        We isolate the target document by matching its title against <h2> headings.
        """
        url = f"/pt/bo/a/link/{link_id}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch detail for link {link_id}: {e}")
            return ""

        html = resp.text

        # Find all h2 headings and their positions
        h2_matches = list(re.finditer(r'<h2>([^<]+)</h2>', html))
        if not h2_matches:
            return ""

        # If only one content h2, extract everything after it
        # First h2 is usually the issuing body (e.g., "GABINETE DO CHEFE DO EXECUTIVO")
        content_h2s = [m for m in h2_matches if not self._is_header_h2(m.group(1))]

        if not content_h2s:
            # All h2s are headers; just use everything
            content_h2s = h2_matches

        target_idx = None
        norm_ref = normalize_title(doc_ref)

        # Find the h2 that matches our target document
        for i, m in enumerate(content_h2s):
            if normalize_title(m.group(1)) == norm_ref:
                target_idx = i
                break

        # Fallback: partial match
        if target_idx is None:
            # Try matching the document number (e.g., "72/2026")
            num_match = re.search(r'n\.?\s*[ºª]?\s*(\d+/\d{4})', doc_ref)
            if num_match:
                num = num_match.group(1)
                for i, m in enumerate(content_h2s):
                    if num in m.group(1):
                        target_idx = i
                        break

        if target_idx is None:
            # If single document page, take the last content h2
            if len(content_h2s) == 1:
                target_idx = 0
            else:
                # Default to last h2 (link IDs often map to last doc on page)
                target_idx = len(content_h2s) - 1

        # Extract text between target h2 and the next h2 (or end of content)
        target_h2 = content_h2s[target_idx]
        start = target_h2.end()

        # Find end boundary
        if target_idx + 1 < len(content_h2s):
            end = content_h2s[target_idx + 1].start()
        else:
            # End at script tags or body end
            script_idx = html.find('<script', start)
            end = script_idx if script_idx > 0 else len(html)

        doc_html = html[start:end]
        text = strip_html(doc_html)

        # Remove navigation/boilerplate lines
        lines = text.split('\n')
        filtered = []
        skip_patterns = [
            'Legislação relacionada', 'Categorias relacionadas',
            'LegisMac', 'PDF original do B.O.',
            'A A A', 'Português', 'Chinês',
            'Adobe Reader', 'Get Adobe',
            'Google tag', 'gtag.js',
            'BOLETIM OFICIAL', 'document.querySelector',
        ]
        for line in lines:
            stripped = line.strip()
            if not stripped:
                if filtered and filtered[-1] != '':
                    filtered.append('')
                continue
            if any(pat in stripped for pat in skip_patterns):
                continue
            if stripped.startswith('//') and len(stripped) < 50:
                continue
            filtered.append(stripped)

        return '\n'.join(filtered).strip()

    def _is_header_h2(self, title: str) -> bool:
        """Check if an h2 is a section header (issuing body) rather than a document title."""
        title = title.strip()
        # Section headers are typically in ALL CAPS or are short org names
        if title.isupper() and len(title) > 3:
            return True
        # Common section headers
        headers = [
            'GABINETE DO CHEFE DO EXECUTIVO',
            'REGIÃO ADMINISTRATIVA ESPECIAL DE MACAU',
            'ASSEMBLEIA LEGISLATIVA',
        ]
        for h in headers:
            if h in title.upper():
                return True
        return False

    def _make_doc_id(self, link_id: str) -> str:
        return f"mo-bo-{link_id}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        link_id = raw.get("link_id", "")
        doc_id = self._make_doc_id(link_id)

        return {
            "_id": f"MO/BoletimOficial/{doc_id}",
            "_source": "MO/BoletimOficial",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": f"{BASE_URL}/pt/bo/a/link/{link_id}",
            "doc_id": doc_id,
            "doc_ref": raw.get("doc_ref"),
            "gazette_info": raw.get("gazette_info"),
            "publication_date": raw.get("date"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        max_pages = 2 if sample else 500
        limit = 15 if sample else None
        count = 0

        for page in range(1, max_pages + 1):
            if limit and count >= limit:
                break

            logger.info(f"Fetching listing page {page}...")
            entries = self._parse_listing_page(page)
            logger.info(f"  Found {len(entries)} entries on page {page}")

            if not entries:
                break

            for entry in entries:
                if limit and count >= limit:
                    break

                link_id = entry["link_id"]
                doc_ref = entry.get("doc_ref", "")
                title = entry.get("title", "?")[:60]
                logger.info(f"  [{count + 1}] Fetching: {title}")

                text = self._extract_document_text(link_id, doc_ref)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  No meaningful text for link {link_id}")
                    continue

                entry["text"] = text
                yield entry
                count += 1

        logger.info(f"Fetched {count} legislation items total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent items from the first few listing pages."""
        for page in range(1, 4):
            logger.info(f"Fetching updates page {page}...")
            entries = self._parse_listing_page(page)

            for entry in entries:
                date = entry.get("date", "")
                if date and date < since:
                    return

                link_id = entry["link_id"]
                doc_ref = entry.get("doc_ref", "")
                text = self._extract_document_text(link_id, doc_ref)
                if not text or len(text.strip()) < 50:
                    continue

                entry["text"] = text
                yield entry


if __name__ == "__main__":
    scraper = MOBoletimOficialScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
