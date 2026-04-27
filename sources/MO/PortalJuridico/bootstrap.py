#!/usr/bin/env python3
"""
MO/PortalJuridico -- Macau Legal Portal (Portal Jurídico / 法律資料庫)

Fetches consolidated laws (Leis) from the Macau Official Gazette portal.
The listing at /pt/legis/list/a/?d=46 contains ~769 laws with full HTML text.

Strategy:
  - Paginate through /pt/legis/list/a/?d=46&p=N (100 per page, ~8 pages)
  - Extract link IDs and metadata from table rows
  - Follow redirect chain /pt/bo/a/link/{ID} → final .asp page
  - Decode detail pages as Windows-1252 (declared in meta charset)
  - Extract content from <div id='content'> after the metadata ficha table

Usage:
  python bootstrap.py bootstrap          # Full pull (~769 laws)
  python bootstrap.py bootstrap --sample # 12 sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MO.PortalJuridico")

BASE_URL = "https://www.bo.dsaj.gov.mo"
LIST_PATH = "/pt/legis/list/a/?d=46"

# Date in gazette reference: YYYY/MM/DD
DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")

# Gazette reference pattern
GAZETTE_RE = re.compile(
    r"B\.O\.\s*n\.º:\s*(\d+),\s*(I+\s+S[eé]rie),\s*(\d{4}/\d{2}/\d{2})",
    re.IGNORECASE,
)


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<h[2-6][^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</h[2-6]>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_body(text: str) -> str:
    """Remove boilerplate lines from extracted text."""
    skip_patterns = [
        "Legislação relacionada",
        "Categorias relacionadas",
        "LegisMac",
        "PDF original do B.O.",
        "A A A",
        "Português",
        "Chinês",
        "Adobe Reader",
        "Get Adobe",
        "Google tag",
        "gtag.js",
        "BOLETIM OFICIAL",
        "document.querySelector",
        "window.dataLayer",
        "function(",
    ]
    lines = text.split("\n")
    filtered = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if filtered and filtered[-1] != "":
                filtered.append("")
            continue
        if any(pat in stripped for pat in skip_patterns):
            continue
        if stripped.startswith("//") and len(stripped) < 50:
            continue
        filtered.append(stripped)
    return "\n".join(filtered).strip()


class MOPortalJuridicoScraper(BaseScraper):
    """Scraper for MO/PortalJuridico -- Macau consolidated laws."""

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
        """Parse a listing page and extract law entries."""
        url = f"{LIST_PATH}&p={page}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch listing page {page}: {e}")
            return []

        html = resp.text
        results = []

        rows = re.findall(
            r'<tr[^>]*>\s*<td\s+class="col-md-9">(.*?)</td>\s*'
            r"<td\s[^>]*>(.*?)</td>\s*</tr>",
            html,
            re.DOTALL,
        )

        for col1, col2 in rows:
            link_match = re.search(
                r'href="/pt/bo/a/link/(\d+)"[^>]*>([^<]+)</a>', col1
            )
            if not link_match:
                continue

            link_id = link_match.group(1)
            diploma = unescape(link_match.group(2)).strip()

            # Description after the link
            desc_part = col1[col1.find("</a>") + 4 :] if "</a>" in col1 else ""
            desc = re.sub(r"<[^>]+>", "", desc_part).strip()
            desc = re.sub(r"^,\s*", "", desc).strip()

            entry: Dict[str, Any] = {
                "link_id": link_id,
                "diploma": diploma,
                "description": desc,
                "title": f"{diploma}, {desc}" if desc else diploma,
            }

            # Extract gazette info and date
            gaz_match = GAZETTE_RE.search(col2)
            if gaz_match:
                entry["gazette_number"] = gaz_match.group(1)
                date_str = gaz_match.group(3)
                dm = DATE_RE.match(date_str)
                if dm:
                    entry["date"] = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                entry["gazette_info"] = (
                    f"B.O. n.º {gaz_match.group(1)}, {gaz_match.group(2).strip()}, {date_str}"
                )
            else:
                dm = DATE_RE.search(col2)
                if dm:
                    entry["date"] = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                entry["gazette_info"] = re.sub(r"<[^>]+>", "", col2).strip()

            results.append(entry)

        return results

    def _fetch_detail(self, link_id: str) -> str:
        """Follow redirect chain and extract full text from the detail page."""
        url = f"/pt/bo/a/link/{link_id}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url, allow_redirects=True)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch detail for link {link_id}: {e}")
            return ""

        # Handle Windows-1252 encoding (detail pages use it)
        raw = resp.content
        meta_match = re.search(rb"charset=([\w-]+)", raw[:3000])
        if meta_match:
            charset = meta_match.group(1).decode("ascii", errors="replace")
            if "1252" in charset or "iso-8859" in charset.lower():
                html = raw.decode(charset, errors="replace")
            else:
                html = raw.decode("utf-8", errors="replace")
        else:
            html = resp.text

        # Find the content div
        content_start = html.find("id='content'")
        if content_start < 0:
            content_start = html.find('id="content"')
        if content_start < 0:
            return ""

        content = html[content_start:]

        # Find the first non-ALL-CAPS h2 (the law title, not section headers)
        h2s = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", content, re.DOTALL))
        body_start = None
        for m in h2s:
            title = m.group(1).strip()
            if not title.isupper() or len(title) < 5:
                body_start = m.start()
                break

        if body_start is None:
            # Fallback: use first h2
            if h2s:
                body_start = h2s[0].start()
            else:
                return ""

        body_html = content[body_start:]

        # Trim at script/footer markers
        for end_marker in ["<script", "<!-- Google", "<!-- RODAPE", "</body"]:
            eidx = body_html.find(end_marker)
            if eidx > 0:
                body_html = body_html[:eidx]
                break

        # Remove any embedded ficha tables
        body_html = re.sub(
            r"<table[^>]*class=['\"]ficha['\"][^>]*>.*?</table>",
            "",
            body_html,
            flags=re.DOTALL,
        )

        text = strip_html(body_html)
        text = clean_body(text)
        return text

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        link_id = raw.get("link_id", "")
        doc_id = f"mo-pj-{link_id}"

        return {
            "_id": f"MO/PortalJuridico/{doc_id}",
            "_source": "MO/PortalJuridico",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": f"{BASE_URL}/pt/bo/a/link/{link_id}",
            "doc_id": doc_id,
            "diploma": raw.get("diploma"),
            "gazette_info": raw.get("gazette_info"),
            "publication_date": raw.get("date"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        max_pages = 2 if sample else 10
        limit = 12 if sample else None
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
                title = entry.get("title", "?")[:60]
                logger.info(f"  [{count + 1}] Fetching: {title}")

                text = self._fetch_detail(link_id)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  Insufficient text for link {link_id} ({len(text)} chars)")
                    continue

                entry["text"] = text
                yield entry
                count += 1

        logger.info(f"Fetched {count} laws total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent laws from the first few listing pages."""
        for page in range(1, 3):
            logger.info(f"Fetching updates page {page}...")
            entries = self._parse_listing_page(page)

            for entry in entries:
                date = entry.get("date", "")
                if date and date < since:
                    return

                link_id = entry["link_id"]
                text = self._fetch_detail(link_id)
                if not text or len(text.strip()) < 50:
                    continue

                entry["text"] = text
                yield entry


if __name__ == "__main__":
    scraper = MOPortalJuridicoScraper()

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
