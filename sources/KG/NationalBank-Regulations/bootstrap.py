#!/usr/bin/env python3
"""
KG/NationalBank-Regulations — National Bank of the Kyrgyz Republic

Fetches normative acts (banking supervision and monetary policy regulations)
and banking laws from the NBKR website (nbkr.kg).

Strategy:
  1. Scrape index pages for document links (contout.jsp and index1.jsp)
  2. Fetch each detail page and extract full text from HTML
  3. Handle both English and Russian content

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental (re-fetches all — no date API)
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional
from urllib.parse import urljoin, parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KG.NationalBank-Regulations")

BASE_URL = "https://www.nbkr.kg"
SOURCE_ID = "KG/NationalBank-Regulations"

# Index pages to scrape for document links
INDEX_PAGES = [
    {
        "url": "/index1.jsp?item=101&lang=ENG",
        "category": "Banking Supervision Regulations",
        "language": "en",
        "link_pattern": "contout.jsp",
    },
    {
        "url": "/index1.jsp?item=101&lang=RUS",
        "category": "Monetary Policy Regulations",
        "language": "ru",
        "link_pattern": "contout.jsp",
    },
]

# Individual law pages (each is a full document)
LAW_PAGES = [
    {"url": "/index1.jsp?item=41&lang=ENG", "title": "Constitutional Law on the National Bank", "language": "en"},
    {"url": "/index1.jsp?item=42&lang=ENG", "title": "Law on Banks and Banking", "language": "en"},
    {"url": "/index1.jsp?item=45&lang=ENG", "title": "Law on Economic Partnership and Cooperation", "language": "en"},
    {"url": "/index1.jsp?item=47&lang=ENG", "title": "Law on Joint-Stock Companies", "language": "en"},
    {"url": "/index1.jsp?item=48&lang=ENG", "title": "Law on Licensing", "language": "en"},
    {"url": "/index1.jsp?item=49&lang=ENG", "title": "Law on Credit Unions", "language": "en"},
    {"url": "/index1.jsp?item=50&lang=ENG", "title": "Law on Micro-Financing Organizations", "language": "en"},
    {"url": "/index1.jsp?item=52&lang=ENG", "title": "Law on Accountancy", "language": "en"},
    {"url": "/index1.jsp?item=53&lang=ENG", "title": "Law on Letter of Credit", "language": "en"},
    {"url": "/index1.jsp?item=54&lang=ENG", "title": "Law on Collateral", "language": "en"},
    {"url": "/index1.jsp?item=55&lang=ENG", "title": "Law on Auditing Activity", "language": "en"},
    {"url": "/index1.jsp?item=1430&lang=ENG", "title": "Law on Prevention of Terrorism Financing", "language": "en"},
    {"url": "/index1.jsp?item=1200&lang=ENG", "title": "Law on Protection of Bank Deposits", "language": "en"},
    {"url": "/index1.jsp?item=2725&lang=ENG", "title": "Law on Payment System", "language": "en"},
]


def _clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?div[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?h[1-6][^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?tr[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?td[^>]*>', ' | ', text, flags=re.IGNORECASE)
    text = re.sub(r'</?li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_main_content(html: str) -> Optional[str]:
    """Extract main content from NBKR page, skipping navigation."""
    # Try to find the main content area
    # NBKR pages use various patterns — try several approaches

    # Pattern 1: content inside contentcell or similar
    m = re.search(
        r'<td[^>]*class="[^"]*contentcell[^"]*"[^>]*>(.*?)</td>\s*</tr>\s*</table>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if m:
        text = _clean_html(m.group(1))
        if len(text) > 200:
            return text

    # Pattern 2: Look for the main document text after navigation
    # NBKR pages typically have document content after the menu section
    m = re.search(
        r'(?:<td[^>]*width="[567]\d{2}"[^>]*>|<td[^>]*class="[^"]*text[^"]*"[^>]*>)(.*?)(?:</td>\s*</tr>\s*</table>)',
        html, re.DOTALL | re.IGNORECASE,
    )
    if m:
        text = _clean_html(m.group(1))
        if len(text) > 200:
            return text

    # Pattern 3: Find the largest text block in the page
    # Split by major table cells and find the one with most content
    cells = re.findall(r'<td[^>]*>(.*?)</td>', html, re.DOTALL | re.IGNORECASE)
    best = ""
    for cell in cells:
        cleaned = _clean_html(cell)
        if len(cleaned) > len(best):
            best = cleaned

    if len(best) > 2000:
        return best

    # Pattern 4: Fallback — clean entire body, strip nav/menu/footer
    m = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL | re.IGNORECASE)
    if m:
        body = m.group(1)
        # Strip navigation menu lists (ul/li blocks with many links)
        body = re.sub(
            r'<ul[^>]*>(?:.*?<li[^>]*>.*?<a[^>]*>.*?</a>.*?</li>.*?){5,}</ul>',
            '', body, flags=re.DOTALL | re.IGNORECASE,
        )
        text = _clean_html(body)
        # Remove common nav/header/footer text patterns
        for noise in [
            r'(?:Главная|Home)\s*›.*?\n',
            r'^.*?(?:NATIONAL BANK OF\s*THE KYRGYZ REPUBLIC|НАЦИОНАЛЬНЫЙ БАНК\s*КЫРГЫЗСКОЙ РЕСПУБЛИКИ)\s*\n',
            r'- \s*About the Bank\s*\n.*?(?=\n(?:Appendix|Regulation|Instruction|Law|Constitutional|PROCEDURE|Recommendations))',
            r'- \s*О Нацбанке\s*\n.*?(?=\n(?:Приложение|Положение|Инструкция|Закон|Правила|ПОРЯДОК|Рекомендации))',
        ]:
            text = re.sub(noise, '', text, flags=re.DOTALL)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        if len(text) > 200:
            return text

    return None


class NBKRScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html, */*",
                "Accept-Language": "ru-RU,ru;q=0.9,ky;q=0.8,en;q=0.7",
            },
            timeout=60,
        )
        self._seen_materials = set()

    def _scrape_index_for_links(self, index_url: str) -> list[dict]:
        """Scrape an index page for contout.jsp document links."""
        full_url = f"{BASE_URL}{index_url}"
        logger.info("Scraping index: %s", full_url)

        resp = self.http.get(full_url, timeout=60)
        if resp.status_code != 200:
            logger.warning("Failed to fetch %s: HTTP %d", full_url, resp.status_code)
            return []

        html = resp.text
        links = []

        # Find all contout.jsp links with material IDs
        pattern = r"""href=["'](contout\.jsp\?[^"']*material=(\d+)[^"']*)["'][^>]*>(.*?)(?:</a>|</span>)"""
        for href, material_id, label in re.findall(pattern, html, re.DOTALL | re.IGNORECASE):
            if material_id in self._seen_materials:
                continue
            self._seen_materials.add(material_id)

            clean_label = unescape(re.sub(r'<[^>]+>', '', label).strip())
            if not clean_label or len(clean_label) < 5:
                continue

            full_href = urljoin(full_url, href)
            links.append({
                "url": full_href,
                "material_id": material_id,
                "title": clean_label,
            })

        return links

    def _fetch_document_text(self, url: str) -> Optional[str]:
        """Fetch a detail page and extract full text."""
        try:
            resp = self.http.get(url, timeout=90)
            if resp.status_code != 200:
                logger.warning("Detail page failed (%d): %s", resp.status_code, url)
                return None
            return _extract_main_content(resp.text)
        except Exception as e:
            logger.warning("Detail page error for %s: %s", url, e)
        return None

    def _build_record(self, item: dict, category: str, language: str, doc_type: str) -> Optional[dict]:
        """Fetch content and build a normalized record."""
        text = self._fetch_document_text(item["url"])
        if not text:
            logger.warning("No text extracted: %s", item["title"][:60])
            return None

        material_id = item.get("material_id", "")
        if material_id:
            doc_id = f"nbkr-{material_id}"
        else:
            parsed = urlparse(item["url"])
            params = parse_qs(parsed.query)
            item_id = params.get("item", ["unknown"])[0]
            doc_id = f"nbkr-law-{item_id}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item["title"],
            "text": text,
            "date": None,
            "url": item["url"],
            "material_id": material_id,
            "category": category,
            "language": language,
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all NBKR regulatory documents."""
        self._seen_materials = set()

        # 1. Normative acts from index pages
        for idx_page in INDEX_PAGES:
            links = self._scrape_index_for_links(idx_page["url"])
            logger.info("%s: %d documents found", idx_page["category"], len(links))
            time.sleep(1)

            for link in links:
                record = self._build_record(
                    link,
                    category=idx_page["category"],
                    language=idx_page["language"],
                    doc_type="doctrine",
                )
                if record:
                    yield record
                time.sleep(1.5)

        # 2. Banking laws
        for law in LAW_PAGES:
            law_item = {
                "url": f"{BASE_URL}{law['url']}",
                "material_id": "",
                "title": law["title"],
            }
            record = self._build_record(
                law_item,
                category="Banking Laws",
                language=law["language"],
                doc_type="legislation",
            )
            if record:
                yield record
            time.sleep(1.5)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="KG/NationalBank-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NBKRScraper()

    if args.command == "test-api":
        for idx_page in INDEX_PAGES:
            links = scraper._scrape_index_for_links(idx_page["url"])
            logger.info("%s: %d document links found", idx_page["category"], len(links))
            time.sleep(1)
        logger.info("Law pages configured: %d", len(LAW_PAGES))
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars (%s)",
                count,
                record["title"][:60],
                len(record.get("text", "")),
                record["language"],
            )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
