#!/usr/bin/env python3
"""
IQ/CBI-Regulations — Central Bank of Iraq Regulations & Circulars

Fetches circulars, guidelines, and standards from the Central Bank of Iraq.

Strategy:
  1. Scrape paginated circulars from /news/section/77 (path-based: /77/2, /77/3 etc.)
  2. For each circular, fetch the detail page to get title, date, and PDF URL(s)
  3. Only keep circulars that have PDF attachments (real regulatory documents)
  4. Download each PDF and extract text with pdfplumber
  5. Also fetch guidelines/standards PDFs from /page/99

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (by date)
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IQ.CBI-Regulations")

BASE_URL = "https://cbi.iq"
SOURCE_ID = "IQ/CBI-Regulations"

MONTH_MAP = {
    "January": "01", "February": "02", "March": "03", "April": "04",
    "May": "05", "June": "06", "July": "07", "August": "08",
    "September": "09", "October": "10", "November": "11", "December": "12",
}

MIN_TEXT_LENGTH = 200


class CBIRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open legal data research)",
                "Accept": "text/html, application/xhtml+xml, */*",
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            },
        )

    # ── Helpers ───────────────────────────────────────────────────────

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse 'Month DD, YYYY' to ISO date."""
        date_str = date_str.strip().rstrip(",")
        m = re.match(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", date_str)
        if m:
            month_name, day, year = m.groups()
            month_num = MONTH_MAP.get(month_name)
            if month_num:
                return f"{year}-{month_num}-{int(day):02d}"
        return None

    def _is_captcha(self, html: str) -> bool:
        return "captcha" in html.lower() or "validation request" in html.lower()

    # ── PDF text extraction ───────────────────────────────────────────

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> Optional[str]:
        """Download a PDF and extract text through the shared helper.

        This used to call pdfplumber directly, which emits the glyphs in the
        order the content stream lists them. For the Arabic circulars that is
        VISUAL order, so every word landed character-reversed in Neon and no
        Arabic keyword could ever match it (issue #1560). Going through
        common.pdf_extract picks up the geometry-based RTL reorder and the
        presentation-form normalization instead.

        force=True because the rows this is meant to replace are exactly the
        ones already in Neon with (reversed) text — without it the helper skips
        them and the refresh emits nothing.
        """
        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            if not resp.content[:5] == b"%PDF-":
                logger.warning("Not a PDF: %s", pdf_url)
                return None
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=source_id,
                pdf_bytes=resp.content,
                table="doctrine",
                force=True,
            )
            if text and len(text.strip()) > MIN_TEXT_LENGTH:
                return text.strip()
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", pdf_url, e)
        return None

    # ── Listing page parsing ──────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict]:
        """Parse a circulars listing page and return list of items with id, title, date."""
        soup = BeautifulSoup(html, "html.parser")
        items = []
        seen_ids = set()

        for a in soup.find_all("a", href=lambda h: h and "/news/view/" in h and "&t=" not in h):
            href = a.get("href", "")
            m = re.search(r"/news/view/(\d+)", href)
            if not m:
                continue
            nid = m.group(1)
            if nid in seen_ids or nid == "94":  # skip sidebar links
                continue

            title = (a.get("title") or a.get_text(strip=True)).strip()
            if not title or title == "إقرأ المزيد":
                continue

            seen_ids.add(nid)

            # Walk up to find date in parent card
            date_text = None
            parent = a.parent
            for _ in range(6):
                if parent is None:
                    break
                date_el = parent.find(class_="date")
                if date_el:
                    date_text = self._parse_date(date_el.get_text(strip=True))
                    break
                parent = parent.parent

            items.append({"id": nid, "title": title, "date": date_text})

        return items

    # ── Circular fetching (streaming, page by page) ──────────────────

    def _iter_circulars(self, max_pages: int = 30) -> Generator[dict, None, None]:
        """Yield circulars one-at-a-time from paginated listing."""
        seen_ids: set[str] = set()

        for page in range(1, max_pages + 1):
            # Path-based pagination: /news/section/77 for page 1, /news/section/77/2 for page 2
            if page == 1:
                url = f"{BASE_URL}/news/section/77"
            else:
                url = f"{BASE_URL}/news/section/77/{page}"

            logger.info("Fetching circulars list page %d/%d", page, max_pages)
            try:
                resp = self.http.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning("Page %d returned %d, stopping", page, resp.status_code)
                    break
                html = resp.text
                if self._is_captcha(html):
                    logger.warning("CAPTCHA on page %d, stopping", page)
                    break
            except Exception as e:
                logger.warning("Failed to fetch page %d: %s", page, e)
                break

            page_items = self._parse_listing_page(html)
            new_items = [item for item in page_items if item["id"] not in seen_ids]
            for item in new_items:
                seen_ids.add(item["id"])

            if not new_items:
                logger.info("No new items on page %d, stopping", page)
                break

            for item in new_items:
                try:
                    record = self._process_circular(item)
                    if record:
                        yield record
                except Exception as e:
                    logger.warning("Failed circular %s: %s", item["id"], e)
                time.sleep(1)

            time.sleep(1)

    def _process_circular(self, item: dict) -> Optional[dict]:
        """Fetch detail page and extract text for one circular."""
        nid = item["id"]
        resp = self.http.get(f"{BASE_URL}/news/view/{nid}", timeout=30)
        html = resp.text

        if self._is_captcha(html):
            logger.warning("CAPTCHA on detail page %s", nid)
            return None

        # Extract PDF links from detail page
        pdfs = re.findall(r'href="(https?://cbi\.iq/static/uploads/[^"]*\.pdf)"', html)
        pdfs = list(dict.fromkeys(pdfs))  # deduplicate preserving order

        # Try PDF extraction first
        text = None
        pdf_url = None
        for url in pdfs:
            text = self._extract_pdf_text(url, f"circular-{nid}")
            if text:
                pdf_url = url
                break

        # Fall back to inline HTML content if substantial
        if not text:
            soup = BeautifulSoup(html, "html.parser")
            content_div = soup.select_one(".col-md-9.col-sm-9.content-bx")
            if content_div:
                for tag in content_div.find_all(["script", "style", "nav"]):
                    tag.decompose()
                inline = content_div.get_text(separator="\n", strip=True)
                inline = re.split(r"أخبار عشوائية", inline)[0].strip()
                if len(inline) > MIN_TEXT_LENGTH:
                    text = inline

        if not text:
            logger.debug("Skipping circular %s (no substantial text): %s", nid, item["title"][:60])
            return None

        return {
            "_id": f"circular-{nid}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item["title"],
            "text": text,
            "date": item.get("date"),
            "url": f"{BASE_URL}/news/view/{nid}",
            "document_type": "circular",
            "pdf_url": pdf_url,
            "category": "Circulars",
        }

    # ── Guidelines/Standards ──────────────────────────────────────────

    def _iter_guidelines(self) -> Generator[dict, None, None]:
        """Fetch guidelines and standards from /page/99."""
        logger.info("Fetching guidelines/standards page...")
        try:
            resp = self.http.get(f"{BASE_URL}/page/99", timeout=30)
            html = resp.text
            if self._is_captcha(html):
                logger.warning("CAPTCHA on guidelines page, skipping")
                return

            pdfs = re.findall(
                r'href="([^"]*static/uploads[^"]*\.pdf)"[^>]*>([^<]*)</a>',
                html,
            )
            if not pdfs:
                pdfs_bare = re.findall(r'href="([^"]*static/uploads[^"]*\.pdf)"', html)
                pdfs = [(url, "") for url in pdfs_bare]

            seen_urls: set[str] = set()
            for url, label in pdfs:
                clean_url = url.split("?")[0]
                if clean_url in seen_urls:
                    continue
                seen_urls.add(clean_url)

                if not url.startswith("http"):
                    url = f"{BASE_URL}{url}" if url.startswith("/") else f"{BASE_URL}/{url}"

                clean_label = label.strip() if label.strip() else Path(clean_url).stem
                text = self._extract_pdf_text(url, f"guideline-{Path(clean_url).stem}")
                if not text:
                    logger.warning("No text for guideline: %s", clean_label[:60])
                    continue

                yield {
                    "_id": f"guideline-{Path(clean_url).stem}",
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": clean_label,
                    "text": text,
                    "date": None,
                    "url": url,
                    "document_type": "guideline",
                    "pdf_url": url,
                    "category": "Guidelines & Standards",
                }
                time.sleep(1)

        except Exception as e:
            logger.warning("Failed to fetch guidelines: %s", e)

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_circulars()
        yield from self._iter_guidelines()

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for record in self._iter_circulars(max_pages=3):
            if since and record.get("date") and record["date"] < since:
                continue
            yield record

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="IQ/CBI-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBIRegulationsScraper()

    if args.command == "test-api":
        logger.info("Testing circulars list page 1...")
        resp = scraper.http.get(f"{BASE_URL}/news/section/77", timeout=30)
        if scraper._is_captcha(resp.text):
            logger.error("CAPTCHA detected — cannot proceed")
            return
        items = scraper._parse_listing_page(resp.text)
        logger.info("OK: %d circulars on page 1", len(items))
        if items:
            item = items[0]
            logger.info("Sample: ID=%s, title=%s, date=%s", item["id"], item["title"][:60], item["date"])
            detail_resp = scraper.http.get(f"{BASE_URL}/news/view/{item['id']}", timeout=30)
            pdfs = re.findall(r'href="(https?://cbi\.iq/static/uploads/[^"]*\.pdf)"', detail_resp.text)
            logger.info("Detail: %d PDFs found", len(set(pdfs)))
        return

    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0

        if args.sample:
            for record in scraper.fetch_all():
                count += 1
                with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    "[%d] %s — %d chars",
                    count,
                    record["title"][:60],
                    len(record.get("text", "")),
                )
                if count >= 15:
                    break
            logger.info("Done: %d sample records saved to %s", count, sample_dir)
            return

        # A full run streams to data/records.jsonl, which is what the pipeline
        # ingests. It used to write only the first 15 records as individual
        # sample/ files and persist nothing else, so a fleet run left
        # records.jsonl absent and only the bundled samples were ever ingested
        # (issue #798 class).
        data_dir = source_dir / "data"
        data_dir.mkdir(exist_ok=True)
        records_file = data_dir / "records.jsonl"

        with open(records_file, "w", encoding="utf-8") as f:
            for record in scraper.fetch_all():
                count += 1
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                logger.info(
                    "[%d] %s — %d chars",
                    count,
                    record["title"][:60],
                    len(record.get("text", "")),
                )

        logger.info("Done: %d records written to %s", count, records_file)

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
            logger.info("[%d] %s", count, record["title"][:60])
        logger.info("Done: %d updates fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
