#!/usr/bin/env python3
"""
ZA/CompTribunal -- South Africa Competition Tribunal decided cases

Fetches full-text decisions from comptrib.co.za. Paginated listing at
/decided-cases?page=N (~168 pages, ~20 cases each). Each case page has
metadata fields and PDF decision documents under "Case File".

Data access:
  - Listing: /decided-cases?page=N (pages 1..168)
  - Case detail: /cases-case-files/<slug>
  - PDFs: /uploads/topics/CompTrib_Case_Files/<filename>.pdf
  - Full text extracted from PDFs via common.pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.CompTribunal")

BASE_URL = "https://www.comptrib.co.za"
DELAY = 2.0


def _clean_text(html_fragment: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r"<[^>]+>", " ", html_fragment)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


class CompTribunalScraper(BaseScraper):
    """Scraper for South Africa Competition Tribunal decided cases."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def _get_case_slugs(self, page: int) -> List[str]:
        """Get unique case slugs from a listing page."""
        url = f"{BASE_URL}/decided-cases?page={page}"
        time.sleep(DELAY)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch listing page %d: %s", page, resp.status_code)
            return []

        links = re.findall(
            r'href=["\'](?:https?://www\.comptrib\.co\.za)?/?(cases-case-files/[^"\'>\s]+)["\']',
            resp.text,
        )
        return list(dict.fromkeys(links))  # deduplicate preserving order

    def _get_max_page(self) -> int:
        """Get the last page number from the first listing page."""
        url = f"{BASE_URL}/decided-cases"
        resp = self.http.get(url)
        if resp.status_code != 200:
            return 168  # fallback
        pages = re.findall(r'decided-cases\?page=(\d+)', resp.text)
        return max(int(p) for p in pages) if pages else 168

    def _parse_case_page(self, slug: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a case detail page. Returns metadata + PDF URLs."""
        url = f"{BASE_URL}/{slug}"
        time.sleep(DELAY)
        resp = self.http.get(url)
        if resp.status_code != 200:
            logger.warning("Failed to fetch case %s: %s", slug, resp.status_code)
            return None

        html = resp.text
        meta: Dict[str, Any] = {"slug": slug, "url": url, "pdf_urls": []}

        # Extract custom fields (title/value pairs)
        field_pattern = (
            r'<h5\s+class="custom-field-title">(.*?)</h5>\s*'
            r'<div\s+class="custom-field-value[^"]*">(.*?)</div>'
        )
        for match in re.finditer(field_pattern, html, re.DOTALL):
            label = _clean_text(match.group(1)).strip()
            value_html = match.group(2)
            value_text = _clean_text(value_html).strip()

            label_lower = label.lower()
            if "case number" in label_lower:
                meta["case_number"] = value_text
            elif "case name" in label_lower:
                meta["case_name"] = value_text
            elif "case type" in label_lower:
                meta["case_type"] = value_text
            elif "case status" in label_lower:
                meta["case_status"] = value_text
            elif "outcome" in label_lower:
                meta["outcome"] = value_text
            elif "order date" in label_lower:
                meta["order_date"] = value_text
            elif "sector" in label_lower:
                meta["sector"] = value_text
            elif "industry" in label_lower:
                meta["industry"] = value_text
            elif "case file" in label_lower:
                # Extract PDF links
                pdf_links = re.findall(
                    r'href="([^"]*\.pdf[^"]*)"[^>]*>\s*(.*?)\s*</a>',
                    value_html, re.DOTALL,
                )
                for pdf_url, pdf_label in pdf_links:
                    pdf_label_clean = _clean_text(pdf_label)
                    meta["pdf_urls"].append({
                        "url": pdf_url if pdf_url.startswith("http") else f"{BASE_URL}/{pdf_url.lstrip('/')}",
                        "label": pdf_label_clean,
                    })

        # Try to extract title from page heading
        title_match = re.search(
            r'<h1[^>]*class="[^"]*topic-title[^"]*"[^>]*>(.*?)</h1>',
            html, re.DOTALL,
        )
        if title_match:
            meta["page_title"] = _clean_text(title_match.group(1))

        return meta

    def _pick_best_pdf(self, pdf_urls: List[Dict[str, str]]) -> Optional[str]:
        """Pick the best PDF to extract text from: prefer Reasons > Order > any."""
        if not pdf_urls:
            return None

        # Prefer "Reasons" PDFs (detailed analysis) over "Order" (short)
        for pdf in pdf_urls:
            label = pdf["label"].lower()
            if "reason" in label:
                return pdf["url"]

        for pdf in pdf_urls:
            label = pdf["label"].lower()
            if "order" in label:
                return pdf["url"]

        return pdf_urls[0]["url"]

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw case dict into standard schema."""
        case_number = raw.get("case_number", "")
        case_name = raw.get("case_name", "") or raw.get("page_title", "")
        title = f"{case_name} ({case_number})" if case_number else case_name

        date = raw.get("order_date")
        if date:
            # Validate date format
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                date = None

        doc_id = case_number.replace("/", "-") if case_number else raw["slug"].split("/")[-1]

        return {
            "_id": f"ZA-CompTrib-{doc_id}",
            "_source": "ZA/CompTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "case_number": case_number,
            "case_type": raw.get("case_type", ""),
            "outcome": raw.get("outcome", ""),
            "case_status": raw.get("case_status", ""),
            "sector": raw.get("sector", ""),
            "industry": raw.get("industry", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield raw case dicts with full text from PDFs."""
        max_page = self._get_max_page()
        logger.info("Total listing pages: %d", max_page)

        for page in range(1, max_page + 1):
            slugs = self._get_case_slugs(page)
            logger.info("Page %d/%d: %d cases", page, max_page, len(slugs))

            for slug in slugs:
                meta = self._parse_case_page(slug)
                if meta is None:
                    continue

                pdf_url = self._pick_best_pdf(meta.get("pdf_urls", []))
                if not pdf_url:
                    logger.warning("No PDF for %s, skipping", slug)
                    continue

                doc_id = meta.get("case_number", slug.split("/")[-1]).replace("/", "-")
                text = extract_pdf_markdown(
                    source="ZA/CompTribunal",
                    source_id=f"ZA-CompTrib-{doc_id}",
                    pdf_url=pdf_url,
                    table="case_law",
                )
                if text is None:
                    logger.warning("PDF extraction failed for %s", slug)
                    continue

                meta["text"] = text
                yield meta

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent cases (first few listing pages)."""
        for page in range(1, 4):
            slugs = self._get_case_slugs(page)
            logger.info("Updates page %d: %d cases", page, len(slugs))

            for slug in slugs:
                meta = self._parse_case_page(slug)
                if meta is None:
                    continue

                pdf_url = self._pick_best_pdf(meta.get("pdf_urls", []))
                if not pdf_url:
                    continue

                doc_id = meta.get("case_number", slug.split("/")[-1]).replace("/", "-")
                text = extract_pdf_markdown(
                    source="ZA/CompTribunal",
                    source_id=f"ZA-CompTrib-{doc_id}",
                    pdf_url=pdf_url,
                    table="case_law",
                )
                if text is None:
                    continue

                meta["text"] = text
                yield meta

    def test_connection(self) -> bool:
        """Test that we can access comptrib.co.za."""
        resp = self.http.get(f"{BASE_URL}/decided-cases")
        if resp.status_code == 200:
            logger.info("Connection test passed: comptrib.co.za is accessible")
            return True
        logger.error("Connection test failed: status %s", resp.status_code)
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZA/CompTribunal bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CompTribunalScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    if args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            scraper.storage.save(record)
            count += 1
            if count % 10 == 0:
                logger.info("Saved %d records", count)
        logger.info("Update complete: %d records", count)
        return

    # bootstrap (optionally with --sample)
    sample_dir = Path(__file__).resolve().parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.sample:
        count = 0
        target = 12
        # Sample from page 100 (older cases more likely to have PDFs)
        for page in [100, 50, 1]:
            if count >= target:
                break
            slugs = scraper._get_case_slugs(page)
            logger.info("Sample page %d: %d cases", page, len(slugs))

            for slug in slugs:
                if count >= target:
                    break

                meta = scraper._parse_case_page(slug)
                if meta is None:
                    continue

                pdf_url = scraper._pick_best_pdf(meta.get("pdf_urls", []))
                if not pdf_url:
                    logger.info("No PDF for %s, skipping", slug)
                    continue

                doc_id = meta.get("case_number", slug.split("/")[-1]).replace("/", "-")
                text = extract_pdf_markdown(
                    source="ZA/CompTribunal",
                    source_id=f"ZA-CompTrib-{doc_id}",
                    pdf_url=pdf_url,
                    table="case_law",
                )
                if not text or len(text) < 100:
                    logger.warning("Insufficient text for %s (%d chars)", slug, len(text or ""))
                    continue

                meta["text"] = text
                record = scraper.normalize(meta)

                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(
                    "Sample %d/%d: %s (%d chars)",
                    count, target, record["_id"], len(record["text"]),
                )

        logger.info("Sample complete: %d records saved to %s", count, sample_dir)
    else:
        stats = scraper.bootstrap()
        logger.info("Bootstrap complete: %s", stats)


if __name__ == "__main__":
    main()
