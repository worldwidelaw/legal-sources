#!/usr/bin/env python3
"""
TH/BOT-Notifications -- Bank of Thailand Notifications and Circulars

Scrapes the FIPCS English listing at app.bot.or.th, downloads English PDFs
from bot.or.th, and extracts full text.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TH.BOT-Notifications")

LIST_URL = "https://app.bot.or.th/FIPCS/eng/PFIPCS_list.aspx"
DELAY = 2.0

DOC_TYPE_MAP = {
    "circular of the bot": "circular",
    "notification": "notification",
    "notice of the competent officer": "notice",
    "ministerial regulation": "ministerial_regulation",
    "ministerial regulations": "ministerial_regulation",
    "royal decree": "royal_decree",
    "act": "act",
}


def _parse_date(date_str: str) -> Optional[str]:
    """Parse BOT date format like '1 Sep 2025' to ISO 8601."""
    date_str = date_str.strip()
    if not date_str:
        return None
    for fmt in ("%d %b %Y", "%d %B %Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _make_id(pdf_url: str) -> str:
    """Extract document number from PDF URL as ID."""
    # URL pattern: .../EngPDF/25680173.pdf or .../ThaiPDF/25680173.pdf
    m = re.search(r"/(\d+)\.pdf", pdf_url, re.I)
    if m:
        return f"TH_BOT_{m.group(1)}"
    # Fallback: use filename
    name = pdf_url.split("/")[-1].replace(".pdf", "").replace(".PDF", "")
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    return f"TH_BOT_{name}"


def _classify_doc_type(raw_type: str) -> str:
    """Normalize document type string."""
    lower = raw_type.lower().strip().rstrip(".")
    for key, val in DOC_TYPE_MAP.items():
        if key in lower:
            return val
    return "other"


class THBOTScraper(BaseScraper):
    """Scraper for Bank of Thailand Notifications and Circulars."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    def _get_page(self, session, page_num: int, viewstate: str,
                  eventvalidation: str, viewstategenerator: str) -> str:
        """Fetch a specific page via ASP.NET postback."""
        data = {
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION": eventvalidation,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$dgDocument$ctl33$btnNext",
            "__EVENTARGUMENT": "",
        }
        resp = session.post(LIST_URL, data=data, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _parse_rows(self, soup) -> List[Dict[str, Any]]:
        """Parse document rows from a page."""
        from bs4 import BeautifulSoup
        docs = []
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 10:
                continue
            header = rows[0].find_all(["th", "td"])
            if len(header) < 4:
                continue
            header_text = " ".join(c.get_text(strip=True) for c in header)
            if "Type" not in header_text or "Subject" not in header_text:
                continue

            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 7:
                    continue

                doc_type_raw = cells[0].get_text(strip=True)
                date_raw = cells[1].get_text(strip=True)
                subject = cells[3].get_text(strip=True)

                # Find English PDF link
                eng_url = None
                for a in row.find_all("a"):
                    href = a.get("href", "")
                    text = a.get_text(strip=True)
                    if "ENG" in text and href.endswith(".pdf"):
                        eng_url = href
                        break

                # Fall back to any PDF link with EngPDF in the URL
                if not eng_url:
                    for a in row.find_all("a"):
                        href = a.get("href", "")
                        if "EngPDF" in href and href.endswith(".pdf"):
                            eng_url = href
                            break

                if not eng_url or not subject:
                    continue

                doc_id = _make_id(eng_url)
                docs.append({
                    "doc_id": doc_id,
                    "title": subject,
                    "date": _parse_date(date_raw),
                    "doc_type": _classify_doc_type(doc_type_raw),
                    "pdf_url": eng_url,
                    "status": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                })
            break  # Only process the first matching table
        return docs

    def _discover_documents(self, max_pages: int = 27) -> List[Dict[str, Any]]:
        """Discover all documents across all pages."""
        import requests
        from bs4 import BeautifulSoup

        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

        all_docs = []
        seen_ids = set()

        # Fetch first page
        logger.info("Fetching page 1...")
        resp = session.get(LIST_URL, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        page_docs = self._parse_rows(soup)
        for d in page_docs:
            if d["doc_id"] not in seen_ids:
                seen_ids.add(d["doc_id"])
                all_docs.append(d)
        logger.info("Page 1: %d documents (total: %d)", len(page_docs), len(all_docs))

        # Navigate subsequent pages
        for page in range(2, max_pages + 1):
            time.sleep(DELAY)
            logger.info("Fetching page %d/%d...", page, max_pages)

            vs = soup.find("input", {"name": "__VIEWSTATE"})
            ev = soup.find("input", {"name": "__EVENTVALIDATION"})
            vsg = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})

            if not vs or not ev:
                logger.warning("Missing form fields on page %d, stopping", page - 1)
                break

            try:
                html = self._get_page(
                    session, page,
                    vs.get("value", ""),
                    ev.get("value", ""),
                    vsg.get("value", "") if vsg else "",
                )
                soup = BeautifulSoup(html, "html.parser")
                page_docs = self._parse_rows(soup)

                if not page_docs:
                    logger.info("No docs on page %d, stopping", page)
                    break

                new_count = 0
                for d in page_docs:
                    if d["doc_id"] not in seen_ids:
                        seen_ids.add(d["doc_id"])
                        all_docs.append(d)
                        new_count += 1

                logger.info("Page %d: %d docs (%d new, total: %d)",
                            page, len(page_docs), new_count, len(all_docs))

                if new_count == 0:
                    logger.info("No new docs on page %d, stopping", page)
                    break

            except Exception as e:
                logger.warning("Error fetching page %d: %s", page, e)
                break

        logger.info("Total unique documents discovered: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("TH/BOT-Notifications", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BOT notification documents with full text."""
        all_docs = self._discover_documents()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = doc["doc_id"]
            logger.info("Processing: %s", doc["title"][:80])

            text = self._download_and_extract(doc["pdf_url"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            yield {
                "_id": doc_id,
                "title": doc["title"],
                "date": doc["date"],
                "doc_type": doc["doc_type"],
                "status": doc.get("status", ""),
                "pdf_url": doc["pdf_url"],
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "TH/BOT-Notifications",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("doc_type", ""),
            "status": raw.get("status", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="TH/BOT-Notifications bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = THBOTScraper()

    if args.command == "test":
        docs = scraper._discover_documents(max_pages=2)
        print(f"OK -- found {len(docs)} documents across 2 pages")
        types = {}
        for d in docs:
            t = d["doc_type"]
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items()):
            print(f"  {t}: {c}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
