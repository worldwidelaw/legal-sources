#!/usr/bin/env python3
"""
BH/PDP -- Bahrain Personal Data Protection Authority

Fetches the PDPL (Law No. 30 of 2018), the royal decree, and all
executive decisions / orders published on pdp.gov.bh.

The site is a static set of HTML pages with PDFs hosted on the same
CloudFront-fronted S3 bucket. There is no API. We scrape the three
landing pages (regulations.html, royal-decree.html,
executive-decisions.html) to enumerate PDF URLs, then download each PDF
and extract markdown via opendataloader-pdf / pdfplumber fallback.

CloudFront returns 403 for the default requests User-Agent, so we pass
pre-downloaded bytes to extract_pdf_markdown.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update
  python bootstrap.py test
"""

from __future__ import annotations

import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BH.PDP")

BASE_URL = "https://www.pdp.gov.bh/en"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

LISTING_PAGES = {
    "regulations.html": "Law",
    "royal-decree.html": "Royal Decree",
    "executive-decisions.html": "Executive Decision",
}


class BHPDPScraper:
    SOURCE_ID = "BH/PDP"

    def _http_get(self, url: str, timeout: int = 30) -> Optional[bytes]:
        req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (HTTPError, URLError) as e:
            logger.warning("Fetch failed %s: %s", url, e)
            return None

    def _fetch_html(self, path: str) -> Optional[str]:
        data = self._http_get(f"{BASE_URL}/{path}")
        if data is None:
            return None
        return data.decode("utf-8", errors="replace")

    def _list_pdfs(self) -> List[Dict[str, str]]:
        """Walk the three landing pages and collect PDF URLs."""
        records: List[Dict[str, str]] = []
        seen_urls: set[str] = set()

        for page, category in LISTING_PAGES.items():
            html = self._fetch_html(page)
            if not html:
                continue
            for href in re.findall(r'href="([^"]+\.pdf)"', html):
                # Resolve relative URLs against the /en/ base.
                if href.startswith("http"):
                    url = href
                else:
                    url = f"{BASE_URL}/{href.lstrip('./')}"

                # Skip the off-site Official Gazette PDF — it is the
                # legalaffairs.gov.bh gazette index, not a PDP document.
                if "legalaffairs.gov.bh" in url:
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                slug = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                # Mark Arabic copies (under /executive-decisions/ root)
                # vs English (under /executive-decisions/eng/).
                lang = "en" if "/eng/" in url or page != "executive-decisions.html" else "ar"
                # The two main law PDFs sit at /assets/pdf/<slug>.pdf
                doc_id = self._make_doc_id(url, slug, lang)
                records.append({
                    "doc_id": doc_id,
                    "slug": slug,
                    "lang": lang,
                    "category": category,
                    "url": url,
                    "page_url": f"{BASE_URL}/{page}",
                })
        return records

    @staticmethod
    def _make_doc_id(url: str, slug: str, lang: str) -> str:
        # Use a path-derived id so Arabic and English don't collide.
        # e.g. "executive-decisions/eng/auditor-fees-en"
        if "/assets/pdf/" in url:
            tail = url.split("/assets/pdf/", 1)[1]
            tail = tail.rsplit(".", 1)[0]
        else:
            tail = slug
        return f"{tail}__{lang}" if lang == "ar" and "-ar" not in tail else tail

    @staticmethod
    def _title_from_slug(slug: str, category: str, lang: str) -> str:
        # Strip language suffixes for display
        base = re.sub(r"[-_](?:en|ar|english|arabic)$", "", slug, flags=re.I)
        pretty = re.sub(r"[-_]+", " ", base).strip().title()
        suffix = " (Arabic)" if lang == "ar" else ""
        if category in ("Law", "Royal Decree"):
            return f"PDPL {category}: {pretty}{suffix}"
        return f"Executive Order — {pretty}{suffix}"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        pdfs = self._list_pdfs()
        logger.info("Found %d PDFs across PDP pages", len(pdfs))

        for rec in pdfs:
            time.sleep(1.0)
            pdf_bytes = self._http_get(rec["url"], timeout=60)
            if pdf_bytes is None:
                logger.warning("Skipping %s: download failed", rec["doc_id"])
                continue
            if not pdf_bytes.startswith(b"%PDF"):
                logger.warning("Skipping %s: not a PDF", rec["doc_id"])
                continue

            # force=True: the Arabic half of this corpus was stored
            # character-reversed (issue #1560), so a refresh has to re-extract
            # the rows already in Neon rather than skip them as present.
            text = extract_pdf_markdown(
                source=self.SOURCE_ID,
                source_id=rec["doc_id"],
                pdf_bytes=pdf_bytes,
                table="doctrine",
                force=True,
            )
            if not text or len(text.strip()) < 100:
                logger.warning(
                    "Skipping %s: insufficient text (%d chars)",
                    rec["doc_id"], len(text) if text else 0,
                )
                continue

            yield self.normalize({
                **rec,
                "title": self._title_from_slug(rec["slug"], rec["category"], rec["lang"]),
                "text": text,
            })

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": None,
            "url": raw.get("page_url") or raw["url"],
            "pdf_url": raw["url"],
            "category": raw["category"],
            "language": raw["lang"],
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BH/PDP bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = BHPDPScraper()

    if args.command == "test":
        pdfs = scraper._list_pdfs()
        print(f"OK: found {len(pdfs)} PDF references")
        for r in pdfs[:5]:
            print(" -", r["doc_id"], "->", r["url"])
        if not pdfs:
            sys.exit(1)
        return

    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    if args.sample:
        for record in scraper.fetch_all():
            count += 1
            fname = re.sub(r"[^\w\-]", "_", record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s (%d chars)",
                count, record["title"][:60], len(record.get("text", "")),
            )
            if count >= limit:
                logger.info("Sample limit reached (%d)", limit)
                break
        print(f"\nDone: {count} records saved to {sample_dir}")
        return

    # A full run streams to data/records.jsonl, which is what the pipeline
    # ingests. It used to write the whole corpus into sample/ as individual
    # files, so a fleet run left records.jsonl empty and only the bundled
    # samples were ever ingested (issue #798 class).
    data_dir = source_dir / "data"
    data_dir.mkdir(exist_ok=True)
    records_file = data_dir / "records.jsonl"

    with open(records_file, "w", encoding="utf-8") as f:
        for record in scraper.fetch_all():
            count += 1
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            logger.info(
                "[%d] %s (%d chars)",
                count, record["title"][:60], len(record.get("text", "")),
            )

    print(f"\nDone: {count} records written to {records_file}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
