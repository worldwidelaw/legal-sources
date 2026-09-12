#!/usr/bin/env python3
"""
TZ/CMSA -- Tanzania Capital Markets and Securities Authority

Fetches regulations, acts, and guidelines from the CMSA website.
Documents are PDFs organized in publication categories:
  1  — Acts (CMS Act + amendments)
  12 — Regulations (licensing, CIS, conduct of business, etc.)
  15 — EAC Council Directives
  11 — Publications and guides

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, quote

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TZ.CMSA")

BASE_URL = "https://www.cmsa.go.tz"

# Publication categories: (category_id, label, document_type)
CATEGORIES = [
    (1, "Acts", "act"),
    (12, "Regulations", "regulation"),
    (4, "Guidelines", "guideline"),
    (13, "Circulars", "circular"),
    (14, "Draft Regulations", "draft_regulation"),
    (15, "EAC Directives", "directive"),
    (17, "Rules", "rule"),
    (11, "Publications", "publication"),
]


class CMSAScraper(BaseScraper):
    """
    Scraper for TZ/CMSA -- Tanzania Capital Markets and Securities Authority.

    Country: TZ
    URL: https://www.cmsa.go.tz/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    def _get_category_pdfs(self, cat_id: int, doc_type: str) -> list[dict]:
        """Scrape all PDF links from a CMSA publication category (all pages)."""
        all_docs = []
        seen_urls = set()
        page = 1

        while True:
            url = f"{BASE_URL}/publications/{cat_id}" + (f"?page={page}" if page > 1 else "")
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch category {cat_id} page {page}: {e}")
                break

            html = resp.text
            pdf_links = re.findall(r'href="([^"]*\.pdf)"', html)

            new_count = 0
            for pdf_url in pdf_links:
                if not pdf_url.startswith("http"):
                    pdf_url = urljoin(BASE_URL, pdf_url)

                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                new_count += 1

                # Extract title from filename
                fname = pdf_url.split("/")[-1]
                fname = requests.utils.unquote(fname)
                # Remove en-TIMESTAMP- prefix
                fname = re.sub(r"^en-\d+-", "", fname)
                # Remove .pdf extension
                title = fname.replace(".pdf", "").strip()
                # Clean up underscores and extra spaces
                title = title.replace("_", " ")
                title = re.sub(r"\s+", " ", title).strip()

                if not title or len(title) < 3:
                    title = f"CMSA Document ({doc_type})"

                date = _extract_year_date(title)

                all_docs.append({
                    "pdf_url": pdf_url,
                    "title": title,
                    "date": date,
                    "document_type": doc_type,
                    "category_id": cat_id,
                })

            logger.info(f"Category {cat_id} page {page}: {new_count} new PDFs")

            if new_count == 0:
                break

            # Check for next page
            has_next = f"page={page + 1}" in html
            if not has_next:
                break
            page += 1
            time.sleep(1.0)

        return all_docs

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        ctype = resp.headers.get("content-type", "")
        if "pdf" not in ctype.lower() and not resp.content[:5].startswith(b"%PDF"):
            logger.warning(f"Not a PDF ({ctype}): {pdf_url}")
            return None

        if len(resp.content) < 500:
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages_text = []
            for pg in pdf.pages:
                text = pg.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    pg.flush_cache(); pg.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            full_text = "\n\n".join(pages_text)
            full_text = _clean_text(full_text)
            return full_text if len(full_text) >= 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw document into the standard schema."""
        text = (raw.get("text") or "").strip()
        if not text or len(text) < 100:
            return None

        title = (raw.get("title") or "").strip()
        if not title:
            return None

        # Generate stable ID from filename slug
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title)[:80].strip("-")
        doc_id = f"TZ-CMSA-{slug}"

        return {
            "_id": doc_id,
            "_source": "TZ/CMSA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "document_type": raw.get("document_type", ""),
        }

    # ------------------------------------------------------------------
    # Main fetch methods
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CMSA documents with full PDF text."""
        all_docs = []
        for cat_id, label, doc_type in CATEGORIES:
            docs = self._get_category_pdfs(cat_id, doc_type)
            all_docs.extend(docs)
            time.sleep(1.0)

        # Deduplicate by URL
        seen = set()
        unique = []
        for doc in all_docs:
            if doc["pdf_url"] not in seen:
                seen.add(doc["pdf_url"])
                unique.append(doc)

        logger.info(f"Total unique documents: {len(unique)}")

        yielded = 0
        skipped = 0
        for i, doc in enumerate(unique):
            logger.info(f"[{i+1}/{len(unique)}] Downloading: {doc['title'][:70]}")
            text = self._extract_pdf_text(doc["pdf_url"])
            if not text:
                skipped += 1
                logger.warning(f"Skipped (no text): {doc['title'][:70]}")
                continue
            doc["text"] = text
            # Yield RAW doc; BaseScraper.bootstrap_fast()/update() call
            # self.normalize() on each. Previously this yielded a normalized
            # record, so the framework double-normalized it — normalize() reads
            # raw["pdf_url"] which the normalized record lacks (it has "url"),
            # raising KeyError for every record → "0 fetched, N errors" on the
            # VPS (issue #976).
            yielded += 1
            yield doc
            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No incremental date filter available; re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        results = {}
        for cat_id, label, _ in CATEGORIES:
            try:
                resp = self.session.get(f"{BASE_URL}/publications/{cat_id}", timeout=15)
                n = len(set(re.findall(r'href="([^"]*\.pdf)"', resp.text)))
                results[label] = {
                    "status": resp.status_code,
                    "ok": resp.status_code == 200,
                    "pdf_links": n,
                }
            except Exception as e:
                results[label] = {"status": "error", "ok": False, "error": str(e)}

        all_ok = all(r.get("ok") for r in results.values())
        return {"status": "ok" if all_ok else "partial", "endpoints": results}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _clean_text(text: str) -> str:
    """Collapse excessive whitespace while preserving paragraph breaks."""
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_year_date(text: str) -> Optional[str]:
    """Extract a 4-digit year from text and return as ISO date."""
    match = re.search(r"\b(19[89]\d|20[0-3]\d)\b", text)
    if match:
        return f"{match.group(1)}-01-01"
    return None


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CMSAScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        result = scraper.test()
        print(json.dumps(result, indent=2))
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample_mode else 99999

        gen = scraper.fetch_all() if command == "bootstrap" else scraper.fetch_updates()

        for raw in gen:
            # fetch_all() yields RAW docs; normalize here to match the VPS
            # bootstrap_fast() path and write proper sample records.
            record = scraper.normalize(raw)
            if not record:
                continue
            count += 1
            if sample_mode:
                outpath = sample_dir / f"{count:04d}.json"
                outpath.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                print(f"[{count}] {record['title'][:60]} ({len(record['text'])} chars)")
            else:
                print(json.dumps(record, ensure_ascii=False))

            if count >= limit:
                break

        print(f"\nTotal records: {count}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
