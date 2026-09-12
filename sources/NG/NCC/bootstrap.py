#!/usr/bin/env python3
"""
NG/NCC -- Nigerian Communications Commission

Fetches regulations, guidelines, and determinations issued by the NCC under
the Nigerian Communications Act 2003. Documents are PDFs covering telecom
licensing, interconnection rates, spectrum, quality of service, consumer
protection, type approval, number portability, and more.

The NCC website serves each document as a PDF behind a stable
`/media/{id}/view` URL. Listing pages group documents into published/draft
sections; we scrape the anchor text as the title and download the PDF.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.NCC")

BASE_URL = "https://ncc.gov.ng"

# Listing pages, each mapped to a document_type. The keys mirror the NCC site's
# own categorisation. Regulations are legally binding (legislation); guidelines
# and determinations are official regulatory guidance/decisions (doctrine).
LISTING_PAGES = [
    ("https://ncc.gov.ng/operators/regulations-guidelines/regulations", "regulation"),
    ("https://ncc.gov.ng/guidelines", "guideline"),
    ("https://ncc.gov.ng/industry/regulations-guidelines/determinations", "determination"),
]

MEDIA_RE = re.compile(r"/media/(\d+)/view")


class NCCScraper(BaseScraper):
    """
    Scraper for NG/NCC -- Nigerian Communications Commission regulations,
    guidelines, and determinations.

    Country: NG
    URL: https://ncc.gov.ng/industry/regulations-guidelines/determinations

    Data types: legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open-data research)",
        })

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _get_listing(self, url: str, doc_type: str) -> list[dict]:
        """Scrape /media/{id}/view document links from an NCC listing page."""
        from bs4 import BeautifulSoup

        try:
            resp = self.session.get(url, timeout=45)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing {url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seen_ids = set()
        documents = []

        for link in soup.find_all("a", href=MEDIA_RE):
            href = link["href"]
            m = MEDIA_RE.search(href)
            if not m:
                continue
            media_id = m.group(1)
            if media_id in seen_ids:
                continue
            seen_ids.add(media_id)

            pdf_url = urljoin(BASE_URL, f"/media/{media_id}/view")

            title = link.get_text(" ", strip=True)
            title = re.sub(r"\s+", " ", title).strip()
            # Drop trailing .pdf if the anchor text was a raw filename
            title = re.sub(r"\.pdf$", "", title, flags=re.IGNORECASE).strip()

            if not title or len(title) < 5 or title.lower() in (
                "view", "download", "pdf", "click here", "read more"
            ):
                # Fall back to the link's title/aria-label attributes
                title = (link.get("title") or link.get("aria-label") or "").strip()
                title = re.sub(r"\.pdf$", "", title, flags=re.IGNORECASE).strip()
            if not title or len(title) < 5:
                continue

            documents.append({
                "media_id": media_id,
                "title": title,
                "pdf_url": pdf_url,
                "date": _extract_year_date(title),
                "document_type": doc_type,
            })

        logger.info(f"Found {len(documents)} {doc_type} documents at {url}")
        return documents

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
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
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

        doc_id = f"NG-NCC-{raw['media_id']}"

        doc_type = raw.get("document_type", "guideline")
        _type = "legislation" if doc_type == "regulation" else "doctrine"

        return {
            "_id": doc_id,
            "_source": "NG/NCC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "document_type": doc_type,
        }

    # ------------------------------------------------------------------
    # Main fetch methods
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all NCC documents with full PDF text."""
        per_page = []
        for url, doc_type in LISTING_PAGES:
            per_page.append(self._get_listing(url, doc_type))
            time.sleep(1.0)

        # Round-robin interleave across the three categories so that any
        # truncated sample run captures a representative mix of legislation
        # (regulations) and doctrine (guidelines, determinations).
        interleaved = []
        for i in range(max((len(p) for p in per_page), default=0)):
            for page in per_page:
                if i < len(page):
                    interleaved.append(page[i])

        # Deduplicate by media_id (a doc may appear on more than one page)
        seen = set()
        unique_docs = []
        for doc in interleaved:
            mid = doc["media_id"]
            if mid not in seen:
                seen.add(mid)
                unique_docs.append(doc)

        logger.info(f"Total unique documents: {len(unique_docs)}")

        yielded = 0
        skipped = 0
        for i, doc in enumerate(unique_docs):
            logger.info(f"[{i+1}/{len(unique_docs)}] Downloading: {doc['title'][:70]}")
            text = self._extract_pdf_text(doc["pdf_url"])
            if not text:
                skipped += 1
                logger.warning(f"Skipped (no text): {doc['title'][:70]}")
                continue
            doc["text"] = text
            normalized = self.normalize(doc)
            if normalized:
                yielded += 1
                yield normalized
            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No incremental date filter available; re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        results = {}
        for url, doc_type in LISTING_PAGES:
            try:
                resp = self.session.get(url, timeout=45)
                n = len(MEDIA_RE.findall(resp.text))
                results[doc_type] = {
                    "status": resp.status_code,
                    "ok": resp.status_code == 200 and n > 0,
                    "media_links": n,
                }
            except Exception as e:
                results[doc_type] = {"status": "error", "ok": False, "error": str(e)}

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
    """Extract a 4-digit year from text and return it as an ISO date."""
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
    scraper = NCCScraper()

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

        for record in gen:
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
