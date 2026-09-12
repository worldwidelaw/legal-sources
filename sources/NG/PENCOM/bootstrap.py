#!/usr/bin/env python3
"""
NG/PENCOM -- Nigeria National Pension Commission

Fetches regulations, guidelines, circulars, frameworks, and codes issued by
PenCom, the regulator of the Nigerian pension industry under the Pension Reform
Act 2014. Documents are PDFs hosted on the PenCom WordPress site, grouped into
five category pages (each paginated).

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
from urllib.parse import urljoin, unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.PENCOM")

BASE_URL = "https://www.pencom.gov.ng"
CATEGORY_BASE = f"{BASE_URL}/category/regulations-guidelines-circulars-frameworks/{{}}/"

# Category slug -> document_type. Regulations are subsidiary legislation under
# the Pension Reform Act 2014; the rest are official regulatory guidance/codes
# (doctrine).
CATEGORIES = [
    ("regulations", "regulation"),
    ("guidelines", "guideline"),
    ("circulars", "circular"),
    ("frameworks", "framework"),
    ("codes", "code"),
]

MAX_PAGES = 25  # safety cap per category


class PENCOMScraper(BaseScraper):
    """
    Scraper for NG/PENCOM -- National Pension Commission regulations,
    guidelines, circulars, frameworks, and codes.

    Country: NG
    URL: https://www.pencom.gov.ng/category/regulations-guidelines-circulars-frameworks/regulations/

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

    def _get_category(self, slug: str, doc_type: str) -> list[dict]:
        """Scrape PDF document links across all pages of a category."""
        from bs4 import BeautifulSoup

        documents = []
        seen_urls = set()

        for page in range(1, MAX_PAGES + 1):
            if page == 1:
                url = CATEGORY_BASE.format(slug)
            else:
                url = CATEGORY_BASE.format(slug) + f"page/{page}/"

            try:
                resp = self.session.get(url, timeout=45)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                break

            if resp.status_code == 404:
                break
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Each document is linked twice (title anchor + a "Download" anchor).
            # Keep the best title per unique PDF URL.
            titles_for_url = {}
            page_pdf_urls = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if not href.lower().split("?")[0].endswith(".pdf"):
                    continue
                pdf_url = urljoin(BASE_URL, href)
                text = link.get_text(" ", strip=True)
                text = re.sub(r"\s+", " ", text).strip()
                if text.lower() in ("download", "view", "pdf", "read more", ""):
                    text = ""
                if pdf_url not in titles_for_url or (
                    text and len(text) > len(titles_for_url.get(pdf_url, ""))
                ):
                    titles_for_url[pdf_url] = titles_for_url.get(pdf_url, "")
                    if text:
                        titles_for_url[pdf_url] = text
                if pdf_url not in page_pdf_urls:
                    page_pdf_urls.append(pdf_url)

            new_on_page = 0
            for pdf_url in page_pdf_urls:
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)
                new_on_page += 1

                title = titles_for_url.get(pdf_url, "")
                if not title or len(title) < 5:
                    title = _title_from_filename(pdf_url)

                documents.append({
                    "title": title,
                    "pdf_url": pdf_url,
                    "date": _extract_date(title) or _extract_date_from_url(pdf_url),
                    "document_type": doc_type,
                })

            if new_on_page == 0:
                break
            time.sleep(0.8)

        logger.info(f"Found {len(documents)} {doc_type} documents in '{slug}'")
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
            full_text = _clean_text("\n\n".join(pages_text))
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

        import hashlib
        url_hash = hashlib.md5(raw["pdf_url"].encode()).hexdigest()[:12]
        doc_id = f"NG-PENCOM-{url_hash}"

        doc_type = raw.get("document_type", "guideline")
        _type = "legislation" if doc_type == "regulation" else "doctrine"

        return {
            "_id": doc_id,
            "_source": "NG/PENCOM",
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
        """Fetch all PenCom documents with full PDF text."""
        per_cat = []
        for slug, doc_type in CATEGORIES:
            per_cat.append(self._get_category(slug, doc_type))

        # Round-robin interleave so a truncated sample covers multiple types.
        interleaved = []
        for i in range(max((len(c) for c in per_cat), default=0)):
            for cat in per_cat:
                if i < len(cat):
                    interleaved.append(cat[i])

        # Deduplicate by PDF URL (a doc may be cross-listed)
        seen = set()
        unique_docs = []
        for doc in interleaved:
            if doc["pdf_url"] not in seen:
                seen.add(doc["pdf_url"])
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
            # Yield RAW per BaseScraper contract; the framework calls normalize().
            # (Previously yielded self.normalize(...) → double-normalize → 0 records.)
            yielded += 1
            yield doc
            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No incremental date filter available; re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        from bs4 import BeautifulSoup
        results = {}
        for slug, doc_type in CATEGORIES:
            url = CATEGORY_BASE.format(slug)
            try:
                resp = self.session.get(url, timeout=45)
                soup = BeautifulSoup(resp.text, "html.parser")
                n = len({a["href"] for a in soup.find_all("a", href=True)
                         if a["href"].lower().split("?")[0].endswith(".pdf")})
                results[slug] = {
                    "status": resp.status_code,
                    "ok": resp.status_code == 200 and n > 0,
                    "pdf_links_page1": n,
                }
            except Exception as e:
                results[slug] = {"status": "error", "ok": False, "error": str(e)}

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


def _title_from_filename(pdf_url: str) -> str:
    """Derive a human title from a PDF filename."""
    name = unquote(pdf_url.split("/")[-1])
    name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^\d{8,}[_-]?", "", name)  # strip leading numeric prefixes
    name = re.sub(r"[-_]+", " ", name).strip()
    return name


def _extract_date(text: str) -> Optional[str]:
    """Extract a 4-digit year from text and return as ISO date."""
    match = re.search(r"\b(19[89]\d|20[0-3]\d)\b", text or "")
    if match:
        return f"{match.group(1)}-01-01"
    return None


def _extract_date_from_url(pdf_url: str) -> Optional[str]:
    """WordPress uploads are stored under /uploads/YYYY/MM/ — use that."""
    m = re.search(r"/uploads/(20[0-3]\d)/(\d{2})/", pdf_url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = PENCOMScraper()

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
