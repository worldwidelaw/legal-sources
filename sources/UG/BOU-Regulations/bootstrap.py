#!/usr/bin/env python3
"""
UG/BOU-Regulations -- Bank of Uganda Acts, Regulations and Guidelines

Fetches acts, regulations, guidelines, and supervisory circulars from the
Bank of Uganda Strapi CMS API. Documents are PDFs served from /uploads/.

API endpoints:
  /api/supervision?populate=actsAndRegulations.category.items.file
  /api/supervision?populate=supervisoryCirculars.year.items.file

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

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UG.BOU-Regulations")

BASE_URL = "https://bou.or.ug"
API_BASE = f"{BASE_URL}/api"


class BOURegulationsScraper(BaseScraper):
    """
    Scraper for UG/BOU-Regulations -- Bank of Uganda.

    Country: UG
    URL: https://www.bou.or.ug/
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
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # API fetching
    # ------------------------------------------------------------------

    def _fetch_acts_and_regulations(self) -> list[dict]:
        """Fetch acts and regulations from the Strapi API."""
        url = (
            f"{API_BASE}/supervision?"
            "populate=actsAndRegulations.overview"
            "&populate=actsAndRegulations.category.items.file"
        )
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch acts/regulations: {e}")
            return []

        data = resp.json()
        ar = data.get("data", {}).get("actsAndRegulations", {})
        categories = ar.get("category", [])

        docs = []
        for ci, cat in enumerate(categories):
            doc_type = "act" if ci == 0 else "regulation"
            items = cat.get("items", [])
            for item in items:
                f = item.get("file") or {}
                if not f.get("url"):
                    continue
                pdf_url = f["url"]
                if not pdf_url.startswith("http"):
                    pdf_url = BASE_URL + pdf_url

                docs.append({
                    "title": _clean_title(item.get("title", "")),
                    "document_type": doc_type,
                    "pdf_url": pdf_url,
                    "file_name": f.get("name", ""),
                    "date": _extract_year_date(item.get("title", "") + " " + f.get("name", "")),
                })

        logger.info(f"Acts/Regulations: {len(docs)} documents")
        return docs

    def _fetch_circulars(self) -> list[dict]:
        """Fetch supervisory circulars from the Strapi API."""
        url = (
            f"{API_BASE}/supervision?"
            "populate=supervisoryCirculars.overview"
            "&populate=supervisoryCirculars.year.items.file"
        )
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch circulars: {e}")
            return []

        data = resp.json()
        sc = data.get("data", {}).get("supervisoryCirculars", {})
        years = sc.get("year", [])

        docs = []
        for yr in years:
            year_title = yr.get("title", "")
            items = yr.get("items", [])
            for item in items:
                f = item.get("file") or {}
                if not f.get("url"):
                    continue
                pdf_url = f["url"]
                if not pdf_url.startswith("http"):
                    pdf_url = BASE_URL + pdf_url

                title = item.get("title", "")
                month = item.get("month", "")
                if year_title and month:
                    title = f"{title} ({month} {year_title})"
                elif year_title:
                    title = f"{title} ({year_title})"

                docs.append({
                    "title": _clean_title(title),
                    "document_type": "circular",
                    "pdf_url": pdf_url,
                    "file_name": f.get("name", ""),
                    "date": _extract_year_date(
                        title + " " + year_title + " " + f.get("name", "")
                    ),
                    "description": item.get("description", ""),
                })

        logger.info(f"Circulars: {len(docs)} documents")
        return docs

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

        if not resp.content[:5].startswith(b"%PDF"):
            logger.warning(f"Not a PDF: {pdf_url}")
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
                # Release per-page cached objects/textmap before moving on.
                # pdfplumber otherwise retains every visited page's parsed layout,
                # so a single large statute PDF (e.g. the FI Act) can grow to
                # multiple GB and OOM-kill the VPS (exit 137, issue #938).
                try:
                    pg.flush_cache()
                    pg.get_textmap.cache_clear()
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

        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title)[:80].strip("-")
        doc_id = f"UG-BOU-{slug}"

        return {
            "_id": doc_id,
            "_source": "UG/BOU-Regulations",
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
        """Fetch all BOU documents with full PDF text."""
        all_docs = self._fetch_acts_and_regulations()
        all_docs.extend(self._fetch_circulars())

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
        try:
            resp = self.session.get(
                f"{API_BASE}/supervision?populate=actsAndRegulations.category.items.file",
                timeout=15,
            )
            ar = resp.json().get("data", {}).get("actsAndRegulations", {})
            cats = ar.get("category", [])
            total = sum(len(c.get("items", [])) for c in cats)
            results["acts_regulations"] = {
                "status": resp.status_code,
                "ok": resp.status_code == 200,
                "documents": total,
            }
        except Exception as e:
            results["acts_regulations"] = {"ok": False, "error": str(e)}

        try:
            resp = self.session.get(
                f"{API_BASE}/supervision?populate=supervisoryCirculars.year.items.file",
                timeout=15,
            )
            sc = resp.json().get("data", {}).get("supervisoryCirculars", {})
            years = sc.get("year", [])
            total = sum(len(y.get("items", [])) for y in years)
            results["circulars"] = {
                "status": resp.status_code,
                "ok": resp.status_code == 200,
                "documents": total,
            }
        except Exception as e:
            results["circulars"] = {"ok": False, "error": str(e)}

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


def _clean_title(title: str) -> str:
    """Clean up a title string."""
    title = title.replace("-", " ").replace("_", " ")
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _extract_year_date(text: str) -> Optional[str]:
    """Extract a 4-digit year from text and return as ISO date."""
    match = re.search(r"\b(19[6-9]\d|20[0-3]\d)\b", text)
    if match:
        return f"{match.group(1)}-01-01"
    return None


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BOURegulationsScraper()

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
