#!/usr/bin/env python3
"""
AU/NTLegislation -- Northern Territory Legislation Fetcher

Fetches Northern Territory Acts and subordinate legislation from
the official register at legislation.nt.gov.au (Sitecore CMS).

Strategy:
  - Discover document slugs via browse listing pages (By-Title)
  - Visit each document page to extract numeric download ID
  - Download PDF via /api/sitecore/Act/PDF?id={NUMERIC_ID}
  - Extract text from PDF using pdfplumber (fallback: pypdf)
  - No auth required; free public access

The register renders each PDF on demand (~3-18 s per document, no
server-side cache), so the per-document work lives in ``normalize()``:
``bootstrap-fast`` then overlaps those downloads across worker threads
while ``fetch_all()`` stays a cheap slug generator. Every request runs
under a wall-clock deadline so one stuck render cannot wedge the run
(issue #1383).

Data:
  - ~385 Acts + ~304 subordinate legislation
  - Full text via PDF download
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull (sequential)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Full pull, concurrent downloads
  python bootstrap.py update             # Re-fetch all (no incremental API)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import request_with_deadline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.NTLegislation")

BASE_URL = "https://legislation.nt.gov.au"
BROWSE_ACTS = f"{BASE_URL}/en/LegislationPortal/Acts/By-Title"
BROWSE_SL = f"{BASE_URL}/en/LegislationPortal/Subordinate-Legislation/By-Title"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 "
        "(LegalDataHunter; legal research; open data)"
    ),
    "Accept-Language": "en-AU,en;q=0.9",
}

# Sitecore renders PDFs on demand: small regulations take ~3 s, the largest
# consolidated Acts ~20 s. Give a generous ceiling but never wait forever.
PAGE_TIMEOUT = 60
PDF_TIMEOUT = 300

# Thread-local sessions — bootstrap-fast normalizes on worker threads.
_local = threading.local()


def _session() -> requests.Session:
    sess = getattr(_local, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        _local.session = sess
    return sess


def _fetch_url(url: str, timeout: int = PAGE_TIMEOUT) -> Optional[bytes]:
    """Fetch a URL under a wall-clock deadline, returning None on failure.

    ``requests``' own timeout is per socket operation, so a host that
    trickles bytes can hold a call open indefinitely. ``request_with_deadline``
    abandons the call once ``timeout`` seconds of wall clock have elapsed.
    """
    try:
        resp = request_with_deadline(
            _session(),
            "GET",
            url,
            wall_timeout=timeout,
            timeout=(15, timeout),
        )
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None

    if resp.status_code != 200:
        logger.debug(f"HTTP {resp.status_code} for {url}")
        return None
    return resp.content


# Links to individual documents. Sitecore serves them as /en/Legislation/SLUG
# on a cookie-less first hit but drops the language prefix to /Legislation/SLUG
# once an ASP.NET_SessionId exists, so accept both spellings.
_SLUG_RE = re.compile(r'/(?:en/)?Legislation/([A-Z0-9][A-Z0-9-]+?)(?:["\']|/)')


def _discover_slugs(browse_url: str, attempts: int = 3) -> List[str]:
    """Discover document slugs from a browse-by-title page.

    Returns deduplicated list of uppercase slugs (e.g., 'CRIMINAL-CODE-ACT-1983').
    """
    for attempt in range(1, attempts + 1):
        data = _fetch_url(browse_url)
        if not data:
            logger.warning(
                f"Failed to fetch browse page (attempt {attempt}/{attempts}): {browse_url}"
            )
            continue

        html = data.decode("utf-8", errors="replace")
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for s in _SLUG_RE.findall(html):
            if s not in seen:
                seen.add(s)
                unique.append(s)

        if unique:
            logger.info(f"Discovered {len(unique)} slugs from {browse_url}")
            return unique

        logger.warning(
            f"Browse page returned no slugs (attempt {attempt}/{attempts}): {browse_url}"
        )

    logger.error(f"Slug discovery failed after {attempts} attempts: {browse_url}")
    return []


def _extract_numeric_id(slug: str) -> Optional[int]:
    """Visit a legislation page and extract the numeric download ID."""
    url = f"{BASE_URL}/en/Legislation/{slug}"
    data = _fetch_url(url)
    if not data:
        return None

    html = data.decode("utf-8", errors="replace")
    # Acts and subordinate legislation both download via the Act endpoint,
    # but accept the SubordinateLegislation spelling in case that changes.
    match = re.search(
        r'/api/sitecore/(?:Act|SubordinateLegislation)/(?:PDF|Word)\?id=(\d+)', html
    )
    if match:
        return int(match.group(1))

    logger.debug(f"No numeric ID found for {slug}")
    return None


def _clean(text: str) -> str:
    """Drop trailing whitespace and runs of blank lines left by PDF extraction."""
    lines = [line.rstrip() for line in text.splitlines()]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract full text from PDF bytes: PyMuPDF, then pdfplumber, then pypdf.

    PyMuPDF is tried first because it is a C extension that releases the GIL.
    pdfplumber is pure Python, so with five worker threads its extraction
    serializes on the GIL and becomes the throughput ceiling — on a 398-page
    consolidated Act it takes ~14 s against PyMuPDF's ~0.5 s.
    """
    try:
        import fitz  # PyMuPDF
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = _clean("\n".join(page.get_text() for page in doc))
        if text:
            return text
    except Exception as e:
        logger.debug(f"PyMuPDF failed: {e}")

    try:
        import pdfplumber
        pages = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # Flush each page's cached layout objects: keeping them for a
            # 500-page consolidated Act is what OOM-kills fleet workers.
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
                page.flush_cache()
        text = _clean("\n\n".join(p for p in pages if p.strip()))
        if text:
            return text
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")

    # Fallback: pypdf
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = _clean("\n\n".join(p for p in pages if p.strip()))
        if text:
            return text
    except Exception as e:
        logger.debug(f"pypdf failed: {e}")

    return ""


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# NT consolidations carry a currency line, e.g. "As in force at 3 November 2025".
_IN_FORCE_RE = re.compile(
    r"As in force at\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", re.IGNORECASE
)


def _extract_in_force_date(text: str) -> Optional[str]:
    """Parse the 'As in force at <date>' currency line into ISO 8601."""
    match = _IN_FORCE_RE.search(text[:4000])
    if not match:
        return None
    day, month_name, year = match.groups()
    month = _MONTHS.get(month_name.lower())
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day)).date().isoformat()
    except ValueError:
        return None


def _human_title(slug: str) -> str:
    """Convert a slug like 'CRIMINAL-CODE-ACT-1983' to 'Criminal Code Act 1983'."""
    return slug.replace("-", " ").title()


class NTLegislationScraper(BaseScraper):
    """
    Scraper for AU/NTLegislation -- Northern Territory Legislation Register.
    Country: AU
    URL: https://legislation.nt.gov.au/

    Data types: legislation
    Auth: none (free public access)
    """

    def __init__(self, resume: bool = True):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        # Skip slugs already in data/records.jsonl. Disabled for sample mode,
        # which would otherwise yield nothing once a full run has completed.
        self.resume = resume

    def _completed_slugs(self) -> set:
        """Slugs already written to data/records.jsonl, for resume after a stall."""
        records_path = Path(__file__).parent / "data" / "records.jsonl"
        done = set()
        if not records_path.exists():
            return done
        with open(records_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                slug = rec.get("slug") or rec.get("_id")
                if slug:
                    done.add(slug)
        return done

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Resolve, download and extract one document, then map it to the schema.

        The download happens here rather than in ``fetch_all`` so that
        ``bootstrap_fast`` overlaps the slow on-demand PDF renders across
        worker threads.
        """
        slug = raw["slug"]

        numeric_id = raw.get("numeric_id") or _extract_numeric_id(slug)
        if numeric_id is None:
            logger.warning(f"Could not resolve numeric ID for {slug}, skipping")
            return None

        pdf_url = f"{BASE_URL}/api/sitecore/Act/PDF?id={numeric_id}"
        data = _fetch_url(pdf_url, timeout=PDF_TIMEOUT)
        if not data or len(data) < 500:
            logger.warning(f"PDF too small or missing for {slug} (id={numeric_id})")
            return None

        text = _extract_text_from_pdf(data)
        if len(text) < 100:
            logger.warning(f"Insufficient text from {slug}: {len(text)} chars")
            return None

        # Prefer the consolidation currency date; fall back to the year in the
        # slug (e.g. 'CRIMINAL-CODE-ACT-1983' -> '1983').
        date = _extract_in_force_date(text)
        if not date:
            year_match = re.search(r"(\d{4})$", slug)
            date = year_match.group(1) if year_match else None

        return {
            "_id": slug,
            "_source": "AU/NTLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or _human_title(slug),
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/en/Legislation/{slug}",
            "slug": slug,
            "numeric_id": numeric_id,
            "category": raw.get("category", "act"),
            "pdf_url": pdf_url,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield one lightweight descriptor per NT legislation document."""
        act_slugs = _discover_slugs(BROWSE_ACTS)
        sl_slugs = _discover_slugs(BROWSE_SL)

        # Both listings are known-populated; an empty one means discovery broke,
        # and silently crawling half the register would look like success.
        if not act_slugs or not sl_slugs:
            raise RuntimeError(
                f"Slug discovery incomplete: {len(act_slugs)} Acts, "
                f"{len(sl_slugs)} subordinate — refusing to report a truncated "
                "corpus as success"
            )

        # Merge and deduplicate (some may appear in both lists)
        seen = set()
        all_slugs = []
        for slug, category in (
            [(s, "act") for s in act_slugs] + [(s, "subordinate") for s in sl_slugs]
        ):
            if slug not in seen:
                seen.add(slug)
                all_slugs.append((slug, category))

        logger.info(
            f"Total unique slugs: {len(all_slugs)} "
            f"({len(act_slugs)} Acts + {len(sl_slugs)} SL)"
        )

        done = self._completed_slugs() if self.resume else set()
        if done:
            logger.info(f"Resuming: {len(done)} slugs already in data/records.jsonl")

        for slug, category in all_slugs:
            if slug in done:
                continue
            yield {
                "slug": slug,
                "category": category,
                "title": _human_title(slug),
            }

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all documents (no incremental API available)."""
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/NTLegislation data fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"]
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--workers", type=int, default=5, help="Concurrent downloads")
    args = parser.parse_args()

    scraper = NTLegislationScraper(resume=not args.sample)

    if args.command == "test":
        logger.info("Testing numeric ID extraction...")
        nid = _extract_numeric_id("CRIMINAL-CODE-ACT-1983")
        if nid:
            logger.info(f"OK — CRIMINAL-CODE-ACT-1983 has ID {nid}")
        else:
            logger.error("FAILED — could not extract ID for CRIMINAL-CODE-ACT-1983")
            sys.exit(1)

        logger.info("Testing PDF download and text extraction...")
        doc = scraper.normalize({"slug": "CRIMINAL-CODE-ACT-1983", "numeric_id": nid})
        if doc and len(doc["text"]) > 1000:
            logger.info(
                f"OK — {doc['title']}: {len(doc['text'])} chars, date={doc['date']}"
            )
        else:
            logger.error("FAILED — could not extract text from PDF")
            sys.exit(1)

        logger.info("Testing browse listing...")
        slugs = _discover_slugs(BROWSE_ACTS)
        logger.info(f"OK — discovered {len(slugs)} Acts")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast(max_workers=args.workers)
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
