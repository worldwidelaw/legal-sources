#!/usr/bin/env python3
"""
KY/Legislation -- Cayman Islands Legislation (legislation.gov.ky)

Fetches consolidated Acts, subordinate legislation and amending instruments from
the official Cayman Islands legislation portal.

The portal's Apache directory listings under /cms/images/LEGISLATION/ used to be
browsable; they now return an empty index, so enumeration goes through the CMS
index pages instead:

  * /cms/legislation/current/by-title.html  -- POSTed once per letter A-Z
    (``submit4`` is the alphabet filter). Each row carries the current version
    PDF plus a "legislation history" modal listing every earlier revision.
  * /cms/legislation/repealed.html
  * /cms/legislation/revoked-secondary-legislation.html
  * /cms/legislation/not-in-force-menu.html

Every distinct PDF found on those pages is one document. The ``_g.pdf`` twins
are the gazette-typeset reprint of the same text and are skipped as duplicates.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for the full pull (fleet runner)
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import html as html_mod
import logging
import re
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.KY.Legislation")

BASE_URL = "https://legislation.gov.ky"
BY_TITLE_PATH = "/cms/legislation/current/by-title.html"
STATIC_INDEX_PATHS = [
    "/cms/legislation/repealed.html",
    "/cms/legislation/revoked-secondary-legislation.html",
    "/cms/legislation/not-in-force-menu.html",
]
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

PDF_HREF_RE = re.compile(r'href="(/cms/images/LEGISLATION/[^"]+\.pdf)"')
MODAL_RE = re.compile(r'<div class="modal fade"(.*?)(?=<div class="modal fade"|\Z)', re.S)
MODAL_TITLE_RE = re.compile(r'id="myModalLabel">\s*(.*?)\s*</h5>', re.S)
NPWRAP_RE = re.compile(r'class="npWrap" href="([^"]+)"[^>]*>(.*?)</a>', re.S)
TAG_RE = re.compile(r"<[^>]+>")

# /cms/images/LEGISLATION/PRINCIPAL/1966/1966-0005/1966-0005_1997 Revision.pdf
PDF_PATH_RE = re.compile(
    r"^/cms/images/LEGISLATION/(?P<category>[A-Z]+)/(?P<year_dir>[^/]+)/"
    r"(?P<item>[^/]+)/(?P<filename>.+)\.pdf$"
)

CATEGORY_TYPES = {
    "PRINCIPAL": "principal",
    "SUBORDINATE": "subordinate",
    "AMENDING": "amending",
}


def _clean_text(fragment: str) -> str:
    """Strip tags/entities from an HTML fragment and normalise whitespace."""
    text = html_mod.unescape(TAG_RE.sub("", fragment))
    text = re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
    # Amendment rows prefix the heading with "amended by...".
    return re.sub(r"^amended by\.*\s*", "", text, flags=re.IGNORECASE).strip()


def _is_duplicate_variant(pdf_path: str) -> bool:
    """The `_g.pdf` files are the gazette reprint of the same consolidated text."""
    return pdf_path.lower().endswith("_g.pdf")


def _parse_index_page(html: str) -> Dict[str, str]:
    """Map every PDF path on an index page to the best available title."""
    found: Dict[str, str] = {}

    # History modals hold every revision of an item under one heading.
    for match in MODAL_RE.finditer(html):
        block = match.group(1)
        title_match = MODAL_TITLE_RE.search(block)
        title = _clean_text(title_match.group(1)) if title_match else ""
        for pdf_path in PDF_HREF_RE.findall(block):
            if _is_duplicate_variant(pdf_path):
                continue
            if title and not found.get(pdf_path):
                found[pdf_path] = title
            else:
                found.setdefault(pdf_path, title)

    # The row anchors carry the in-force version and its display title.
    for pdf_path, label in NPWRAP_RE.findall(html):
        if not pdf_path.startswith("/cms/images/LEGISLATION/"):
            continue
        if _is_duplicate_variant(pdf_path):
            continue
        title = _clean_text(label)
        # Trim the trailing "[2024 Revision]" version marker from the anchor.
        title = re.sub(r"\s*\[[^\]]*\]\s*$", "", title).strip()
        if title:
            found[pdf_path] = title
        else:
            found.setdefault(pdf_path, "")

    # Anything linked outside a modal or row anchor (e.g. plain listing pages).
    for pdf_path in PDF_HREF_RE.findall(html):
        if not _is_duplicate_variant(pdf_path):
            found.setdefault(pdf_path, "")

    return found


def _split_pdf_path(pdf_path: str) -> Optional[Tuple[str, str, str]]:
    """Return (category, item_id, version_label) for a legislation PDF path."""
    match = PDF_PATH_RE.match(pdf_path)
    if not match:
        return None
    category = match.group("category")
    item = urllib.parse.unquote(match.group("item")).strip()
    filename = urllib.parse.unquote(match.group("filename")).strip()
    version = filename
    if version.startswith(item):
        version = version[len(item):].lstrip("_- ").strip()
    return category, item, version


def _version_year(version_label: str, item_id: str) -> Optional[str]:
    """Best-effort year for a version: revision year, act year, then item year."""
    for pattern in (r"(\d{4})\s*Revision", r"\bof\s+(\d{4})\b", r"\b(19|20)\d{2}\b"):
        match = re.search(pattern, version_label, re.IGNORECASE)
        if match:
            candidate = match.group(0)
            year = re.search(r"(19|20)\d{2}", candidate)
            if year:
                return year.group(0)
    match = re.match(r"^(\d{4})", item_id)
    return match.group(1) if match else None


def _crawl_order(pdf_paths) -> List[str]:
    """Newest revision of every item first, principal Acts ahead of the rest.

    An item like the Companies Act carries ~20 revisions; walking them in path
    order would spend the whole run inside one Act. Sweeping one version per
    item at a time keeps a truncated run broad instead of deep.
    """
    category_rank = {"PRINCIPAL": 0, "SUBORDINATE": 1, "AMENDING": 2}
    by_item: Dict[Tuple[int, str], List[Tuple[str, str]]] = {}
    for path in pdf_paths:
        parts = _split_pdf_path(path)
        if not parts:
            by_item.setdefault((3, path), []).append(("", path))
            continue
        category, item_id, version = parts
        key = (category_rank.get(category, 3), f"{category}/{item_id}")
        by_item.setdefault(key, []).append((_version_year(version, item_id) or "", path))

    ordered: List[Tuple[int, int, str]] = []
    for key in sorted(by_item):
        # Newest version first within each item.
        versions = sorted(by_item[key], key=lambda v: (v[0], v[1]), reverse=True)
        for depth, (_, path) in enumerate(versions):
            ordered.append((key[0], depth, path))
    return [path for _, _, path in sorted(ordered)]


def _title_from_pdf_text(text: str) -> str:
    """Extract a title from the first lines of extracted PDF text."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    title_lines = []
    for line in lines[:10]:
        if re.match(r"^(Cayman Islands|CAYMAN ISLANDS|Page \d|Supplement)", line, re.IGNORECASE):
            continue
        if re.match(r"^\d{4}\s+Revision$", line, re.IGNORECASE):
            continue
        if re.match(r"^(Published by|Printed and|Under the authority)", line, re.IGNORECASE):
            continue
        title_lines.append(line)
        if len(title_lines) >= 2:
            break
    return " ".join(title_lines) if title_lines else "Untitled"


class CaymanLegislationScraper(BaseScraper):
    """Scraper for KY/Legislation -- Cayman Islands Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    # ── discovery ────────────────────────────────────────────────────

    def _fetch_letter_page(self, letter: str) -> str:
        """POST the alphabet filter on the current-legislation index."""
        point_in_time = datetime.now(timezone.utc).strftime("%Y-%m-%d 00:00:00")
        self.rate_limiter.wait()
        resp = self.client.post(
            BY_TITLE_PATH,
            data={"submit4": letter, "pointintime_post": point_in_time},
        )
        resp.raise_for_status()
        return resp.text

    def _fetch_static_page(self, path: str) -> str:
        self.rate_limiter.wait()
        resp = self.client.get(path)
        resp.raise_for_status()
        return resp.text

    def discover(self) -> Dict[str, str]:
        """Return {pdf_path: title} for every legislation PDF on the portal."""
        found: Dict[str, str] = {}

        for letter in ALPHABET:
            try:
                page = self._fetch_letter_page(letter)
            except Exception as exc:
                logger.warning(f"Letter {letter}: index fetch failed: {exc}")
                continue
            page_hits = _parse_index_page(page)
            for pdf_path, title in page_hits.items():
                if title or pdf_path not in found:
                    found[pdf_path] = title or found.get(pdf_path, "")
            logger.info(f"Letter {letter}: {len(page_hits)} PDFs ({len(found)} total)")

        for path in STATIC_INDEX_PATHS:
            try:
                page = self._fetch_static_page(path)
            except Exception as exc:
                logger.warning(f"{path}: fetch failed: {exc}")
                continue
            page_hits = _parse_index_page(page)
            for pdf_path, title in page_hits.items():
                if title or pdf_path not in found:
                    found[pdf_path] = title or found.get(pdf_path, "")
            logger.info(f"{path}: {len(page_hits)} PDFs ({len(found)} total)")

        if not found:
            raise RuntimeError(
                "legislation.gov.ky enumeration produced 0 PDFs — the CMS index "
                "pages returned nothing usable (layout change or IP block)"
            )
        return found

    # ── fetching ─────────────────────────────────────────────────────

    def _download_pdf(self, pdf_path: str) -> Optional[bytes]:
        """Download a PDF file and return bytes."""
        quoted = urllib.parse.quote(pdf_path, safe="/")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(quoted)
            resp.raise_for_status()
            if resp.content and resp.content[:5] == b"%PDF-":
                return resp.content
            logger.warning(f"Not a valid PDF: {pdf_path}")
            return None
        except Exception as exc:
            logger.warning(f"Failed to download PDF {pdf_path}: {exc}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        title = raw.get("title") or _title_from_pdf_text(raw.get("text", ""))
        version = raw.get("version", "")
        if version and version.lower() not in title.lower():
            title = f"{title} [{version}]"

        return {
            "_id": raw["_id"],
            "_source": "KY/Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "legislation_id": raw.get("legislation_id", ""),
            "legislation_type": raw.get("legislation_type", ""),
            "version": version,
            "pdf_filename": raw.get("pdf_filename", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        catalogue = self.discover()
        logger.info(f"Discovered {len(catalogue)} legislation PDFs")

        count = 0
        errors = 0
        for pdf_path in _crawl_order(catalogue):
            parts = _split_pdf_path(pdf_path)
            if not parts:
                logger.warning(f"Unrecognised PDF path: {pdf_path}")
                errors += 1
                continue
            category, item_id, version = parts

            pdf_bytes = self._download_pdf(pdf_path)
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="KY/Legislation",
                source_id=item_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if len(text.strip()) < 50:
                logger.warning(f"Insufficient text for {pdf_path}: {len(text)} chars")
                errors += 1
                continue

            year = _version_year(version, item_id)
            slug = re.sub(r"[^A-Za-z0-9]+", "-", version).strip("-") or "current"

            yield {
                "_id": f"KY/Legislation/{category}/{item_id}/{slug}",
                "legislation_id": item_id,
                "legislation_type": CATEGORY_TYPES.get(category, category.lower()),
                "version": version,
                "title": catalogue.get(pdf_path, ""),
                "text": text,
                "date": f"{year}-01-01" if year else None,
                "pdf_filename": pdf_path.rsplit("/", 1)[-1],
                "pdf_url": BASE_URL + urllib.parse.quote(pdf_path, safe="/"),
            }
            count += 1

            if count % 50 == 0:
                logger.info(f"Progress: {count} records, {errors} errors")

        logger.info(f"Completed: {count} records, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = CaymanLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing alphabet-filtered index...")
        page = scraper._fetch_letter_page("A")
        hits = _parse_index_page(page)
        if not hits:
            logger.error("FAILED — letter A returned no PDFs")
            sys.exit(1)
        logger.info(f"OK — {len(hits)} PDFs for letter A")

        pdf_path, title = sorted(hits.items())[0]
        pdf_bytes = scraper._download_pdf(pdf_path)
        if not pdf_bytes:
            logger.error(f"FAILED — could not download {pdf_path}")
            sys.exit(1)
        parts = _split_pdf_path(pdf_path)
        text = extract_pdf_markdown(
            source="KY/Legislation",
            source_id=parts[1] if parts else "test",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""
        logger.info(f"OK — {len(text)} chars from {title or pdf_path}")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command in ("bootstrap-fast", "update"):
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
