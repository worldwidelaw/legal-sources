#!/usr/bin/env python3
"""
ET/AbyssiniaLaw -- Abyssinia Law Ethiopian Legal Information Portal

Fetches downloadable legal documents (cassation decision volumes,
constitutions, proclamations) from abyssinialaw.com.

Strategy:
  - Scrape category listing pages for document download links
  - Download each PDF and extract full text via pypdf
  - Covers: cassation decisions, constitutions, latest laws

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

# NOTE: `lib.neon_client` is only needed for the standalone `--full` upsert path
# below; it is absent on the fleet image, so import it lazily at point of use
# rather than at module load (the fleet's generic runner only calls fetch_all()). #1555

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ET/AbyssiniaLaw")

BASE_URL = "https://www.abyssinialaw.com"
SOURCE_ID = "ET/AbyssiniaLaw"

CATEGORY_PAGES = [
    {
        "name": "Federal Supreme Court Cassation Decisions",
        "path": "/decisions/federal-supreme-court-cassation-decisions",
        "link_pattern": r'/decisions/[^"]*?/download',
        "data_type": "case_law",
        "paginate": True,
    },
    {
        "name": "First Instance Court Commercial Bench Decisions",
        "path": "/decisions/first-instance-court-commercial-bench-decisions",
        "link_pattern": r'/decisions/[^"]*?/download',
        "data_type": "case_law",
        "paginate": True,
    },
    {
        "name": "Constitutions",
        "path": "/laws/constitutions",
        "link_pattern": r'/laws/constitutions/[^"]*?/download',
        "data_type": "legislation",
        "paginate": False,
    },
    {
        "name": "Latest Laws",
        "path": "/online-resources/codes-commentaries-and-explanatory-notes/latest-laws",
        "link_pattern": r'/online-resources/[^"]*?/download',
        "data_type": "legislation",
        "paginate": False,
    },
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pypdf."""
    import io
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        return "\n\n".join(pages_text)
    except Exception as e:
        log.warning(f"pypdf extraction failed: {e}")
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages_text)
    except Exception as e:
        log.warning(f"pdfplumber extraction failed: {e}")
    return ""


def _get_page_count(pdf_bytes: bytes) -> int:
    """Get number of pages in PDF."""
    import io
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        return len(reader.pages)
    except Exception:
        return 0


def _fetch_category_links(category: dict) -> List[Tuple[str, str]]:
    """Fetch all download links from a category page. Returns [(download_url, title)]."""
    results = []
    start = 0
    while True:
        url = BASE_URL + category["path"]
        if start > 0:
            url += f"?start={start}"

        log.info(f"Fetching category page: {url}")
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Find download links
        pattern = category["link_pattern"]
        download_paths = re.findall(pattern, html)
        download_paths = list(set(download_paths))

        if not download_paths:
            break

        # For each download link, extract the title from the slug
        for dl_path in download_paths:
            # The page link is the download path minus "/download"
            page_path = dl_path.replace("/download", "")
            slug = page_path.rstrip("/").split("/")[-1]
            # Convert slug to title
            title = slug.replace("-", " ").strip()
            title = " ".join(w.capitalize() if w not in ("of", "the", "and", "in", "to", "for", "a", "an") else w
                            for w in title.split())
            download_url = BASE_URL + dl_path
            results.append((download_url, title))

        if not category.get("paginate"):
            break

        # Check for next page
        next_start = start + 20
        if f"start={next_start}" in html:
            start = next_start
            time.sleep(1)
        else:
            break

    return results


def _make_id(category_name: str, slug: str) -> str:
    """Create a deterministic document ID."""
    prefix = {
        "Federal Supreme Court Cassation Decisions": "ET-ABYS-CASS",
        "First Instance Court Commercial Bench Decisions": "ET-ABYS-FICB",
        "Constitutions": "ET-ABYS-CONST",
        "Latest Laws": "ET-ABYS-LAW",
    }.get(category_name, "ET-ABYS")
    # Clean slug
    clean = re.sub(r'[^a-z0-9]+', '-', slug.lower()).strip('-')
    if len(clean) > 60:
        clean = clean[:60]
    return f"{prefix}-{clean}"


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw document data into standard schema."""
    return {
        "_id": raw["_id"],
        "_source": SOURCE_ID,
        "_type": raw.get("data_type", "legislation"),
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw["text"],
        "date": None,
        "url": raw["url"],
        "category": raw.get("category", ""),
        "section": raw.get("section", ""),
        "pages": raw.get("pages", 0),
    }


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    """Yield all documents with full text from PDF extraction."""
    count = 0
    sample_limit = 15 if sample else float("inf")

    for category in CATEGORY_PAGES:
        if count >= sample_limit:
            break

        log.info(f"Processing category: {category['name']}")
        links = _fetch_category_links(category)
        log.info(f"  Found {len(links)} download links")

        # Filter out table-of-contents documents (less useful)
        if not sample:
            links = [(url, title) for url, title in links
                     if "table-of-content" not in url.lower() and "table-of-contents" not in url.lower()]

        for download_url, title in links:
            if count >= sample_limit:
                break

            slug = download_url.replace("/download", "").rstrip("/").split("/")[-1]
            doc_id = _make_id(category["name"], slug)

            log.info(f"  Downloading: {title} ({download_url})")
            try:
                resp = SESSION.get(download_url, timeout=120)
                resp.raise_for_status()

                content_type = resp.headers.get("Content-Type", "")
                if "pdf" not in content_type.lower() and len(resp.content) < 1000:
                    log.warning(f"  Skipping {title}: not a PDF (Content-Type: {content_type})")
                    continue

                pdf_bytes = resp.content
                pages = _get_page_count(pdf_bytes)
                text = _extract_pdf_text(pdf_bytes)

                if not text or len(text.strip()) < 50:
                    log.warning(f"  Skipping {title}: insufficient text extracted ({len(text)} chars)")
                    continue

                raw = {
                    "_id": doc_id,
                    "title": title,
                    "text": text,
                    "url": download_url,
                    "data_type": category["data_type"],
                    "category": category["name"],
                    "section": category["path"],
                    "pages": pages,
                }

                record = normalize(raw)
                log.info(f"  OK: {title} — {pages} pages, {len(text)} chars")
                yield record
                count += 1

            except requests.RequestException as e:
                log.warning(f"  Failed to download {title}: {e}")
                continue

            time.sleep(1.5)  # Rate limit

    log.info(f"Total documents yielded: {count}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """No date-based filtering available; re-fetch all."""
    yield from fetch_all()


def test_connectivity():
    """Quick test to verify the site is accessible."""
    resp = SESSION.get(BASE_URL, timeout=15)
    resp.raise_for_status()
    assert "abyssinia" in resp.text.lower() or "law" in resp.text.lower()
    log.info(f"Connectivity OK: {resp.status_code}, {len(resp.text)} bytes")

    # Test a download link
    test_url = BASE_URL + "/laws/constitutions/the-1995-ethiopian-constitution-english-and-amharic-version/download"
    resp2 = SESSION.get(test_url, timeout=30, stream=True)
    resp2.raise_for_status()
    ct = resp2.headers.get("Content-Type", "")
    log.info(f"PDF download OK: {resp2.status_code}, Content-Type: {ct}")
    resp2.close()


# ── CLI entry point ──────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="ET/AbyssiniaLaw bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Push to Neon DB")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    records = []
    for record in fetch_all(sample=args.sample):
        records.append(record)
        # Save sample files
        if args.sample or not args.full:
            fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

    log.info(f"Total records: {len(records)}")

    if args.full and records:
        log.info(f"Upserting {len(records)} records to Neon...")
        from lib.neon_client import upsert_records  # lazy: standalone-only dep (#1555)
        upsert_records(records)
        log.info("Upsert complete.")
    elif records:
        # Validate sample
        texts = [r.get("text", "") for r in records]
        non_empty = sum(1 for t in texts if t and len(t) > 50)
        log.info(f"Validation: {non_empty}/{len(records)} records have substantial text")
        avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
        log.info(f"Average text length: {avg_len:.0f} chars")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
