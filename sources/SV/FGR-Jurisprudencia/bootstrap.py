#!/usr/bin/env python3
"""
SV/FGR-Jurisprudencia -- El Salvador Attorney General Legislation & Jurisprudence

Fetches legislation and court rulings from the FGR portal via WordPress AJAX API
(busqueda-archivos plugin). Downloads PDFs and extracts full text via pdfminer.

Strategy:
  - Use 'materiasCategorias' action to walk the category tree
  - Use 'docsPorCategoria' action to list documents per leaf category
  - Download each PDF and extract text with pdfminer
  - Deduplicate by WordPress post ID

Usage:
  python bootstrap.py bootstrap          # Full initial pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 15 sample records -> sample/
  python bootstrap.py bootstrap-fast     # Alias for the full bootstrap (fleet entry point)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

try:
    import pdfplumber
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber", "-q"])
    import pdfplumber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SV.FGR-Jurisprudencia")

SOURCE_ID = "SV/FGR-Jurisprudencia"
AJAX_URL = "https://jurisprudenciaylegislacion.fgr.gob.sv/wp-admin/admin-ajax.php"

# Top-level category IDs and their data types
TOP_CATEGORIES = [
    {"id": "4", "name": "Legislación", "data_type": "legislation"},
    {"id": "5", "name": "Jurisprudencia", "data_type": "case_law"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

RATE_LIMIT = 1.5  # seconds between PDF downloads


def _get_session():
    import requests
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _ajax_post(session, action: str, data: dict, retries: int = 3):
    """Make an AJAX POST request to the WordPress admin-ajax endpoint."""
    payload = {"action": action, **data}
    for attempt in range(retries):
        try:
            r = session.post(AJAX_URL, data=payload, timeout=30)
            r.raise_for_status()
            result = r.json()
            return result if result is not None else []
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"AJAX retry {attempt+1}/{retries} for {action}: {e}")
                time.sleep(2)
            else:
                logger.error(f"AJAX failed after {retries} attempts for {action}: {e}")
                return []


def _walk_categories(session, parent_id: str, depth: int = 0) -> list:
    """Recursively walk category tree and collect all leaf category IDs."""
    subcats = _ajax_post(session, "materiasCategorias", {"id_cat": parent_id, "opc": ""})
    if not subcats:
        return [parent_id]

    leaf_ids = []
    for sc in subcats:
        cat_id = str(sc["term_id"])
        children = _ajax_post(session, "materiasCategorias", {"id_cat": cat_id, "opc": ""})
        if children:
            # Has children — recurse
            for child in children:
                leaf_ids.append(str(child["term_id"]))
        else:
            leaf_ids.append(cat_id)
    return leaf_ids


def _fetch_docs_for_category(session, cat_id: str) -> list:
    """Fetch all documents for a given category ID."""
    docs = _ajax_post(session, "docsPorCategoria", {"id_cat": cat_id, "opc": ""})
    return docs if docs else []


def _download_pdf_text(session, pdf_url: str) -> Optional[str]:
    """Download a PDF and extract text content using pdfplumber."""
    try:
        r = session.get(pdf_url, timeout=60)
        r.raise_for_status()
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"Not a PDF: {pdf_url}")
            return None
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            pages_text = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages_text.append(t)
                # Release per-page cache to avoid pdfplumber OOM (exit 137, #953)
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
            text = "\n\n".join(pages_text)
        if len(text.strip()) > 50:
            return text.strip()
        logger.warning(f"PDF text too short ({len(text)} chars): {pdf_url}")
        return None
    except Exception as e:
        logger.error(f"PDF download failed: {pdf_url}: {e}")
        return None


def _normalize(doc: dict, data_type: str, category_path: str) -> dict:
    """Normalize a raw document record."""
    wp_id = doc.get("ID", "")
    title = doc.get("post_title", "").strip()
    date_str = doc.get("post_date", "")
    pdf_url = doc.get("guid", "")
    page_url = doc.get("enlace", "")

    # Parse date
    date_iso = None
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            date_iso = dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            pass

    doc_id = f"SV-FGR-{wp_id}" if wp_id else hashlib.md5(
        (title + pdf_url).encode()
    ).hexdigest()[:12]

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": data_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "date": date_iso,
        "url": page_url or pdf_url,
        "pdf_url": pdf_url,
        "category": category_path,
        "text": "",  # filled in later
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents with full text extracted from PDFs."""
    session = _get_session()
    seen_ids = set()
    count = 0
    sample_limit = 15 if sample else 999999

    for top in TOP_CATEGORIES:
        top_id = top["id"]
        top_name = top["name"]
        data_type = top["data_type"]
        logger.info(f"Processing top category: {top_name} (id={top_id})")

        # Get subcategories
        subcats = _ajax_post(session, "materiasCategorias", {"id_cat": top_id, "opc": ""})
        if not subcats:
            continue

        for subcat in subcats:
            if count >= sample_limit:
                return
            sub_id = str(subcat["term_id"])
            sub_name = subcat.get("name", "").strip()
            logger.info(f"  Subcategory: {sub_name} (id={sub_id})")

            # Get direct docs in this subcategory
            docs = _fetch_docs_for_category(session, sub_id)
            for doc in docs:
                if count >= sample_limit:
                    return
                wp_id = doc.get("ID", "")
                if wp_id in seen_ids:
                    continue
                seen_ids.add(wp_id)

                record = _normalize(doc, data_type, f"{top_name} > {sub_name}")
                pdf_url = record.get("pdf_url", "")
                if pdf_url:
                    time.sleep(RATE_LIMIT)
                    text = _download_pdf_text(session, pdf_url)
                    if text:
                        record["text"] = text
                        count += 1
                        yield record
                    else:
                        logger.warning(f"Skipping {record['_id']}: no text from PDF")

            # Get leaf subcategories and their docs
            leaf_ids = _walk_categories(session, sub_id)
            for leaf_id in leaf_ids:
                if count >= sample_limit:
                    return
                if leaf_id == sub_id:
                    continue  # already fetched direct docs

                leaf_docs = _fetch_docs_for_category(session, leaf_id)
                for doc in leaf_docs:
                    if count >= sample_limit:
                        return
                    wp_id = doc.get("ID", "")
                    if wp_id in seen_ids:
                        continue
                    seen_ids.add(wp_id)

                    record = _normalize(doc, data_type, f"{top_name} > {sub_name}")
                    pdf_url = record.get("pdf_url", "")
                    if pdf_url:
                        time.sleep(RATE_LIMIT)
                        text = _download_pdf_text(session, pdf_url)
                        if text:
                            record["text"] = text
                            count += 1
                            yield record
                        else:
                            logger.warning(f"Skipping {record['_id']}: no text from PDF")

    logger.info(f"Total documents yielded: {count}")


def test():
    """Quick connectivity test."""
    session = _get_session()
    cats = _ajax_post(session, "materiasCategorias", {"id_cat": "4", "opc": ""})
    if cats:
        logger.info(f"OK: Found {len(cats)} legislation categories")
        # Test one doc
        subcats = _ajax_post(session, "materiasCategorias", {"id_cat": str(cats[0]["term_id"]), "opc": ""})
        if subcats:
            docs = _fetch_docs_for_category(session, str(subcats[0]["term_id"]))
            logger.info(f"OK: Found {len(docs)} docs in first sub-subcategory")
            if docs:
                pdf_url = docs[0].get("guid", "")
                if pdf_url:
                    text = _download_pdf_text(session, pdf_url)
                    if text:
                        logger.info(f"OK: PDF text extracted ({len(text)} chars)")
                    else:
                        logger.error("FAIL: Could not extract PDF text")
    else:
        logger.error("FAIL: Could not fetch categories")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SV/FGR-Jurisprudencia bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records (default for bootstrap)")
    args = parser.parse_args()

    if args.command == "test":
        test()
        return

    # bootstrap-fast is the fleet's entry point and must run the FULL crawl;
    # only --sample writes the 15-record sample/ set.
    is_sample = args.sample
    count = 0

    if is_sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in fetch_all(sample=True):
            count += 1
            with open(sample_dir / f"{record['_id']}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                f"[{count}] Saved {record['_id']}.json — {record['title'][:60]} "
                f"({len(record.get('text', ''))} chars)"
            )
        logger.info(f"Done. {count} sample records saved to {sample_dir}")
        return

    # Full crawl: stream to data/records.jsonl so the pipeline can ingest it.
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / "records.jsonl"
    with open(out_path, "w", encoding="utf-8") as out:
        for record in fetch_all(sample=False):
            count += 1
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()
            logger.info(
                f"[{count}] {record['title'][:60]} "
                f"({len(record.get('text', ''))} chars)"
            )

    if count == 0:
        logger.error(
            "No records written — the FGR AJAX endpoint returned nothing or "
            "refused this vantage. Refusing to report an empty crawl as success."
        )
        sys.exit(1)

    logger.info(f"Done. {count} records written to {out_path}")


if __name__ == "__main__":
    main()
