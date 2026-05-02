#!/usr/bin/env python3
"""
BR/SUSEP -- Brazilian Insurance Regulator (Superintendência de Seguros Privados)

Fetches normative acts (circulares, resoluções, portarias, etc.) from SUSEP's
BNWeb API at www2.susep.gov.br.

Strategy:
  - GET search API (BNWeb bnmapi.exe) to list all norms with metadata
  - For each norm, download the "Versão Original" PDF via upload/{cod_anexo}
  - Extract text from PDF with pdfplumber

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

# Try common pdf_extract module
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(PROJECT_ROOT))
    from common.pdf_extract import extract_pdf_markdown
except ImportError:
    extract_pdf_markdown = None

SOURCE_ID = "BR/SUSEP"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.SUSEP")

API_BASE = "https://www2.susep.gov.br/safe/scripts/bnweb/bnmapi.exe?router="
SEARCH_URL = API_BASE + "search"
UPLOAD_URL = API_BASE + "upload/"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
})
session.verify = False  # SUSEP cert chain sometimes incomplete

# Suppress SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PAGE_SIZE = 48  # Max supported by BNWeb


def search_norms(exp: str = "", page: int = 1, limit: int = PAGE_SIZE,
                 order: str = "datadoc desc,codigo desc") -> dict:
    """Search BNWeb API for normative acts."""
    params = {
        "exp": exp,
        "page": str(page),
        "limit": str(limit),
        "order": order,
    }
    url = SEARCH_URL + "&" + "&".join(f"{k}={v}" for k, v in params.items())
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def download_pdf_text(cod_anexo: int) -> Optional[str]:
    """Download a PDF by its cod_anexo and extract text."""
    url = UPLOAD_URL + str(cod_anexo)
    try:
        r = session.get(url, timeout=60)
        r.raise_for_status()

        ct = r.headers.get("Content-Type", "")
        if "pdf" not in ct.lower() and not r.content[:5].startswith(b"%PDF"):
            logger.warning("Not a PDF for anexo %d (Content-Type: %s)", cod_anexo, ct)
            return None

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(r.content)
            tmp_path = tmp.name

        # Try common pdf_extract first, then pdfplumber
        text = None
        if extract_pdf_markdown:
            try:
                text = extract_pdf_markdown(tmp_path)
            except Exception:
                pass

        if not text and pdfplumber:
            text_parts = []
            with pdfplumber.open(tmp_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
            text = "\n\n".join(text_parts)

        Path(tmp_path).unlink(missing_ok=True)

        if text and len(text.strip()) > 20:
            return text.strip()
        return None

    except Exception as e:
        logger.warning("PDF extraction failed for anexo %d: %s", cod_anexo, e)
        return None


def parse_date(date_str: str) -> Optional[str]:
    """Parse Brazilian date format to ISO 8601."""
    if not date_str:
        return None
    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def get_original_pdf_anexo(item: dict) -> Optional[int]:
    """Get the cod_anexo for the 'Versão Original' PDF, or first PDF."""
    anexos = item.get("anexos", [])
    if not anexos:
        return None

    # Prefer "Versão Original"
    for a in anexos:
        if not a.get("cod_fonte") and "original" in a.get("descricao", "").lower():
            return a["cod_anexo"]

    # Fall back to first own (non-source) PDF
    for a in anexos:
        if not a.get("cod_fonte") and a.get("extensao", "").upper() == ".PDF":
            return a["cod_anexo"]

    # Fall back to any PDF
    for a in anexos:
        if a.get("extensao", "").upper() == ".PDF":
            return a["cod_anexo"]

    return None


def normalize(item: dict, full_text: str) -> dict:
    """Normalize a BNWeb record to standard schema."""
    codigo = item.get("codigo", "")
    especie = item.get("nome_especie", "")
    numero = item.get("numero", "")
    titulo = item.get("titulo", "")
    ementa = item.get("ementa", "")

    doc_id = f"br-susep-{codigo}"
    date = parse_date(item.get("data_documento", ""))

    title = titulo or f"{especie} {numero}".strip()
    if ementa and ementa not in title:
        title = f"{title} — {ementa}"

    subjects = [a["nome"] for a in item.get("assuntos", []) if a.get("nome")]

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": f"https://www2.susep.gov.br/safe/bnportal/internet/pt-br/detail/{codigo}",
        "norm_type": especie,
        "number": numero,
        "ementa": ementa,
        "publication": item.get("publicacao", ""),
        "subjects": subjects,
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized records."""
    max_records = 15 if sample else None
    count = 0
    page = 1

    # Get total count first
    result = search_norms(exp="", page=1, limit=1)
    total = result.get("pagination", {}).get("total", 0)
    logger.info("Total norms in SUSEP BNWeb: %d", total)

    while True:
        if max_records and count >= max_records:
            return

        time.sleep(1.5)
        try:
            result = search_norms(exp="", page=page, limit=PAGE_SIZE)
        except Exception as e:
            logger.error("Search failed page %d: %s", page, e)
            break

        items = result.get("data", [])
        if not items:
            break

        pagination = result.get("pagination", {})
        total_pages = (pagination.get("total", 0) + PAGE_SIZE - 1) // PAGE_SIZE
        logger.info("Page %d/%d: %d items", page, total_pages, len(items))

        for item in items:
            if max_records and count >= max_records:
                return

            cod_anexo = get_original_pdf_anexo(item)
            if not cod_anexo:
                logger.debug("No PDF for item %s (%s)", item.get("codigo"), item.get("titulo", ""))
                continue

            time.sleep(1.0)
            text = download_pdf_text(cod_anexo)
            if not text:
                logger.warning("No text for item %s (anexo %d)", item.get("codigo"), cod_anexo)
                continue

            record = normalize(item, text)
            if record["text"] and len(record["text"]) > 50:
                count += 1
                logger.info("Record %d: %s (%d chars)", count, record["title"][:80], len(record["text"]))
                yield record

        page += 1
        if page > total_pages:
            break


def test_api():
    """Quick connectivity test."""
    result = search_norms(exp="", page=1, limit=3)
    total = result.get("pagination", {}).get("total", 0)
    items = result.get("data", [])
    print(f"Search OK: {len(items)} items, {total} total norms")

    if items:
        item = items[0]
        print(f"First: {item.get('nome_especie', '')} {item.get('numero', '')} — {item.get('titulo', '')}")
        print(f"Date: {item.get('data_documento', '')}")
        print(f"Anexos: {len(item.get('anexos', []))}")

        cod_anexo = get_original_pdf_anexo(item)
        if cod_anexo:
            text = download_pdf_text(cod_anexo)
            if text:
                print(f"PDF text: {len(text)} chars")
                print(text[:300])
            else:
                print("PDF text extraction failed")


def main():
    parser = argparse.ArgumentParser(description="BR/SUSEP data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
        return

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    records = []

    for record in fetch_all(sample=args.sample):
        records.append(record)
        if args.sample:
            out = SAMPLE_DIR / f"{record['_id']}.json"
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2))

    logger.info("Total records: %d", len(records))

    if args.sample:
        texts = [r for r in records if r.get("text") and len(r["text"]) > 50]
        print(f"\n=== SAMPLE SUMMARY ===")
        print(f"Records saved: {len(records)}")
        print(f"Records with text: {len(texts)}")
        if texts:
            avg_len = sum(len(r["text"]) for r in texts) // len(texts)
            print(f"Average text length: {avg_len} chars")
            print(f"Sample titles:")
            for r in texts[:5]:
                print(f"  - {r['title'][:100]}")


if __name__ == "__main__":
    main()
