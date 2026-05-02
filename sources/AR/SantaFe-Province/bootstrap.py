#!/usr/bin/env python3
"""
AR/SantaFe-Province -- Santa Fe Province Legislation Data Fetcher

Fetches legislation from the Sistema de Información de Normativa (SIN)
of Santa Fe Province, Argentina.

Strategy:
  - POST search to busqueda.php with document type filter (paginated, 10/page)
  - Parse HTML table for metadata and detail page links
  - Fetch item.php for full metadata
  - Download PDF via getFile.php and extract text with pdfplumber

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
from html import unescape
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

SOURCE_ID = "AR/SantaFe-Province"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.SantaFe-Province")

BASE_URL = "https://www.santafe.gov.ar/normativa/"
SEARCH_URL = BASE_URL + "src/busqueda.php?organismo=&tbusqueda="
ITEM_URL = BASE_URL + "item.php"
FILE_URL = BASE_URL + "getFile.php"

NORM_TYPES = {
    "1": "Ley",
    "2": "Decreto",
    "4": "Disposición",
    "5": "Resolución",
    "6": "Dictamen",
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
})


def init_session():
    """Visit main page to get session cookie."""
    r = session.get(BASE_URL, timeout=30)
    r.raise_for_status()
    logger.info("Session initialized, cookies: %s", dict(session.cookies))


def clean_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date_ar(date_str: str) -> Optional[str]:
    if not date_str:
        return None
    for fmt in ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def search_norms(tipo: str, page: int = 1) -> tuple[list[dict], int]:
    """
    Search SIN for norms of given type.
    Returns (list of result dicts, total count).
    """
    data = {
        "action": "buscar",
        "tipoNorma": tipo,
        "organismoSelect": "",
        "numNorma": "",
        "anio": "",
        "numExpediente": "",
        "frase": "cualquiera",
        "iniciador": "",
        "fechaDesde": "",
        "fechaHasta": "",
        "pagina": str(page),
        "ordenarPor": "2",
        "ordenBusqueda": "DESC",
    }
    r = session.post(
        SEARCH_URL,
        data=data,
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": BASE_URL + "index.php",
        },
        timeout=30,
    )
    r.raise_for_status()

    total = 0
    total_match = re.search(r'(\d[\d.]*)\s*resultados', r.text)
    if total_match:
        total = int(total_match.group(1).replace(".", ""))

    results = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', r.text, re.DOTALL)
    for row in rows:
        link_match = re.search(
            r'href="item\.php\?id=(\d+)&(?:amp;)?cod=([a-f0-9]+)"', row
        )
        if not link_match:
            continue
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 4:
            continue
        item_id = link_match.group(1)
        item_cod = link_match.group(2)
        results.append({
            "item_id": item_id,
            "item_cod": item_cod,
            "number": clean_html(cells[0]).strip(),
            "norm_type_label": clean_html(cells[1]).strip(),
            "description": clean_html(cells[2]).strip(),
            "date_str": clean_html(cells[3]).strip(),
        })

    return results, total


def fetch_detail(item_id: str, item_cod: str) -> dict:
    """Fetch detail page and extract metadata + PDF download info."""
    url = f"{ITEM_URL}?id={item_id}&cod={item_cod}"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    html = r.text

    meta = {"item_id": item_id, "item_cod": item_cod, "detail_url": url}

    # Number
    m = re.search(r'<label>Número:\s*</label>\s*<span>([^<]+)</span>', html)
    if m:
        meta["number"] = m.group(1).strip()

    # Date
    m = re.search(r'<label>Fecha:\s*</label>\s*<span>([^<]+)</span>', html)
    if m:
        meta["date_str"] = m.group(1).strip()

    # Firmantes
    m = re.search(r'<label>Firmantes:\s*</label>\s*<span>([^<]+)</span>', html)
    if m:
        meta["firmantes"] = m.group(1).strip()

    # Temas
    m = re.search(r'<label>Temas:\s*</label>\s*<span>([^<]+)</span>', html)
    if m:
        meta["temas"] = m.group(1).strip()

    # Jurisdicción
    m = re.search(r'<label>Jurisdicci[^<]*</label>\s*<span>([^<]+)</span>', html)
    if m:
        meta["jurisdiction"] = m.group(1).strip()

    # Description
    m = re.search(r'<label>Descripci[^<]*</label>\s*<span>(.*?)</span>', html, re.DOTALL)
    if m:
        meta["description"] = clean_html(m.group(1)).strip()

    # Modifica a (references)
    refs = re.findall(
        r'<a[^>]*href="item\.php\?id=(\d+)[^"]*"[^>]*>([^<]+)</a>',
        html
    )
    if refs:
        meta["modifica_a"] = [{"id": r[0], "label": r[1].strip()} for r in refs]

    # PDF file links
    pdf_links = re.findall(
        r'href="getFile\.php\?id=(\d+)&(?:amp;)?item=(\d+)&(?:amp;)?cod=([a-f0-9]+)"',
        html
    )
    if pdf_links:
        meta["pdf_files"] = [
            {"file_id": p[0], "item_id": p[1], "cod": p[2]}
            for p in pdf_links
        ]

    # PDF filenames
    filenames = re.findall(r'<td>([^<]+\.pdf)</td>', html, re.IGNORECASE)
    if filenames:
        meta["pdf_filenames"] = filenames

    return meta


def download_pdf_text(file_id: str, item_id: str, cod: str) -> Optional[str]:
    """Download a PDF and extract text."""
    if pdfplumber is None:
        logger.error("pdfplumber not installed — cannot extract PDF text")
        return None

    url = f"{FILE_URL}?id={file_id}&item={item_id}&cod={cod}"
    try:
        r = session.get(url, timeout=60)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "pdf" not in ct and not r.content[:5].startswith(b"%PDF"):
            logger.warning("Not a PDF (Content-Type: %s)", ct)
            return None

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(r.content)
            tmp_path = tmp.name

        text_parts = []
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)

        Path(tmp_path).unlink(missing_ok=True)

        full_text = "\n\n".join(text_parts)
        return full_text if len(full_text) > 20 else None

    except Exception as e:
        logger.warning("PDF extraction failed for file %s: %s", file_id, e)
        return None


def normalize(meta: dict, full_text: str) -> dict:
    norm_type = meta.get("norm_type_label", "").upper()
    number = meta.get("number", "")
    date = parse_date_ar(meta.get("date_str", ""))

    type_slug = norm_type.lower().replace("á", "a").replace("ó", "o")
    type_slug = re.sub(r'[^a-z0-9]', '', type_slug)
    doc_id = f"ar-sf-{type_slug}-{number}".lower()
    doc_id = re.sub(r'[^a-z0-9-]', '-', doc_id)

    title = meta.get("description", "")
    if not title:
        title = f"{norm_type} {number}" if number else f"Norma {meta.get('item_id', '')}"

    detail_url = meta.get("detail_url", "")

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"{norm_type} {number} — {title}" if number else title,
        "text": full_text,
        "date": date,
        "url": detail_url,
        "norm_type": norm_type,
        "number": number,
        "firmantes": meta.get("firmantes", ""),
        "temas": meta.get("temas", ""),
        "jurisdiction": meta.get("jurisdiction", ""),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized records."""
    init_session()
    tipos = ["1"] if sample else list(NORM_TYPES.keys())
    max_records = 15 if sample else None
    count = 0

    for tipo in tipos:
        page = 1
        tipo_label = NORM_TYPES[tipo]
        logger.info("Fetching %s (type=%s)", tipo_label, tipo)

        while True:
            if max_records and count >= max_records:
                return

            time.sleep(1.5)
            try:
                results, total = search_norms(tipo, page)
            except Exception as e:
                logger.error("Search failed for type=%s page=%d: %s", tipo, page, e)
                break

            if not results:
                break

            logger.info("Type %s page %d: %d results (total=%d)", tipo_label, page, len(results), total)

            for res in results:
                if max_records and count >= max_records:
                    return

                time.sleep(1.5)
                try:
                    detail = fetch_detail(res["item_id"], res["item_cod"])
                except Exception as e:
                    logger.warning("Detail fetch failed for item %s: %s", res["item_id"], e)
                    continue

                detail["norm_type_label"] = res.get("norm_type_label", NORM_TYPES.get(tipo, ""))

                pdf_files = detail.get("pdf_files", [])
                if not pdf_files:
                    logger.warning("No PDF for item %s (%s %s)", res["item_id"], tipo_label, res.get("number", ""))
                    continue

                time.sleep(1.0)
                text = download_pdf_text(
                    pdf_files[0]["file_id"],
                    pdf_files[0]["item_id"],
                    pdf_files[0]["cod"],
                )
                if not text:
                    logger.warning("No text extracted for item %s", res["item_id"])
                    continue

                record = normalize(detail, text)
                if record["text"] and len(record["text"]) > 50:
                    count += 1
                    logger.info("Record %d: %s (%d chars)", count, record["title"][:80], len(record["text"]))
                    yield record

            total_pages = (total + 9) // 10
            if page >= total_pages:
                break
            page += 1


def test_api():
    """Quick connectivity test."""
    init_session()
    results, total = search_norms("1", 1)
    print(f"Search OK: {len(results)} results on page 1, {total} total laws")

    if results:
        res = results[0]
        print(f"First result: {res['norm_type_label']} {res['number']} — {res['description'][:80]}")
        detail = fetch_detail(res["item_id"], res["item_cod"])
        print(f"Detail: number={detail.get('number')}, date={detail.get('date_str')}")
        print(f"PDF files: {len(detail.get('pdf_files', []))}")

        pdf_files = detail.get("pdf_files", [])
        if pdf_files:
            text = download_pdf_text(pdf_files[0]["file_id"], pdf_files[0]["item_id"], pdf_files[0]["cod"])
            if text:
                print(f"PDF text: {len(text)} chars")
                print(text[:300])
            else:
                print("PDF text extraction failed")


def main():
    parser = argparse.ArgumentParser(description="AR/SantaFe-Province data fetcher")
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
