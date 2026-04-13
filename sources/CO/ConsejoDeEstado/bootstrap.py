#!/usr/bin/env python3
"""
CO/ConsejoDeEstado -- Colombia Council of State Jurisprudence

Fetches case-law decisions from the Consejo de Estado via its legacy
WebRelatoria JSF application (PrimeFaces). The strategy:

  1. POST search forms by year to the JSF search endpoint, paginating
     through results. Each result row carries a document ID (data-rk).
  2. For each document ID, fetch full HTML text via FileReferenceServlet.
  3. Strip HTML tags to produce clean text.

Covers decisions from all sections (Secciones 1-5, Sala Plena, etc.)
dating back to the 1990s.

Base URL: http://190.217.24.55:8080/WebRelatoria/ce/index.xhtml
File URL: http://190.217.24.55:8080/WebRelatoria/FileReferenceServlet?corp=ce&ext=html&file={ID}

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import hashlib
import json
import logging
import re
import sys
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

SOURCE_ID = "CO/ConsejoDeEstado"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

BASE_URL = "http://190.217.24.55:8080/WebRelatoria/ce/index.xhtml"
FILE_URL = "http://190.217.24.55:8080/WebRelatoria/FileReferenceServlet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.ConsejoDeEstado")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
}

SEARCH_YEARS = list(range(2021, 1994, -1))  # 2021 down to 1995

# Decision types available in the search form
DECISION_TYPES = ["SENTENCIA", "AUTO", "CONCEPTO"]


def strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    text = re.sub(r'<(br|BR)\s*/?>', '\n', html_text)
    text = re.sub(r'</(p|P|div|DIV|tr|TR|li|LI|h[1-6]|H[1-6])>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def get_session() -> requests.Session:
    """Create a session with the initial page load to get cookies + ViewState."""
    sess = requests.Session()
    sess.headers.update(HEADERS)
    resp = sess.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    vs = extract_viewstate(resp.text)
    sess._viewstate = vs
    return sess


def extract_viewstate(html: str) -> str:
    """Extract javax.faces.ViewState from HTML."""
    m = re.search(r'javax\.faces\.ViewState["\s]+value="([^"]+)"', html)
    if not m:
        raise ValueError("Could not extract ViewState from page")
    return m.group(1)


def search_by_year(sess: requests.Session, year: int,
                   decision_type: str = "SENTENCIA") -> list[dict]:
    """
    Search the legacy WebRelatoria for decisions of a given type and year.
    Returns list of dicts with keys: doc_id, nr, radicado, tipo, section, date_str, snippet.
    """
    date_from = f"1/01/{year}"
    date_to = f"31/12/{year}"

    form_data = {
        "searchForm": "searchForm",
        "searchForm:j_idt63": decision_type,
        "searchForm:temaInput": "",
        "searchForm:j_idt74_input": "",  # section = TODAS
        "searchForm:j_idt98": "",  # radicado
        "searchForm:j_idt100": "",  # ponente
        "searchForm:fechaIniCal_input": date_from,
        "searchForm:fechaFinCal_input": date_to,
        "searchForm:j_idt109": "",  # demandante
        "searchForm:j_idt110": "",  # demandado
        "searchForm:j_idt61": "searchForm:j_idt61",
        "javax.faces.ViewState": sess._viewstate,
    }

    resp = sess.post(BASE_URL, data=form_data, timeout=60)
    resp.raise_for_status()

    # Update ViewState for next request
    try:
        sess._viewstate = extract_viewstate(resp.text)
    except ValueError:
        pass

    return parse_search_results(resp.text)


def parse_search_results(html: str) -> list[dict]:
    """Parse search result HTML to extract document metadata from data table rows."""
    results = []
    # Each result row: <tr data-ri="N" data-rk="DOC_ID" ...>
    row_pattern = re.compile(
        r'<tr[^>]*data-rk="(\d+)"[^>]*>(.*?)</tr>', re.DOTALL
    )
    for m in row_pattern.finditer(html):
        doc_id = m.group(1)
        row_html = m.group(2)

        # Extract metadata from the row content
        info = {"doc_id": doc_id}

        # NR (same as doc_id usually)
        nr_m = re.search(r'<b>NR:\s*</b>.*?(\d+)', row_html)
        if nr_m:
            info["nr"] = nr_m.group(1)

        # Radicado (case number) - appears right after NR line
        lines = [strip_html(l).strip() for l in row_html.split('<br>')]
        lines = [l for l in lines if l]
        if len(lines) >= 3:
            info["radicado"] = lines[2] if len(lines) > 2 else ""

        # Decision type
        for dt in DECISION_TYPES + ["SENTENCIA DE UNIFICACION", "EXTENSION JURISPRUDENCIAL"]:
            if dt in row_html:
                info["tipo"] = dt
                break

        # Date - look for DD/MM/YYYY pattern
        date_m = re.search(r'(\d{2}/\d{2}/\d{4})', row_html)
        if date_m:
            info["date_str"] = date_m.group(1)

        # Section
        sec_m = re.search(r'SECCI[OÓ]N\s+(PRIMERA|SEGUNDA|TERCERA|CUARTA|QUINTA)', row_html)
        if sec_m:
            info["section"] = f"SECCION {sec_m.group(1)}"
        elif "SALA PLENA" in row_html:
            info["section"] = "SALA PLENA"
        elif "SALA DE CONSULTA" in row_html:
            info["section"] = "SALA DE CONSULTA Y SERVICIO CIVIL"

        # Ponente (reporting judge)
        pon_m = re.search(r'<b>PONENTE:\s*</b>.*?</font>\s*<font[^>]*>([^<]+)', row_html)
        if pon_m:
            info["ponente"] = pon_m.group(1).strip()

        results.append(info)

    return results


def fetch_document_text(sess: requests.Session, doc_id: str) -> Optional[str]:
    """Fetch full text of a decision via FileReferenceServlet."""
    url = f"{FILE_URL}?corp=ce&ext=html&file={doc_id}"
    try:
        resp = sess.get(url, timeout=30)
        if resp.status_code != 200 or len(resp.content) < 100:
            return None
        text = strip_html(resp.text)
        if len(text) < 50:
            return None
        return text
    except Exception as e:
        logger.warning(f"Failed to fetch doc {doc_id}: {e}")
        return None


def normalize(meta: dict, text: str) -> dict:
    """Normalize a raw document into the standard schema."""
    doc_id = meta.get("doc_id", "")
    radicado = meta.get("radicado", "")

    # Parse date
    date_iso = None
    date_str = meta.get("date_str", "")
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    title_parts = ["Consejo de Estado"]
    if meta.get("tipo"):
        title_parts.append(meta["tipo"])
    if radicado:
        title_parts.append(radicado)
    if date_iso:
        title_parts.append(date_iso)
    title = " - ".join(title_parts)

    return {
        "_id": f"CO_CE_{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"{FILE_URL}?corp=ce&ext=html&file={doc_id}",
        "radicado": radicado,
        "decision_type": meta.get("tipo", ""),
        "section": meta.get("section", ""),
        "ponente": meta.get("ponente", ""),
    }


def fetch_all(max_records: Optional[int] = None) -> Generator[dict, None, None]:
    """Yield all normalized decisions, searching by year and type."""
    sess = get_session()
    count = 0

    for year in SEARCH_YEARS:
        for dtype in DECISION_TYPES:
            if max_records and count >= max_records:
                return

            logger.info(f"Searching {dtype} for year {year}...")
            time.sleep(1.5)

            try:
                results = search_by_year(sess, year, dtype)
            except Exception as e:
                logger.error(f"Search failed for {year}/{dtype}: {e}")
                # Re-establish session
                try:
                    sess = get_session()
                except Exception:
                    pass
                continue

            logger.info(f"  Found {len(results)} results for {year}/{dtype}")

            for meta in results:
                if max_records and count >= max_records:
                    return

                doc_id = meta["doc_id"]
                time.sleep(1.5)

                text = fetch_document_text(sess, doc_id)
                if not text:
                    logger.warning(f"  No text for doc {doc_id}, skipping")
                    continue

                record = normalize(meta, text)
                count += 1
                logger.info(f"  [{count}] {record['title'][:80]}... ({len(text)} chars)")
                yield record


def bootstrap_sample(sample_count: int = 15) -> bool:
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old samples
    for f in SAMPLE_DIR.glob("*.json"):
        f.unlink()

    records = []
    for record in fetch_all(max_records=sample_count + 5):
        if len(record.get("text", "")) >= 100:
            records.append(record)
            fname = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {fname.name} ({len(record['text'])} chars)")

        if len(records) >= sample_count:
            break

    logger.info(f"Sample complete: {len(records)} records saved")
    return len(records) >= 10


def main():
    parser = argparse.ArgumentParser(
        description="CO/ConsejoDeEstado data fetcher"
    )
    parser.add_argument(
        "command", choices=["bootstrap", "fetch", "updates", "test-api"]
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    if args.command == "test-api":
        logger.info("Testing API connectivity...")
        sess = get_session()
        logger.info(f"Session established, ViewState: {sess._viewstate[:30]}...")
        results = search_by_year(sess, 2020, "SENTENCIA")
        logger.info(f"Search returned {len(results)} results")
        if results:
            doc_id = results[0]["doc_id"]
            logger.info(f"Fetching doc {doc_id}...")
            text = fetch_document_text(sess, doc_id)
            if text:
                logger.info(f"Got {len(text)} chars of text")
                logger.info(f"First 300 chars: {text[:300]}")
            else:
                logger.error("No text returned")
        return

    if args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        since = args.since or "2024-01-01"
        logger.info(f"Fetching updates since {since}")
        for record in fetch_all():
            if record.get("date") and record["date"] >= since:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
