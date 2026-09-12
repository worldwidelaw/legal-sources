#!/usr/bin/env python3
"""
SV/BCR-Regulations -- Banco Central de Reserva de El Salvador, financial-system regulations

Fetches the technical regulatory framework (marco normativo) of El Salvador's
financial system, issued by the Comité de Normas del Banco Central de Reserva
(BCR). This covers normas técnicas (prudential, accounting, general), reglamentos,
instructivos, lineamientos and resoluciones that govern banks, insurers, the
pension system, the securities market and other supervised entities.

Data source: https://www.bcr.gob.sv/regulaciones/
License: Open Government Data (official regulations of a public authority)

Strategy:
  - The regulations portal renders each norm's detail via a POST to
    `normativa.php` with `norma=<numeric id>`. The detail HTML gives the
    Referencia (e.g. NCF-13), title (Normativa), Objeto, approval date,
    effective date and approving body.
  - Each norm's full text PDF lives at a predictable URL:
        /regulaciones/upload/<Referencia>.pdf
  - We enumerate the numeric ids (the backing DB tops out around ~1000),
    keep the ones that resolve to a real Referencia, download the linked PDF
    and extract its text. Scanned/empty PDFs (no text layer) are skipped.
  - This yields the BCR's published normas técnicas with full text, not just
    titles/metadata.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch a sample for validation
  python bootstrap.py bootstrap            # Full bootstrap (all norm ids)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import html as html_lib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "SV/BCR-Regulations"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SV.BCR-Regulations")

BASE_URL = "https://www.bcr.gob.sv/regulaciones"
DETAIL_URL = f"{BASE_URL}/normativa.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

RATE_LIMIT = 1.0            # seconds between PDF downloads
DETAIL_DELAY = 0.3          # seconds between (lightweight) detail lookups
MIN_TEXT_CHARS = 500        # below this we treat the PDF as scanned / empty
MAX_NORMA_ID = 1100         # backing DB tops out ~1000; scan with headroom
SAMPLE_TARGET = 14          # text-bearing records to collect in sample mode

# Referencia (norm code) -> PDF filename. The portal stores most norms as
# upload/<ref>.pdf. A few are uploaded with the ref kept verbatim, so we also
# try a couple of mild variants before giving up.
def _pdf_candidates(ref: str) -> list[str]:
    ref = ref.strip()
    variants = [ref, ref.replace(" ", "_"), ref.replace(" ", "")]
    seen, out = set(), []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(f"{BASE_URL}/upload/{v}.pdf")
    return out


def _clean(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", html_lib.unescape(s)).strip()


def _norm_date(raw: str) -> Optional[str]:
    """BCR dates are DD/MM/YYYY. Return ISO 8601 (YYYY-MM-DD) or None."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw or "")
    if not m:
        return None
    d, mo, y = m.groups()
    try:
        return datetime(int(y), int(mo), int(d)).date().isoformat()
    except ValueError:
        return None


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _parse_detail(norma_id: int, html: str) -> Optional[dict]:
    """Parse a normativa.php detail response. Returns None if the id is empty."""
    ref_m = re.search(r"<strong>\s*Referencia:\s*</strong>\s*([^<]+)", html)
    if not ref_m:
        return None
    ref = _clean(ref_m.group(1))
    if not ref:
        return None

    def field(label: str) -> str:
        m = re.search(
            r'class="negrita">\s*' + label + r'\s*</span>\s*([^<]+)', html
        )
        return _clean(m.group(1)) if m else ""

    title = field("Normativa:")
    objeto = field("Objeto:")
    aprobacion = field(r"Aprobaci&oacute;n:") or field("Aprobación:")
    vigencia = field("Entrada en vigencia:")
    aprobado = field("Aprobado por:")

    return {
        "norma_id": norma_id,
        "ref": ref,
        "title": title,
        "objeto": objeto,
        "aprobacion": aprobacion,
        "vigencia": vigencia,
        "aprobado_por": aprobado,
    }


def _fetch_detail(session: requests.Session, norma_id: int) -> Optional[dict]:
    try:
        resp = session.post(DETAIL_URL, data={"norma": str(norma_id)}, timeout=(15, 45))
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Detail fetch failed for id %s: %s", norma_id, e)
        return None
    return _parse_detail(norma_id, resp.text)


def _download_pdf(session: requests.Session, ref: str) -> Optional[bytes]:
    """Try the candidate PDF URLs for a norm. Return PDF bytes or None."""
    for url in _pdf_candidates(ref):
        try:
            resp = session.get(url, timeout=(15, 90))
        except Exception as e:
            logger.warning("PDF fetch error %s: %s", url, e)
            continue
        if resp.status_code != 200:
            continue
        data = resp.content
        if data[:5].startswith(b"%PDF"):
            return data
    return None


def normalize(meta: dict, text: str) -> dict:
    """Transform raw metadata + extracted text into the standard schema."""
    ref = meta["ref"]
    title = meta.get("title") or ref
    full_title = f"{ref} — {title}" if title and title != ref else ref

    date = _norm_date(meta.get("vigencia", "")) or _norm_date(meta.get("aprobacion", ""))
    pdf_url = _pdf_candidates(ref)[0]

    # Refs like "NCF-C-012026" are consultation drafts (en consulta); the rest
    # are enacted norms (vigente).
    is_consulta = bool(re.search(r"-C-\d", ref))
    regulatory_status = "consulta" if is_consulta else "vigente"

    return {
        "_id": f"BCR-{re.sub(r'[^A-Za-z0-9]+', '-', ref).strip('-')}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": text,
        "date": date,
        "url": pdf_url,
        "reference": ref,
        "objeto": meta.get("objeto") or None,
        "approval_date": _norm_date(meta.get("aprobacion", "")),
        "effective_date": _norm_date(meta.get("vigencia", "")),
        "approved_by": meta.get("aprobado_por") or None,
        "regulatory_status": regulatory_status,
        "jurisdiction": "SV",
        "language": "es",
        "publisher": "Banco Central de Reserva de El Salvador (Comité de Normas)",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    session = _session()
    yielded = 0
    seen_refs: set[str] = set()

    # Recent norms have the highest ids; scan downward so a bounded sample
    # captures the current framework first.
    ids = range(MAX_NORMA_ID, 0, -1)
    for norma_id in ids:
        meta = _fetch_detail(session, norma_id)
        time.sleep(DETAIL_DELAY)
        if not meta:
            continue
        ref = meta["ref"]
        if ref in seen_refs:
            continue
        seen_refs.add(ref)

        pdf_bytes = _download_pdf(session, ref)
        time.sleep(RATE_LIMIT)
        if pdf_bytes is None:
            logger.info("Skip %s (id %s) — no downloadable PDF", ref, norma_id)
            continue

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=normalize(meta, "")["_id"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            logger.info("Skip %s (id %s) — no extractable text (scanned)", ref, norma_id)
            continue

        yielded += 1
        logger.info("[%d] %s — %s (%d chars)", yielded, ref, meta.get("title", "")[:55], len(text))
        yield normalize(meta, text.strip())

        if sample and yielded >= SAMPLE_TARGET:
            break

    logger.info("Total records yielded: %d", yielded)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Yield norms whose effective/approval date is on/after `since` (YYYY-MM-DD)."""
    for record in fetch_all(sample=False):
        d = record.get("date")
        if d and since and d < since:
            continue
        yield record


def test_api() -> bool:
    """Quick connectivity test: resolve a known norm and extract its PDF."""
    session = _session()
    for norma_id in (989, 1000, 990, 950, 900):
        meta = _fetch_detail(session, norma_id)
        time.sleep(DETAIL_DELAY)
        if not meta:
            continue
        logger.info("Resolved id %s -> %s (%s)", norma_id, meta["ref"], meta.get("title", "")[:50])
        pdf = _download_pdf(session, meta["ref"])
        if not pdf:
            continue
        text = extract_pdf_markdown(
            source=SOURCE_ID, source_id="test", pdf_bytes=pdf, table="doctrine"
        )
        if text and len(text.strip()) >= MIN_TEXT_CHARS:
            logger.info("Extracted %d chars from %s", len(text), meta["ref"])
            logger.info("Preview: %s", text.strip()[:200].replace("\n", " "))
            logger.info("API test passed!")
            return True
    logger.error("Could not resolve and extract any norm")
    return False


def bootstrap(sample: bool = False) -> int:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = SAMPLE_DIR if sample else (SOURCE_DIR / "data")
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        fname = f"{record['_id'].replace('/', '_')}.json"
        with open(output_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info("Saved: %s — %s", record["_id"], record["title"][:70])

    logger.info("Bootstrap complete: %d records saved to %s", count, output_dir)
    return count


def main():
    parser = argparse.ArgumentParser(description="SV/BCR-Regulations data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        sys.exit(0 if test_api() else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
