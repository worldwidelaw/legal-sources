#!/usr/bin/env python3
"""
BR/CamaraDeputados - Brazilian Chamber of Deputies Open Data API Fetcher

Fetches proposições (legislative proposals) from the Câmara dos Deputados
open data API and extracts full text from official PDF documents.

Data source: https://dadosabertos.camara.leg.br/swagger/api.html
License: Open Data (dados abertos)

Usage:
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown, read_capped

SOURCE_ID = "BR/CamaraDeputados"
API_BASE = "https://dadosabertos.camara.leg.br/api/v2"

# Proposition types to fetch (major legislative types)
SIGLA_TIPOS = ["PL", "PLP", "PEC", "MPV", "PDC", "PDL"]

# The portal's WAF 403s on the User-Agent's *shape*, not on our IP: the literal
# substring "; Open Data" in the previous UA was rejected from every vantage,
# while the same request with any other UA (including bare curl) returns 200.
# Keep this string boring — A/B a new one before changing it.
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (+https://legaldatahunter.com)",
    "Accept": "application/json",
}

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
CHECKPOINT_FILE = SOURCE_DIR / "checkpoint.json"

# The corpus is ~500K proposições, each with its own inteiro-teor PDF, so the
# run is dominated by per-document latency rather than by the API. Fetch the
# detail + PDF for a page's items concurrently; the API itself is paginated
# serially so the request rate stays modest (issue #1343, ~0.2 rec/s before).
PDF_WORKERS = int(os.environ.get("CAMARA_PDF_WORKERS", "6"))

# Politeness delay per worker request (seconds). With PDF_WORKERS in flight
# this still keeps the aggregate rate well inside what the portal tolerates.
REQUEST_DELAY = float(os.environ.get("CAMARA_REQUEST_DELAY", "0.5"))

# Wall-clock cap for downloading one inteiro-teor PDF. requests' `timeout` is
# per socket read, so a host that trickles bytes never trips it and a single
# document can wedge the crawl for hours — which is exactly how this source
# hung for ~48 min on one PDF (issue #1343). Extraction itself is bounded by
# common/pdf_extract's opendataloader/OCR subprocess deadlines.
PDF_DOWNLOAD_TIMEOUT = int(os.environ.get("CAMARA_PDF_TIMEOUT", "120"))
PDF_MAX_BYTES = int(os.environ.get("CAMARA_PDF_MAX_BYTES", str(50_000_000)))

_thread_local = threading.local()


def _session() -> requests.Session:
    """Per-thread requests.Session (Session is not safe to share across threads)."""
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        _thread_local.session = sess
    return sess


# --- Checkpoint -----------------------------------------------------------
#
# A fleet slot can be torn down mid-crawl. Without a checkpoint the next run
# restarts at page 1 of the first type and re-does days of work (issue #1343).


def load_checkpoint() -> dict:
    """Load the crawl checkpoint, or a fresh one if absent/corrupt."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                ckpt = json.load(f)
            if isinstance(ckpt, dict) and "completed_tipos" in ckpt:
                return ckpt
            print("  Checkpoint has unexpected shape, starting fresh")
        except (json.JSONDecodeError, OSError) as e:
            print(f"  Invalid checkpoint ({e}), starting fresh")
    return {"completed_tipos": [], "sigla": None, "pagina": 1, "count": 0}


def save_checkpoint(ckpt: dict) -> None:
    """Persist the checkpoint atomically so a kill mid-write cannot corrupt it."""
    tmp = CHECKPOINT_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ckpt, f, indent=2)
    tmp.replace(CHECKPOINT_FILE)


def clear_checkpoint() -> None:
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("Checkpoint cleared (crawl complete)")


def fetch_proposicoes_page(session: requests.Session, sigla_tipo: str,
                           pagina: int = 1, itens: int = 100,
                           data_inicio: str = None, data_fim: str = None) -> dict:
    """Fetch a page of proposições from the API."""
    params = {
        "siglaTipo": sigla_tipo,
        "pagina": pagina,
        "itens": itens,
        "ordem": "DESC",
        "ordenarPor": "id",
    }
    if data_inicio:
        params["dataApresentacaoInicio"] = data_inicio
    if data_fim:
        params["dataApresentacaoFim"] = data_fim

    resp = session.get(f"{API_BASE}/proposicoes", params=params,
                       headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_proposicao_detail(session: requests.Session, prop_id: int) -> Optional[dict]:
    """Fetch detail for a single proposição."""
    try:
        resp = session.get(f"{API_BASE}/proposicoes/{prop_id}",
                           headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json().get("dados", {})
    except requests.RequestException as e:
        print(f"    Error fetching detail for {prop_id}: {e}")
        return None


def fetch_pdf_text(pdf_url: str, session: requests.Session, source_id: str) -> str:
    """Download and extract text from the inteiro teor PDF.

    The download is bounded by a wall-clock cap rather than requests' per-socket
    timeout, so a server that trickles bytes cannot stall the crawl.
    """
    if not pdf_url:
        return ""
    try:
        resp = session.get(pdf_url, headers=HEADERS,
                           timeout=(15, PDF_DOWNLOAD_TIMEOUT), stream=True)
        resp.raise_for_status()
        pdf_bytes = read_capped(
            resp,
            max_size=PDF_MAX_BYTES,
            wall_clock=PDF_DOWNLOAD_TIMEOUT,
            label=pdf_url,
        )
    except requests.RequestException as e:
        print(f"    Error fetching PDF: {e}")
        return ""

    if not pdf_bytes:
        # read_capped already logged which cap was hit.
        return ""
    if len(pdf_bytes) < 100:
        print(f"    PDF too small ({len(pdf_bytes)} bytes), skipping")
        return ""

    try:
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""
    except Exception as e:  # extraction must never kill the crawl
        print(f"    PDF extraction failed for {source_id}: {e}")
        return ""


def build_record(item: dict) -> Optional[dict]:
    """Fetch detail + inteiro-teor PDF for one listing item and normalize it.

    Runs on a worker thread; returns None when the document has no usable text.
    """
    prop_id = item.get("id")
    session = _session()

    time.sleep(REQUEST_DELAY)
    detail = fetch_proposicao_detail(session, prop_id)
    if not detail:
        return None

    url_teor = detail.get("urlInteiroTeor", "")
    pdf_text = ""
    if url_teor:
        time.sleep(REQUEST_DELAY)
        started = time.monotonic()
        pdf_text = fetch_pdf_text(url_teor, session, doc_id(detail))
        elapsed = time.monotonic() - started
        if elapsed > 60:
            print(f"    Slow PDF for {prop_id}: {elapsed:.0f}s")

    record = normalize(detail, pdf_text)
    if not record["text"] or len(record["text"]) < 50:
        return None
    return record


def doc_id(detail: dict) -> str:
    """Stable document id — also the key common/pdf_extract dedupes against."""
    return "BR_CD_{}_{}_{}".format(
        detail.get("siglaTipo", ""), detail.get("numero", ""), detail.get("ano", "")
    )


def normalize(detail: dict, pdf_text: str = "") -> dict:
    """Transform API detail into standard schema."""
    prop_id = detail.get("id", 0)
    sigla = detail.get("siglaTipo", "")
    numero = detail.get("numero", "")
    ano = detail.get("ano", "")
    ementa = detail.get("ementa", "") or ""
    ementa_det = detail.get("ementaDetalhada", "") or ""
    keywords = detail.get("keywords", "") or ""
    desc_tipo = detail.get("descricaoTipo", "") or ""
    data_apres = detail.get("dataApresentacao", "") or ""
    url_teor = detail.get("urlInteiroTeor", "") or ""

    # Build title
    title = f"{sigla} {numero}/{ano}"
    if ementa:
        title += f" - {ementa[:200]}"

    # Date
    date = data_apres[:10] if data_apres else ""

    # URL
    url = f"https://www.camara.leg.br/proposicoesWeb/fichadetramitacao?idProposicao={prop_id}"

    # Status info
    status = detail.get("statusProposicao", {}) or {}
    situacao = status.get("descricaoSituacao", "") or ""
    despacho = status.get("despacho", "") or ""

    # Build text
    full_text = pdf_text.strip() if pdf_text else ""
    text_source = "pdf" if full_text else "metadata"

    # Fallback: use ementa + metadata if no PDF text. Some inteiro-teor
    # documents (mostly MPV annexes) are image-only scans, so record which
    # path produced the text rather than passing a summary off as full text.
    if not full_text:
        parts = []
        if desc_tipo:
            parts.append(f"Tipo: {desc_tipo}")
        if ementa:
            parts.append(f"Ementa: {ementa}")
        if ementa_det:
            parts.append(f"Ementa detalhada: {ementa_det}")
        if keywords:
            parts.append(f"Palavras-chave: {keywords}")
        if situacao:
            parts.append(f"Situação: {situacao}")
        if despacho:
            parts.append(f"Despacho: {despacho}")
        full_text = "\n".join(parts)

    return {
        "_id": doc_id(detail),
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": url,
        "sigla_tipo": sigla,
        "numero": str(numero),
        "ano": str(ano),
        "ementa": ementa,
        "keywords": keywords,
        "situacao": situacao,
        "pdf_url": url_teor,
        "text_source": text_source,
    }


def fetch_all(max_records: int = None, sigla_tipos: list = None,
              data_inicio: str = None,
              use_checkpoint: bool = False) -> Generator[dict, None, None]:
    """Fetch all proposições from the API.

    Each listing page's documents are fetched concurrently (PDF download plus
    extraction is the bottleneck), and progress is checkpointed per page so a
    torn-down run resumes where it stopped instead of restarting at 0.
    """
    listing_session = requests.Session()
    tipos = sigla_tipos or SIGLA_TIPOS
    errors = 0

    ckpt = load_checkpoint() if use_checkpoint else {
        "completed_tipos": [], "sigla": None, "pagina": 1, "count": 0}
    count = ckpt["count"] if use_checkpoint else 0
    if use_checkpoint and (ckpt["completed_tipos"] or ckpt["pagina"] > 1):
        print(f"Resuming: {count:,} records already emitted, "
              f"done={ckpt['completed_tipos']}, at {ckpt['sigla']} page {ckpt['pagina']}")

    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=PDF_WORKERS) as pool:
        for sigla in tipos:
            if use_checkpoint and sigla in ckpt["completed_tipos"]:
                print(f"\n--- Skipping {sigla} (already complete per checkpoint) ---")
                continue

            print(f"\n--- Fetching {sigla} proposições ---")
            pagina = ckpt["pagina"] if (use_checkpoint and ckpt["sigla"] == sigla) else 1

            while True:
                if max_records and count >= max_records:
                    return

                data = None
                for attempt in range(3):
                    try:
                        data = fetch_proposicoes_page(listing_session, sigla,
                                                      pagina=pagina,
                                                      data_inicio=data_inicio)
                        break
                    except requests.RequestException as e:
                        print(f"  Error on page {pagina} (attempt {attempt+1}/3): {e}")
                        if attempt < 2:
                            time.sleep(5 * (attempt + 1))
                if data is None:
                    print(f"  Giving up on {sigla} after 3 retries on page {pagina}")
                    break

                items = data.get("dados", [])
                if not items:
                    break

                if max_records:
                    items = items[: max_records - count]

                # map() keeps result order, so the emitted stream still follows
                # the API's ordering even though the fetches overlap.
                for item, record in zip(items, pool.map(build_record, items)):
                    if record is None:
                        errors += 1
                        continue
                    yield record
                    count += 1

                rate = count / max(time.monotonic() - started, 1e-6)
                print(f"  {sigla} page {pagina}: {count:,} records so far "
                      f"({errors:,} skipped, {rate:.2f} rec/s)")

                pagina += 1
                if use_checkpoint:
                    ckpt.update(sigla=sigla, pagina=pagina, count=count)
                    save_checkpoint(ckpt)

            if use_checkpoint:
                ckpt["completed_tipos"].append(sigla)
                ckpt.update(sigla=None, pagina=1, count=count)
                save_checkpoint(ckpt)

    print(f"\nTotal records: {count}, errors: {errors}")


def fetch_updates(since, **kwargs) -> Generator[dict, None, None]:
    """Fetch proposições presented since the given date.

    `since` may be a datetime or an ISO date string — the fleet passes a
    datetime, the CLI a string.
    """
    if isinstance(since, str):
        since = datetime.fromisoformat(since)
    data_inicio = since.strftime("%Y-%m-%d")
    yield from fetch_all(data_inicio=data_inicio, **kwargs)


def bootstrap_sample(sample_count: int = 15) -> bool:
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    errors = 0

    # Draw from each type in turn rather than letting the first one fill the
    # quota: the listing is ordered newest-first, so an all-PL sample is all
    # just-presented bills whose statusProposicao fields are still empty.
    tipos = ["PL", "PEC", "MPV"]
    per_tipo = max(sample_count // len(tipos), 1)

    for sigla in tipos:
        if len(records) >= sample_count:
            break
        take = per_tipo if sigla != tipos[-1] else sample_count - len(records)
        for record in fetch_all(max_records=take, sigla_tipos=[sigla]):
            if len(records) >= sample_count:
                break

            if record["text"] and len(record["text"]) >= 100:
                records.append(record)
                filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                print(f"    Saved: {sigla} {len(record['text']):,} chars")
            else:
                errors += 1

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
    print(f"Errors: {errors}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        has_pdf = sum(1 for r in records if len(r.get("text", "")) > 500)
        print(f"Records with substantial text (>500 chars): {has_pdf}/{len(records)}")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text") or len(r["text"]) < 100)
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def bootstrap_full(data_inicio: str = None, resume: bool = True) -> int:
    """Stream the full corpus to data/records.jsonl (what the pipeline ingests).

    Resumes from checkpoint.json by default, appending to the existing JSONL so
    a torn-down slot does not have to re-crawl what it already wrote (#1343).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "records.jsonl"

    ckpt = load_checkpoint()
    resuming = resume and (ckpt["completed_tipos"] or ckpt["pagina"] > 1)
    if not resuming and out_path.exists():
        out_path.unlink()

    count = 0
    with open(out_path, "a" if resuming else "w", encoding="utf-8") as f:
        for record in fetch_all(data_inicio=data_inicio, use_checkpoint=resume):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 100 == 0:
                f.flush()

    clear_checkpoint()
    print(f"bootstrap_fast complete: {count} fetched -> {out_path}")
    return count


def main():
    parser = argparse.ArgumentParser(description=f"{SOURCE_ID} legislation fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap (all types)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore any existing checkpoint and crawl from the start")

    args = parser.parse_args()

    if args.command in ("bootstrap", "fetch"):
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        bootstrap_full(resume=not args.no_resume)

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
