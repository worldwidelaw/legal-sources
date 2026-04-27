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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "BR/CamaraDeputados"
API_BASE = "https://dadosabertos.camara.leg.br/api/v2"

# Proposition types to fetch (major legislative types)
SIGLA_TIPOS = ["PL", "PLP", "PEC", "MPV", "PDC", "PDL"]

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "application/json",
}

SAMPLE_DIR = Path(__file__).parent / "sample"


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
    """Download and extract text from the inteiro teor PDF."""
    if not pdf_url:
        return ""
    try:
        resp = session.get(pdf_url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        pdf_bytes = resp.content
        if len(pdf_bytes) < 100:
            print(f"    PDF too small ({len(pdf_bytes)} bytes), skipping")
            return ""
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""
        return text
    except requests.RequestException as e:
        print(f"    Error fetching PDF: {e}")
        return ""


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

    # Fallback: use ementa + metadata if no PDF text
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

    doc_id = f"BR_CD_{sigla}_{numero}_{ano}"

    return {
        "_id": doc_id,
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
    }


def fetch_all(max_records: int = None, sigla_tipos: list = None,
              data_inicio: str = None) -> Generator[dict, None, None]:
    """Fetch all proposições from the API."""
    session = requests.Session()
    tipos = sigla_tipos or SIGLA_TIPOS
    count = 0
    errors = 0

    for sigla in tipos:
        print(f"\n--- Fetching {sigla} proposições ---")
        pagina = 1

        while True:
            if max_records and count >= max_records:
                return

            data = None
            for attempt in range(3):
                try:
                    data = fetch_proposicoes_page(session, sigla, pagina=pagina,
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

            for item in items:
                if max_records and count >= max_records:
                    return

                prop_id = item.get("id")
                print(f"  [{count+1}] {item.get('siglaTipo', '')} {item.get('numero', '')}/{item.get('ano', '')} (id={prop_id})")

                time.sleep(1.0)
                detail = fetch_proposicao_detail(session, prop_id)
                if not detail:
                    errors += 1
                    continue

                url_teor = detail.get("urlInteiroTeor", "")
                pdf_text = ""
                if url_teor:
                    time.sleep(1.0)
                    pdf_text = fetch_pdf_text(url_teor, session, f"BR_CD_{prop_id}")
                    if pdf_text:
                        print(f"    PDF: {len(pdf_text):,} chars")
                    else:
                        print(f"    No PDF text extracted")

                record = normalize(detail, pdf_text)
                if record["text"] and len(record["text"]) >= 50:
                    yield record
                    count += 1
                else:
                    print(f"    Skipped (insufficient text)")
                    errors += 1

            pagina += 1
            time.sleep(0.5)

    print(f"\nTotal records: {count}, errors: {errors}")


def fetch_updates(since: datetime, **kwargs) -> Generator[dict, None, None]:
    """Fetch proposições updated since the given date."""
    data_inicio = since.strftime("%Y-%m-%d")
    yield from fetch_all(data_inicio=data_inicio, **kwargs)


def bootstrap_sample(sample_count: int = 15) -> bool:
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    errors = 0

    for record in fetch_all(max_records=sample_count + 5, sigla_tipos=["PL", "PEC", "MPV"]):
        if len(records) >= sample_count:
            break

        if record["text"] and len(record["text"]) >= 100:
            records.append(record)
            filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            print(f"    Saved: {len(record['text']):,} chars")
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

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
