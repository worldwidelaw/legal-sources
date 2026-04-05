#!/usr/bin/env python3
"""
AR/CNACAF -- Cámara Nacional de Apelaciones en lo Contencioso Administrativo Federal

Fetches Argentine Federal Administrative Appeals Court decisions from SAIJ.

Data source: https://www.saij.gob.ar
License: Open public access (government data)

Strategy:
  - SAIJ search API with tribunal filter for CNACAF
  - Full judgments (fallo) have PDF attachments — download and extract text
  - Inline text used when available (sumarios)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # API connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
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

SOURCE_ID = "AR/CNACAF"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.CNACAF")

BASE_URL = "https://www.saij.gob.ar"
SEARCH_URL = f"{BASE_URL}/busqueda"
DOCUMENT_URL = f"{BASE_URL}/view-document"
DOWNLOAD_URL = f"{BASE_URL}/descarga-archivo"

TRIBUNAL_NAME = "CAMARA NAC. APELAC. EN LO CONTENCIOSO ADMINISTRATIVO FEDERAL"

CNACAF_FACETS = (
    "Total|Fecha[20,1]|Estado de Vigencia[5,1]|Tema[5,1]|Organismo[5,1]"
    "|Autor[5,1]|Jurisdicción|Tribunal[5,1]|Publicación[5,1]"
    "|Colección temática[5,1]|Tipo de Documento/Jurisprudencia"
    f"|Tribunal/{TRIBUNAL_NAME}"
)

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/json",
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\[\[/?p\]\]', '', text)
    text = re.sub(r'\[\[r uuid:[^\]]+\]\]', '', text)
    text = re.sub(r'\[\[/r uuid:[^\]]+\]\]', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_document_abstract(abstract_json: str) -> Optional[dict]:
    try:
        data = json.loads(abstract_json)
        doc = data.get("document", {})
        return {
            "metadata": doc.get("metadata", {}),
            "content": doc.get("content", {})
        }
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to parse document abstract: {e}")
        return None


def search_documents(offset: int = 0, page_size: int = 20, timeout: int = 60) -> dict:
    params = {
        "o": offset,
        "p": page_size,
        "f": CNACAF_FACETS,
        "s": "fecha-rango|DESC",
        "v": "colapsada",
    }
    response = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_document_detail(uuid: str, timeout: int = 60) -> Optional[dict]:
    try:
        response = requests.get(
            DOCUMENT_URL, params={"guid": uuid}, headers=HEADERS, timeout=timeout
        )
        response.raise_for_status()
        data = response.json()
        if "data" in data and isinstance(data["data"], str):
            return json.loads(data["data"])
        return data
    except (requests.RequestException, json.JSONDecodeError) as e:
        logger.warning(f"Failed to fetch document {uuid}: {e}")
        return None


def extract_pdf_text(pdf_uuid: str, pdf_name: str, timeout: int = 60) -> str:
    """Download a PDF from SAIJ and extract its text content."""
    if not pdfplumber:
        return ""
    try:
        r = requests.get(
            DOWNLOAD_URL,
            params={"guid": pdf_uuid, "name": pdf_name},
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=timeout,
        )
        r.raise_for_status()
        if len(r.content) < 100:
            return ""
        pdf = pdfplumber.open(io.BytesIO(r.content))
        pages = []
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                pages.append(t)
        pdf.close()
        return "\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {pdf_name}: {e}")
        return ""


def normalize(raw: dict) -> dict:
    metadata = raw.get("metadata", {})
    content = raw.get("content", {})

    uuid = metadata.get("uuid", "")
    doc_type = metadata.get("document-content-type", "unknown")
    friendly_url = metadata.get("friendly-url", {})

    subdomain = friendly_url.get("subdomain", "documento")
    description = friendly_url.get("description", uuid)
    url = f"{BASE_URL}/{subdomain}/{description}"

    # Extract text: prefer inline, then PDF extraction
    text = ""
    if "texto" in content and content["texto"]:
        text = clean_text(content["texto"])
    elif "texto-doc" in content:
        texto_doc = content["texto-doc"]
        if isinstance(texto_doc, dict) and texto_doc.get("uuid") and texto_doc.get("file-name"):
            text = extract_pdf_text(texto_doc["uuid"], texto_doc["file-name"])

    # Title
    title = content.get("titulo", "")
    if not title:
        if content.get("actor") and content.get("demandado"):
            title = f"{content['actor']} c/ {content['demandado']}"
        elif content.get("caratula"):
            title = content["caratula"]
        elif content.get("actor"):
            title = content["actor"]
        elif content.get("sobre"):
            title = content["sobre"]

    # Date
    date = content.get("fecha")
    if date:
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            date = None

    # Jurisdiction
    jurisdiction = content.get("jurisdiccion", {})
    if isinstance(jurisdiction, dict):
        jurisdiction_str = jurisdiction.get("descripcion", "")
    else:
        jurisdiction_str = str(jurisdiction)

    # Descriptors
    descriptors = content.get("descriptores", {})
    descriptors_text = ""
    if isinstance(descriptors, dict):
        descriptor_list = descriptors.get("descriptor", [])
        if isinstance(descriptor_list, list):
            descriptors_text = ", ".join(
                d.get("elegido", {}).get("termino", "")
                for d in descriptor_list
                if isinstance(d, dict)
            )

    return {
        "_id": uuid,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "uuid": uuid,
        "doc_type": doc_type,
        "title": title or f"Case {uuid[:20]}",
        "text": text,
        "date": date,
        "url": url,
        "tribunal": content.get("tribunal", ""),
        "jurisdiction": jurisdiction_str,
        "materia": content.get("materia", ""),
        "actor": content.get("actor", ""),
        "demandado": content.get("demandado", ""),
        "sobre": content.get("sobre", ""),
        "magistrados": content.get("magistrados", ""),
        "descriptores": descriptors_text,
        "sumario": clean_text(content.get("sumario", "")),
        "id_infojus": content.get("id-infojus", ""),
    }


def fetch_sample(count: int = 15) -> list:
    records = []
    logger.info(f"Fetching {count} sample CNACAF records from SAIJ...")

    try:
        result = search_documents(offset=0, page_size=min(count * 2, 50))
        total = result.get("searchResults", {}).get("totalSearchResults", 0)
        logger.info(f"Total CNACAF records available: {total:,}")

        doc_list = result.get("searchResults", {}).get("documentResultList", [])

        for doc in doc_list:
            abstract_str = doc.get("documentAbstract", "{}")
            parsed = parse_document_abstract(abstract_str)
            if not parsed:
                continue

            uuid = parsed.get("metadata", {}).get("uuid")
            if uuid:
                time.sleep(0.5)
                detail = get_document_detail(uuid)
                if detail:
                    doc_data = detail.get("document", {})
                    parsed = {
                        "metadata": doc_data.get("metadata", {}),
                        "content": doc_data.get("content", {})
                    }

            normalized = normalize(parsed)
            if normalized.get("text") and len(normalized["text"]) > 50:
                records.append(normalized)
                logger.info(
                    f"  [{len(records)}/{count}] {normalized['title'][:60]}... "
                    f"({len(normalized['text']):,} chars)"
                )

            if len(records) >= count:
                break

            time.sleep(1)

    except Exception as e:
        logger.error(f"Error fetching samples: {e}")

    return records


def fetch_all(max_records: int = None, output_file: str = None) -> Generator[dict, None, None]:
    result = search_documents(offset=0, page_size=1)
    total = result.get("searchResults", {}).get("totalSearchResults", 0)
    logger.info(f"Total CNACAF records: {total:,}")

    if max_records:
        total = min(total, max_records)

    page_size = 50
    fetched = 0
    errors = 0

    output_fh = None
    if output_file:
        output_fh = open(output_file, "w", encoding="utf-8")

    try:
        for offset in range(0, total, page_size):
            try:
                result = search_documents(offset=offset, page_size=page_size)
                doc_list = result.get("searchResults", {}).get("documentResultList", [])

                if not doc_list:
                    break

                for doc in doc_list:
                    try:
                        abstract_str = doc.get("documentAbstract", "{}")
                        parsed = parse_document_abstract(abstract_str)
                        if not parsed:
                            errors += 1
                            continue

                        uuid = parsed.get("metadata", {}).get("uuid")
                        if uuid:
                            time.sleep(0.5)
                            detail = get_document_detail(uuid)
                            if detail:
                                doc_data = detail.get("document", {})
                                parsed = {
                                    "metadata": doc_data.get("metadata", parsed.get("metadata", {})),
                                    "content": doc_data.get("content", parsed.get("content", {}))
                                }

                        normalized = normalize(parsed)
                        if not normalized.get("text") or len(normalized["text"]) < 50:
                            errors += 1
                            continue

                        fetched += 1
                        if output_fh:
                            output_fh.write(json.dumps(normalized, ensure_ascii=False) + "\n")
                        yield normalized

                        if fetched % 100 == 0:
                            logger.info(f"Progress: {fetched:,}/{total:,}")

                        if max_records and fetched >= max_records:
                            break
                    except Exception as e:
                        logger.warning(f"Error processing document: {e}")
                        errors += 1

                if max_records and fetched >= max_records:
                    break
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error at offset {offset}: {e}")
                time.sleep(5)
    finally:
        if output_fh:
            output_fh.close()

    logger.info(f"Fetch complete: {fetched:,} records, {errors:,} errors")


def test_api():
    logger.info("Testing SAIJ API with CNACAF tribunal filter...")
    try:
        result = search_documents(offset=0, page_size=2)
        total = result.get("searchResults", {}).get("totalSearchResults", 0)
        logger.info(f"Search API OK - Total CNACAF results: {total:,}")

        docs = result.get("searchResults", {}).get("documentResultList", [])
        if docs:
            abstract = parse_document_abstract(docs[0].get("documentAbstract", "{}"))
            if abstract:
                uuid = abstract.get("metadata", {}).get("uuid")
                if uuid:
                    detail = get_document_detail(uuid)
                    if detail:
                        logger.info(f"Document API OK - Fetched {uuid[:30]}...")
                        return True
        return True
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def bootstrap_sample():
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        filename = f"sample_{i:02d}_{record['uuid'][:20]}.json"
        with open(SAMPLE_DIR / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")
    logger.info(f"Validation:")
    logger.info(f"  Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  Avg text length: {avg_text:.0f} chars")
    logger.info(f"  Min text length: {min(text_lengths) if text_lengths else 0} chars")
    logger.info(f"  Max text length: {max(text_lengths) if text_lengths else 0} chars")

    return len(records) >= 10 and avg_text > 100


def bootstrap_full(output_dir: Path = None):
    if output_dir is None:
        output_dir = SOURCE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "records.jsonl"

    logger.info(f"Starting full bootstrap to {output_file}")
    count = sum(1 for _ in fetch_all(output_file=str(output_file)))
    logger.info(f"Bootstrap complete: {count:,} records")
    return count > 0


def main():
    parser = argparse.ArgumentParser(description="AR/CNACAF Case Law Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap on VPS")
    args = parser.parse_args()

    if args.command == "test-api":
        sys.exit(0 if test_api() else 1)
    elif args.command == "bootstrap":
        if args.sample:
            sys.exit(0 if bootstrap_sample() else 1)
        elif args.full:
            sys.exit(0 if bootstrap_full() else 1)
        else:
            sys.exit(0 if bootstrap_sample() else 1)


if __name__ == "__main__":
    main()
