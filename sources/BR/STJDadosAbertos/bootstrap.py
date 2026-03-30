#!/usr/bin/env python3
"""
BR/STJDadosAbertos -- Brazilian Superior Court of Justice Open Data

Fetches STJ case law from the CKAN-based open data portal. Uses the
"espelhos de acórdãos" (decision summaries) JSON datasets which contain
ementa (headnote) and decisão (decision text) for each ruling.

10 judging bodies are covered: Corte Especial, 3 Seções, 6 Turmas.
Each has monthly JSON snapshots. The latest snapshot per body is used
to avoid duplicates.

Data includes:
  - ementa: legal headnote/summary
  - decisao: full decision text
  - ministroRelator: reporting justice
  - dataDecisao/dataPublicacao: dates
  - referenciasLegislativas: cited legislation

Usage:
  python bootstrap.py bootstrap          # Full initial pull (latest snapshot per body)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import hashlib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.STJDadosAbertos")

CKAN_API = "https://dadosabertos.web.stj.jus.br/api/3/action"

ESPELHOS_DATASETS = [
    "espelhos-de-acordaos-corte-especial",
    "espelhos-de-acordaos-primeira-secao",
    "espelhos-de-acordaos-primeira-turma",
    "espelhos-de-acordaos-quarta-turma",
    "espelhos-de-acordaos-quinta-turma",
    "espelhos-de-acordaos-segunda-secao",
    "espelhos-de-acordaos-segunda-turma",
    "espelhos-de-acordaos-sexta-turma",
    "espelhos-de-acordaos-terceira-secao",
    "espelhos-de-acordaos-terceira-turma",
]


def _parse_stj_date(date_str: str) -> Optional[str]:
    """Parse STJ date formats to ISO 8601."""
    if not date_str:
        return None
    # Format: "20220523" (YYYYMMDD)
    date_str = date_str.strip()
    if re.match(r'^\d{8}$', date_str):
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Format: "DJE        DATA:25/05/2022"
    m = re.search(r'(\d{2}/\d{2}/\d{4})', date_str)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


class STJDadosAbertosScraper(BaseScraper):
    """Scraper for BR/STJDadosAbertos -- STJ Open Data."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _get_all_json_urls(self, dataset_id: str) -> List[str]:
        """Get URLs of ALL JSON resources from a CKAN dataset (all monthly snapshots)."""
        import requests

        url = f"{CKAN_API}/package_show"
        resp = requests.get(url, params={"id": dataset_id}, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            return []

        resources = data["result"]["resources"]
        json_resources = [r for r in resources if r.get("format") == "JSON"]

        if not json_resources:
            return []

        # Sort by name (date-based: YYYYMMDD.json) chronologically
        json_resources.sort(key=lambda r: r.get("name", ""))
        return [r["url"] for r in json_resources]

    def _fetch_json_resource(self, url: str) -> List[Dict]:
        """Download and parse a JSON resource."""
        import requests

        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all records from ALL monthly snapshots of each espelhos dataset."""
        seen_ids = set()
        for dataset_id in ESPELHOS_DATASETS:
            logger.info(f"Processing dataset: {dataset_id}")
            try:
                urls = self._get_all_json_urls(dataset_id)
                if not urls:
                    logger.warning(f"No JSON resources found for {dataset_id}")
                    continue

                logger.info(f"Found {len(urls)} monthly snapshots for {dataset_id}")
                for url in urls:
                    try:
                        records = self._fetch_json_resource(url)
                        logger.info(f"Got {len(records)} records from {url.split('/')[-1]}")

                        for record in records:
                            # Deduplicate by STJ record id
                            rec_id = str(record.get("id", "")).strip()
                            if rec_id and rec_id in seen_ids:
                                continue
                            if rec_id:
                                seen_ids.add(rec_id)
                            record["_dataset"] = dataset_id
                            yield record

                        time.sleep(2)
                    except Exception as e:
                        logger.error(f"Error downloading {url}: {e}")
                        continue

            except Exception as e:
                logger.error(f"Error processing {dataset_id}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch records from snapshots after the given date."""
        logger.info("Use bootstrap for full refresh. Incremental not supported for snapshot data.")
        return
        yield

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw espelhos record into standardized schema."""
        ementa = (raw.get("ementa") or "").strip()
        decisao = (raw.get("decisao") or "").strip()

        # Build full text from ementa + decisao
        text_parts = []
        if ementa:
            text_parts.append(ementa)
        if decisao:
            text_parts.append(decisao)
        text = "\n\n".join(text_parts)

        if not text:
            return None

        # ID from STJ's own id field
        stj_id = str(raw.get("id", "")).strip()
        processo = (raw.get("processo") or "").strip()
        if stj_id:
            doc_id = f"BR-STJ-{stj_id}"
        elif processo:
            doc_id = f"BR-STJ-{processo.replace(' ', '')}"
        else:
            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            doc_id = f"BR-STJ-{text_hash}"

        # Parse dates
        decision_date = _parse_stj_date(raw.get("dataDecisao", ""))
        pub_date = _parse_stj_date(raw.get("dataPublicacao", ""))
        date = decision_date or pub_date

        # Title from classe + processo
        desc_classe = (raw.get("descricaoClasse") or raw.get("siglaClasse") or "").strip()
        title_parts = [desc_classe, processo]
        title = " - ".join(p for p in title_parts if p)
        if not title:
            title = ementa[:150] + ("..." if len(ementa) > 150 else "")

        orgao = (raw.get("nomeOrgaoJulgador") or "").strip()
        dataset = raw.get("_dataset", "")

        return {
            "_id": doc_id,
            "_source": "BR/STJDadosAbertos",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://dadosabertos.web.stj.jus.br/dataset/{dataset}" if dataset else "https://dadosabertos.web.stj.jus.br/",
            "process_number": processo,
            "numero_registro": (raw.get("numeroRegistro") or "").strip(),
            "orgao_julgador": orgao,
            "judge_relator": (raw.get("ministroRelator") or "").strip(),
            "decision_type": (raw.get("tipoDeDecisao") or "").strip(),
            "decision_outcome": (raw.get("teor") or "").strip(),
        }


if __name__ == "__main__":
    scraper = STJDadosAbertosScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing STJ CKAN API connectivity...")
        try:
            import requests
            resp = requests.get(f"{CKAN_API}/package_list", timeout=10)
            data = resp.json()
            if data.get("success"):
                espelhos = [p for p in data["result"] if "espelhos" in p]
                print(f"OK: Found {len(espelhos)} espelhos datasets")
                # Test one JSON download
                urls = scraper._get_all_json_urls(ESPELHOS_DATASETS[0])
                if urls:
                    print(f"Found {len(urls)} snapshots, latest: {urls[-1]}")
            else:
                print("FAIL: CKAN API returned error")
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample else None

        for raw in scraper.fetch_all():
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue

            count += 1
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            if count % 500 == 0:
                logger.info(f"Processed {count} records")

            if limit and count >= limit:
                break

        print(f"Saved {count} records to {sample_dir}/")

    elif cmd == "update":
        print("Snapshot-based dataset -- use bootstrap for full refresh.")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
