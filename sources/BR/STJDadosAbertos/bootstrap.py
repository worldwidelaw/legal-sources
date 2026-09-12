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

    @staticmethod
    def _get_json_with_retry(url: str, params: Optional[Dict] = None,
                             timeout: int = 60, attempts: int = 4):
        """GET and parse JSON, retrying transient upstream failures (#1453).

        dadosabertos.web.stj.jus.br sits behind a proxy that intermittently
        answers 5xx (520 observed on package_show) or returns a truncated body
        that will not parse. Both were seen in a single probing session, and
        either one previously cost the run an entire collegiate body's
        snapshots, so they are retried rather than propagated.
        """
        import requests

        last = None
        for attempt in range(attempts):
            try:
                resp = requests.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                last = e
                status = getattr(getattr(e, "response", None), "status_code", None)
                # A real 404 will not fix itself; stop rather than burn retries.
                if status is not None and status in (400, 401, 403, 404):
                    raise
                if attempt == attempts - 1:
                    raise
                delay = 2 ** attempt
                logger.warning(f"Retrying {url} in {delay}s after {type(e).__name__}: {e}")
                time.sleep(delay)
        raise last  # unreachable; keeps the contract explicit

    def _get_json_resources(self, dataset_id: str) -> List[Dict]:
        """Get ALL JSON resources of a CKAN dataset (the monthly snapshots)."""
        data = self._get_json_with_retry(f"{CKAN_API}/package_show",
                                         params={"id": dataset_id}, timeout=15)

        if not data.get("success"):
            return []

        resources = data["result"]["resources"]
        json_resources = [r for r in resources if r.get("format") == "JSON"]

        # Sort by name (date-based: YYYYMMDD.json) chronologically
        json_resources.sort(key=lambda r: r.get("name", ""))
        return json_resources

    def _get_all_json_urls(self, dataset_id: str) -> List[str]:
        """URLs of all JSON snapshots of a dataset, chronologically."""
        return [r["url"] for r in self._get_json_resources(dataset_id)]

    @staticmethod
    def _published_at(resource: Dict) -> str:
        """When a snapshot became available to us, as an ISO timestamp.

        Deliberately NOT the `YYYYMMDD.json` name: that is the period the
        snapshot *covers*, and STJ uploads every one of them after that period
        closes -- all 150 checked, by up to 52 days. Comparing the name against
        a crawl-time cutoff would therefore skip snapshots that appeared since
        the last run, which is exactly the set we are here to collect.
        """
        return max(resource.get("last_modified") or "",
                   resource.get("created") or "")

    def _fetch_json_resource(self, url: str) -> List[Dict]:
        """Download and parse a JSON resource."""
        return self._get_json_with_retry(url, timeout=60)

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
        """Yield records from snapshots published since the last run.

        This was a `return; yield` stub, so every refresh reported 0 records
        and the caller fell back to a full re-crawl of all ~500 snapshots
        across the 10 collegiate bodies (#1502).

        Snapshot data turns out to be well suited to an incremental path: the
        monthly files are disjoint deltas, not cumulative dumps -- verified
        live, consecutive snapshots share zero record ids. So downloading only
        the snapshots published after the cutoff yields exactly the new records
        and nothing else.

        Note the filter is on snapshot publication, never on the records'
        `dataDecisao`. STJ routinely publishes older decisions in a current
        snapshot, so a per-record decision-date cutoff would silently drop
        precisely those late-published acordaos.
        """
        cutoff = since.isoformat()
        seen_ids = set()
        considered = downloaded = 0

        for dataset_id in ESPELHOS_DATASETS:
            try:
                resources = self._get_json_resources(dataset_id)
            except Exception as e:
                logger.error(f"Error listing {dataset_id}: {e}")
                continue

            considered += len(resources)
            fresh = [r for r in resources if self._published_at(r) >= cutoff]
            if not fresh:
                logger.info(f"{dataset_id}: no snapshot published since {cutoff[:10]}")
                continue

            logger.info(f"{dataset_id}: {len(fresh)} of {len(resources)} snapshots are new")
            for resource in fresh:
                try:
                    records = self._fetch_json_resource(resource["url"])
                    downloaded += 1
                    logger.info(f"Got {len(records)} records from {resource.get('name')}")

                    for record in records:
                        rec_id = str(record.get("id", "")).strip()
                        if rec_id and rec_id in seen_ids:
                            continue
                        if rec_id:
                            seen_ids.add(rec_id)
                        record["_dataset"] = dataset_id
                        yield record

                    time.sleep(2)
                except Exception as e:
                    logger.error(f"Error downloading {resource.get('url')}: {e}")
                    continue

        logger.info(
            f"Incremental refresh: {len(seen_ids)} record(s) from {downloaded} "
            f"snapshot(s); skipped {considered - downloaded} already-ingested snapshots"
        )

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

        # ID from STJ's own id field. (Field is `numeroProcesso`, not `processo` —
        # the old `processo` lookup always returned "" so process_number/title were
        # blank and the id fallbacks never had a usable value.)
        stj_id = str(raw.get("id", "")).strip()
        processo = (raw.get("numeroProcesso") or "").strip()
        registro = (raw.get("numeroRegistro") or "").strip()
        if stj_id:
            doc_id = f"BR-STJ-{stj_id}"
        elif registro:
            doc_id = f"BR-STJ-{re.sub(r'[^0-9]', '', registro)}"
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

        # Per-document URL. The dataset-only URL made every record in a judging
        # body share one of just 10 URLs, which collapsed the corpus at ingest
        # (dedup/upsert keyed on url). Anchor the (valid, resolvable) dataset page
        # with the STJ registration number so each acórdão has a distinct URL.
        anchor = stj_id or re.sub(r"[^0-9]", "", registro)
        if dataset and anchor:
            url = f"https://dadosabertos.web.stj.jus.br/dataset/{dataset}#{anchor}"
        elif dataset:
            url = f"https://dadosabertos.web.stj.jus.br/dataset/{dataset}"
        else:
            url = "https://dadosabertos.web.stj.jus.br/"

        return {
            "_id": doc_id,
            "_source": "BR/STJDadosAbertos",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "process_number": processo,
            "numero_registro": registro,
            "orgao_julgador": orgao,
            "judge_relator": (raw.get("ministroRelator") or "").strip(),
            "decision_type": (raw.get("tipoDeDecisao") or "").strip(),
            "decision_outcome": (raw.get("tipoDeDecisao") or "").strip(),
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
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif cmd == "bootstrap-fast":
        workers = 5
        batch_size = 100
        for i, arg in enumerate(sys.argv):
            if arg == "--workers" and i + 1 < len(sys.argv):
                workers = int(sys.argv[i + 1])
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        fetched = stats.get("records_fetched", 0)
        logger.info(f"Bootstrap-fast complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
