#!/usr/bin/env python3
"""
IT/ConsiglioStatoOrdinanze -- Italian Council of State: Ordinanze & Decreti

Fetches the *interlocutory* administrative decisions of the Consiglio di Stato
(Council of State) -- ordinanze (orders, incl. ordinanze cautelari / interim
relief) and decreti (monocratic decrees) -- from the OpenGA CKAN portal, with
full text pulled from the mdp XML endpoint.

This is the companion to IT/ConsiglioDiStato (which covers *sentenze* / final
judgments). The two corpora do not overlap: NUMERO_PROVVEDIMENTO sequences are
per document type, so the _id here carries the doc-type marker (ORD / DEC).

Data Portal:       https://openga.giustizia-amministrativa.it
Full Text Endpoint: https://mdp.giustizia-amministrativa.it/visualizza/
License:           CC BY 4.0

Scope note: only the Consiglio di Stato jurisdictional datasets resolve full
text on the mdp endpoint with schema=cds (verified). The TAR/CGA datasets use
internal mdp schema codes that are not derivable from the CKAN dataset slug, so
they are intentionally out of scope here.

MDP document-model suffixes (verified 2026-07-14 against live records):
  sentenze -> _11   ordinanze -> _15   decreti -> _35

Usage:
  python bootstrap.py bootstrap --sample   # validation sample -> sample/
  python bootstrap.py bootstrap            # full pull (BaseScraper persist)
  python bootstrap.py bootstrap-fast       # concurrent full pull (fleet entry)
  python bootstrap.py update               # incremental (recent decisions)
  python bootstrap.py test-api             # connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
import xml.etree.ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.ConsiglioStatoOrdinanze")

CKAN_API = "https://openga.giustizia-amministrativa.it/api/3/action"
MDP_ENDPOINT = "https://mdp.giustizia-amministrativa.it/visualizza/"

# CKAN datasets covered by this source (Consiglio di Stato only).
# Maps dataset name -> (mdp schema, primary document-model suffix, doc-type code)
DATASETS = {
    "cds-ordinanze": ("cds", "15", "ORD"),
    "cds-decreti": ("cds", "35", "DEC"),
}

# Suffixes to try (primary first) in case a record uses a different model code.
# The ordinanze model code evolves by year: _15 (2023-2024) -> _18 (2025+);
# decreti are stable at _35. The fallback list covers all observed codes.
FALLBACK_SUFFIXES = ["15", "18", "35", "11"]


class ConsiglioStatoOrdinanzeScraper(BaseScraper):
    """
    Scraper for IT/ConsiglioStatoOrdinanze.
    Country: IT | Data type: case_law | Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- CKAN helpers ---------------------------------------------------------

    def _get_dataset_resources(self, dataset_name: str) -> list:
        self.rate_limiter.wait()
        resp = self.client.get(f"{CKAN_API}/package_show", params={"id": dataset_name})
        resp.raise_for_status()
        data = resp.json()
        return data["result"].get("resources", []) if data.get("success") else []

    def _get_all_json_resources(self, dataset_name: str) -> list:
        """Return [(year, resource_id), ...] sorted ascending by year."""
        out = []
        for r in self._get_dataset_resources(dataset_name):
            if r.get("format", "").upper() == "JSON":
                m = re.search(r"(\d{4})", r.get("name", ""))
                out.append((int(m.group(1)) if m else 0, r["id"]))
        out.sort()
        return out

    def _query_datastore(self, resource_id: str, limit: int = 100, offset: int = 0) -> dict:
        self.rate_limiter.wait()
        resp = self.client.get(
            f"{CKAN_API}/datastore_search",
            params={"resource_id": resource_id, "limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        return resp.json()

    # -- Full text ------------------------------------------------------------

    def _mdp_url(self, schema: str, nrg, num_provv, suffix: str) -> str:
        return (
            f"{MDP_ENDPOINT}?nodeRef=&schema={schema}&nrg={nrg}"
            f"&nomeFile={num_provv}_{suffix}.html&subDir=Provvedimenti"
        )

    def _extract_text_from_xml(self, xml_content: str) -> str:
        try:
            xml_content = re.sub(r"<\?xml[^>]*\?>", "", xml_content)
            xml_content = re.sub(r"<\?xml-stylesheet[^>]*\?>", "", xml_content)
            root = ET.fromstring(xml_content)

            def get_text(el) -> str:
                parts = []
                if el.text:
                    parts.append(el.text.strip())
                for c in el:
                    parts.append(get_text(c))
                    if c.tail:
                        parts.append(c.tail.strip())
                return " ".join(filter(None, parts))

            text = html.unescape(re.sub(r"\s+", " ", get_text(root)))
            return text.strip()
        except ET.ParseError:
            text = re.sub(r"<[^>]+>", " ", xml_content)
            return html.unescape(re.sub(r"\s+", " ", text)).strip()

    def _fetch_full_text(self, schema: str, nrg, num_provv, primary_suffix: str):
        """Return (text, used_suffix) or (None, None). Tries primary then fallbacks."""
        tried = [primary_suffix] + [s for s in FALLBACK_SUFFIXES if s != primary_suffix]
        for suffix in tried:
            url = self._mdp_url(schema, nrg, num_provv, suffix)
            try:
                self.rate_limiter.wait()
                resp = self.client.get(url)
                if resp.status_code == 200 and ("<Provvedimento>" in resp.text or "<GA" in resp.text):
                    return self._extract_text_from_xml(resp.text), suffix
            except Exception as e:
                logger.debug(f"fetch {url} failed: {e}")
        return None, None

    # -- Normalize ------------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        num_provv = raw.get("NUMERO_PROVVEDIMENTO", "")
        num_ricorso = raw.get("NUMERO_RICORSO", "")
        data_pub = raw.get("DATA_PUBBLICAZIONE", "")
        anno = raw.get("ANNO_PUBBLICAZIONE", "")
        sede = raw.get("NOME_SEDE", "")
        sezione = raw.get("NOME_SEZIONE", "")
        tipo = raw.get("TIPO_PROVVEDIMENTO", "")
        esito = raw.get("ESITO_PROVVEDIMENTO", "")
        oggetto = raw.get("OGGETTO_RICORSO", "") or ""

        full_text = raw.get("_text", "")
        doctype = raw.get("_doctype", "")
        used_suffix = raw.get("_suffix", "11")
        if not full_text:
            return None

        doc_id = f"IT:GA:CDS:{anno}:{doctype}:{num_provv}"

        title = f"{tipo} n. {num_provv}/{anno}"
        if sezione:
            title += f" - {sezione}"
        if oggetto:
            title += f" - {oggetto[:100]}"

        source_url = self._mdp_url("cds", num_ricorso, num_provv, used_suffix)

        return {
            "_id": doc_id,
            "_source": "IT/ConsiglioStatoOrdinanze",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": data_pub if data_pub else f"{anno}-01-01",
            "url": source_url,
            "court": sede,
            "section": sezione,
            "decision_type": tipo,
            "outcome": esito,
            "case_number": str(num_ricorso),
            "decision_number": str(num_provv),
            "year": str(anno),
            "subject": oggetto,
            "language": "it",
            "license": "CC BY 4.0",
        }

    # -- BaseScraper API ------------------------------------------------------

    def _iter_records(self, datasets: dict, since_str: Optional[str] = None):
        """Yield raw CKAN records enriched with full text (_text/_doctype/_suffix)."""
        for dataset_name, (schema, primary_suffix, doctype) in datasets.items():
            logger.info(f"Processing dataset: {dataset_name}")
            try:
                year_resources = self._get_all_json_resources(dataset_name)
                if not year_resources:
                    logger.warning(f"No JSON resources for {dataset_name}")
                    continue
                for year, resource_id in year_resources:
                    logger.info(f"  {dataset_name} year {year}")
                    offset, limit = 0, 100
                    while True:
                        result = self._query_datastore(resource_id, limit=limit, offset=offset)
                        if not result.get("success"):
                            break
                        records = result.get("result", {}).get("records", [])
                        if not records:
                            break
                        for rec in records:
                            if since_str:
                                pub = rec.get("DATA_PUBBLICAZIONE", "")
                                if not pub or pub < since_str:
                                    continue
                            nrg = rec.get("NUMERO_RICORSO")
                            np = rec.get("NUMERO_PROVVEDIMENTO")
                            if not (nrg and np):
                                continue
                            text, suffix = self._fetch_full_text(schema, nrg, np, primary_suffix)
                            if text and len(text) > 500:
                                rec["_text"] = text
                                rec["_doctype"] = doctype
                                rec["_suffix"] = suffix
                                rec["_dataset_name"] = dataset_name
                                yield rec
                        offset += limit
            except Exception as e:
                logger.error(f"Error processing {dataset_name}: {e}")
                continue

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_records(DATASETS)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self._iter_records(DATASETS, since_str=since.strftime("%Y-%m-%d"))

    def test_api(self) -> bool:
        try:
            resp = self.client.get(f"{CKAN_API}/package_show", params={"id": "cds-ordinanze"})
            if resp.status_code != 200 or not resp.json().get("success"):
                logger.error("CKAN package_show failed for cds-ordinanze")
                return False
            # Verify a live full-text fetch works
            res = self._get_all_json_resources("cds-ordinanze")
            year, rid = res[-1]
            recs = self._query_datastore(rid, limit=1).get("result", {}).get("records", [])
            if not recs:
                logger.error("No ordinanze records returned")
                return False
            r = recs[0]
            text, suffix = self._fetch_full_text("cds", r["NUMERO_RICORSO"], r["NUMERO_PROVVEDIMENTO"], "15")
            if not text or len(text) < 500:
                logger.error("MDP full-text fetch returned no/short text")
                return False
            logger.info(f"API test passed (sample {len(text)} chars, suffix _{suffix})")
            return True
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IT/ConsiglioStatoOrdinanze data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--full", action="store_true", help="Full fetch (default for bootstrap)")
    args = parser.parse_args()

    scraper = ConsiglioStatoOrdinanzeScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap" and args.sample:
        stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
        logger.info(f"Sample complete: {stats.get('sample_records_saved', 0)} records saved")
        return

    if args.command in ("bootstrap", "bootstrap-fast"):
        # Fleet calls bootstrap-fast; route to the concurrent full path.
        stats = scraper.bootstrap_fast() if args.command == "bootstrap-fast" else scraper.bootstrap()
        logger.info(
            f"{args.command} complete: {stats['records_fetched']} fetched, "
            f"{stats['records_new']} new, {stats['records_updated']} updated, "
            f"{stats['records_skipped']} skipped, {stats['errors']} errors"
        )
        return

    if args.command == "update":
        stats = scraper.update()
        logger.info(
            f"Update complete: {stats['records_fetched']} fetched, "
            f"{stats['records_new']} new, {stats['records_skipped']} skipped"
        )


if __name__ == "__main__":
    main()
