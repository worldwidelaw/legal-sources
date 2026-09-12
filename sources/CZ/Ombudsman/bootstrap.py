#!/usr/bin/env python3
"""
Czech Ombudsman ESO Database — Investigation Reports & Legal Opinions

Fetches from eso.ochrance.cz (Evidence stanovisek ombudsmana).
Session-based: POST search form to select all docs, paginate table,
then fetch individual detail pages for full text.
~6,500+ documents covering ombudsman investigations, discrimination cases,
detention monitoring, and legal opinions from 2000 onwards.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://eso.ochrance.cz"
SEARCH_URL = f"{BASE_URL}/Vyhledavani/Search"
TABLE_URL = f"{BASE_URL}/Nalezene/GetTableContent"
DETAIL_URL = f"{BASE_URL}/Nalezene/Edit"
COUNT_URL = f"{BASE_URL}/Vyhledavani/GetPocetVysledku"
SOURCE_ID = "CZ/Ombudsman"
RATE_LIMIT = 1.5
ROWS_PER_PAGE = 50


class OmbudsmanFetcher:
    """Fetcher for Czech Ombudsman ESO Database."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self._last_request = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < RATE_LIMIT:
            time.sleep(RATE_LIMIT - elapsed)

    def _get(self, url: str, **kwargs) -> requests.Response:
        self._rate_limit()
        self._last_request = time.time()
        resp = self.session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def _post(self, url: str, data: dict = None, **kwargs) -> requests.Response:
        self._rate_limit()
        self._last_request = time.time()
        resp = self.session.post(url, data=data, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def _init_search(self) -> int:
        """Initialize session and submit search for all documents. Returns total count."""
        # Get session cookie
        self._get(SEARCH_URL)

        # Submit search selecting all documents via Poslednich (last N) param
        search_data = {
            "ModelMode": "Vyhledat",
            "FullText": "",
            "SpisovaZnackaVcelku": "",
            "PoradoveCislo": "",
            "Rok": "",
            "Agenda": "",
            "FormaZjisteni": "",
            "VysledekSetreni": "",
            "OblastPrava": "",
            "Vec": "",
            "DatumPodaniOd": "",
            "DatumPodaniDo": "",
            "DatumVydaniOd": "",
            "DatumVydaniDo": "",
            "Poslednich": "100000",
            "JenSPravniVetou": "false",
            "TextDokumentuFulltext": "",
            "PravniVetaFulltext": "",
        }

        # POST form — expects 302 redirect to /Nalezene
        resp = self.session.post(
            SEARCH_URL,
            data=search_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
            allow_redirects=False,
        )
        self._last_request = time.time()

        if resp.status_code not in (200, 302):
            raise RuntimeError(f"Search form POST returned {resp.status_code}")

        # Get result count
        self._rate_limit()
        self._last_request = time.time()
        count_resp = self.session.post(
            COUNT_URL,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Content-Length": "0",
            },
            timeout=30,
        )
        count_data = count_resp.json()
        total = count_data.get("Nalezene", 0)
        logger.info(f"Search returned {total} documents")
        return total

    def _get_table_page(self, page: int) -> str:
        """Fetch one page of table results HTML."""
        data = {
            "page": str(page),
            "rows": str(ROWS_PER_PAGE),
            "sidx": "",
            "sord": "asc",
        }
        resp = self._post(
            TABLE_URL,
            data=data,
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        return resp.text

    def _parse_table_page(self, html: str) -> List[Tuple[int, str]]:
        """Extract (doc_id, file_reference) pairs from table HTML."""
        results = []
        pattern = r'data-itemid="(\d+)".*?<a href="/Nalezene/Edit/\d+"><span[^>]*>(.*?)</span></a>'
        for match in re.finditer(pattern, html, re.DOTALL):
            doc_id = int(match.group(1))
            file_ref = unescape(re.sub(r"<[^>]+>", "", match.group(2))).strip()
            results.append((doc_id, file_ref))
        return results

    def _parse_detail_page(self, html: str) -> Dict[str, Any]:
        """Extract metadata and full text from a detail page."""
        meta = {}

        # Extract labeled metadata fields
        field_pattern = (
            r'<span[^>]*class="tucny col-sm-3 form-control-label"[^>]*>(.*?)</span>\s*'
            r'<span[^>]*class="col-sm-9"[^>]*>(.*?)</span>'
        )
        for match in re.finditer(field_pattern, html, re.DOTALL):
            label = unescape(re.sub(r"<[^>]+>", "", match.group(1))).strip()
            value = unescape(re.sub(r"<[^>]+>", " ", match.group(2))).strip()
            value = re.sub(r"\s+", " ", value).strip()

            if "Spisová značka" in label:
                meta["file_reference"] = value
            elif "Oblast práva" in label:
                meta["legal_area"] = value
            elif "Věc" in label:
                meta["subject"] = value
            elif "Forma zjištění" in label:
                meta["finding_type"] = value
            elif "Výsledek šetření" in label:
                meta["result"] = value
            elif "Datum podání" in label:
                meta["submission_date"] = self._parse_czech_date(value)
            elif "Datum vydání" in label:
                meta["issue_date"] = self._parse_czech_date(value)

        # Extract full text from text_dokumentu
        text_match = re.search(
            r'class="text_dokumentu"[^>]*>(.*?)</(?:div|p)>',
            html,
            re.DOTALL,
        )
        if text_match:
            raw_text = text_match.group(1)
            # Clean HTML tags but preserve line breaks
            text = re.sub(r"<br\s*/?>", "\n", raw_text)
            text = re.sub(r"<[^>]+>", "", text)
            text = unescape(text).strip()
            meta["text"] = text

        # Extract legal opinion (právní věta) if present
        pv_match = re.search(
            r'class="pravni_vety"[^>]*>(.*?)</div>',
            html,
            re.DOTALL,
        )
        if pv_match:
            pv_text = re.sub(r"<[^>]+>", "", pv_match.group(1))
            pv_text = unescape(pv_text).strip()
            if pv_text:
                meta["legal_opinion"] = pv_text

        return meta

    @staticmethod
    def _parse_czech_date(date_str: str) -> Optional[str]:
        """Parse Czech date format '09. 04. 2025' or '09.04.2025' to ISO."""
        if not date_str:
            return None
        cleaned = date_str.strip().replace(" ", "")
        match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", cleaned)
        if match:
            return f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
        return None

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield raw document data from all ESO records."""
        total = self._init_search()
        if total == 0:
            logger.warning("No documents found in search")
            return

        count = 0
        page = 1
        total_pages = (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE

        while page <= total_pages:
            if limit and count >= limit:
                return

            logger.info(f"Fetching table page {page}/{total_pages}")
            try:
                table_html = self._get_table_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch table page {page}: {e}")
                break

            items = self._parse_table_page(table_html)
            if not items:
                logger.info(f"No items on page {page}, stopping")
                break

            for doc_id, file_ref in items:
                if limit and count >= limit:
                    return

                logger.info(f"  Fetching detail {doc_id}: {file_ref}")
                try:
                    detail_resp = self._get(f"{DETAIL_URL}/{doc_id}")
                    meta = self._parse_detail_page(detail_resp.text)
                except Exception as e:
                    logger.warning(f"  Failed to fetch detail {doc_id}: {e}")
                    continue

                meta["doc_id"] = doc_id
                if "file_reference" not in meta:
                    meta["file_reference"] = file_ref

                count += 1
                yield meta

                if count % 10 == 0:
                    logger.info(f"  Fetched {count} documents so far...")

            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into standard schema."""
        file_ref = raw.get("file_reference", "")
        doc_id = raw.get("doc_id", "")
        safe_id = re.sub(r"[/\\]", "-", file_ref) if file_ref else str(doc_id)

        date = raw.get("issue_date") or raw.get("submission_date")

        return {
            "_id": f"OMBUDSMAN-{safe_id}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Ombudsman: {file_ref}" if file_ref else f"Ombudsman doc {doc_id}",
            "text": raw.get("text", ""),
            "date": date,
            "url": f"{DETAIL_URL}/{doc_id}",
            "file_reference": file_ref,
            "legal_area": raw.get("legal_area", ""),
            "subject": raw.get("subject", ""),
            "finding_type": raw.get("finding_type", ""),
            "result": raw.get("result", ""),
            "submission_date": raw.get("submission_date"),
            "issue_date": raw.get("issue_date"),
            "legal_opinion": raw.get("legal_opinion", ""),
        }


def main():
    fetcher = OmbudsmanFetcher()

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap of CZ/Ombudsman...")

        sample_count = 0
        target = 15 if "--sample" in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target * 2):
            if sample_count >= target:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            if text_len < 100:
                logger.warning(
                    f"  Skipping {normalized['file_reference']}: "
                    f"only {text_len} chars of text"
                )
                continue

            doc_id = normalized["_id"].replace("/", "_").replace(" ", "_")
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            sample_count += 1
            logger.info(
                f"Saved [{sample_count}/{target}]: {normalized['file_reference']} "
                f"({text_len:,} chars)"
            )

        logger.info(f"Bootstrap complete. {sample_count} documents saved to {sample_dir}")

        files = list(sample_dir.glob("*.json"))
        total_chars = sum(
            len(json.load(open(f, encoding="utf-8")).get("text", ""))
            for f in files
        )
        logger.info(f"Summary: {len(files)} files, {total_chars:,} total text chars")
        if files:
            logger.info(f"Average: {total_chars // len(files):,} chars/document")
    else:
        print("Usage: python bootstrap.py bootstrap [--sample]")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
