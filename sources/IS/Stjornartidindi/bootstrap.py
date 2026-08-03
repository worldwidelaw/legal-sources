#!/usr/bin/env python3
"""
IS/Stjornartidindi -- Iceland Official Gazette (Stjórnartíðindi)

The official State Gazette of Iceland, published under Lög um Stjórnartíðindi og
Lögbirtingablað nr. 15/2005 on the island.is national platform. Three series:
  - A-deild: Acts of the Althingi (LÖG) and presidential/legislative instruments
  - B-deild: regulations, tariffs and administrative rules (reglugerðir)
  - C-deild: treaties and international agreements

Strategy:
  - Enumerate via the public JSON API:
        https://api.stjornartidindi.is/api/v1/adverts?department={dep}&page=N&pageSize=100
    The paging block carries totalPages/totalItems. Each advert object already
    embeds the full born-digital text as document.html (inline) — no per-document
    fetch or OCR is required.
  - normalize() strips the HTML to clean text and maps the metadata.

~39,600 documents total (a-deild ~3,860, b-deild ~34,840, c-deild ~970).

fetch_all() yields RAW advert dicts (already carrying full text); normalize()
does no network, so bootstrap/bootstrap-fast are both efficient.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # full pull, concurrent
  python bootstrap.py update 2025          # documents published in a year onward
  python bootstrap.py test                 # connectivity/parse test
"""

import re
import sys
import json
import time
import logging
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IS.Stjornartidindi")

API = "https://api.stjornartidindi.is/api/v1/adverts"
DEPARTMENTS = ["a-deild", "b-deild", "c-deild"]
PAGE_SIZE = 100

DEPT_LABEL = {
    "a-deild": "A-deild (Lög / Acts)",
    "b-deild": "B-deild (Reglugerðir / Regulations)",
    "c-deild": "C-deild (Milliríkjasamningar / Treaties)",
}


class StjornartidindiScraper(BaseScraper):
    SOURCE_ID = "IS/Stjornartidindi"

    def __init__(self, source_dir=None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "application/json",
        })

    # ── HTTP ──────────────────────────────────────────────────────────
    def _get_page(self, department: str, page: int) -> Optional[dict]:
        url = f"{API}?department={department}&page={page}&pageSize={PAGE_SIZE}"
        for attempt in range(4):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                logger.warning("Page %s of %s attempt %d failed: %s",
                               page, department, attempt + 1, e)
                time.sleep(2 ** attempt)
        return None

    # ── Helpers ───────────────────────────────────────────────────────
    @staticmethod
    def _html_to_text(html: str) -> str:
        html = re.sub(r"<script.*?</script>", " ", html, flags=re.S)
        html = re.sub(r"<style.*?</style>", " ", html, flags=re.S)
        # keep paragraph/line structure
        html = re.sub(r"</(p|div|h[1-6]|li|tr|br|table)\s*>", "\n", html, flags=re.I)
        html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", html)
        text = _html.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text

    @staticmethod
    def _iso_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return value[:10]

    # ── Enumeration ───────────────────────────────────────────────────
    def _iter_department(self, department: str) -> Generator[Dict[str, Any], None, None]:
        first = self._get_page(department, 1)
        if not first:
            return
        total_pages = first.get("paging", {}).get("totalPages", 1)
        total_items = first.get("paging", {}).get("totalItems", 0)
        logger.info("%s: %d items across %d pages", department, total_items, total_pages)
        page = 1
        data = first
        while True:
            for advert in data.get("adverts", []):
                advert["_department_slug"] = department
                yield advert
            page += 1
            if page > total_pages:
                break
            time.sleep(0.3)
            data = self._get_page(department, page)
            if not data:
                logger.warning("Stopping %s at page %d (fetch failed)", department, page)
                break

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for dep in DEPARTMENTS:
            yield from self._iter_department(dep)

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        since_str = str(since)[:10]
        # pad a bare year to a full date for comparison
        if re.fullmatch(r"\d{4}", since_str):
            since_str = f"{since_str}-01-01"
        for advert in self.fetch_all():
            pub = self._iso_date(advert.get("publicationDate"))
            if pub and pub >= since_str:
                yield advert

    # ── Normalization ─────────────────────────────────────────────────
    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        doc = raw.get("document") or {}
        html = doc.get("html") or ""
        if not html:
            return None
        text = self._html_to_text(html)
        if len(text) < 40:
            return None

        dep = raw.get("_department_slug", "")
        pub_num = raw.get("publicationNumber") or {}
        full_num = pub_num.get("full") or str(raw.get("id"))
        year = pub_num.get("year")

        dep_code = dep.split("-")[0].upper() if dep else "X"  # A / B / C
        doc_id = f"IS/Stjornartidindi/{dep_code}-{full_num.replace('/', '-')}"

        party = (raw.get("involvedParty") or {}).get("title")
        type_title = (raw.get("type") or {}).get("title")

        return {
            "_id": doc_id,
            "_source": "IS/Stjornartidindi",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "") or full_num,
            "text": text,
            "date": self._iso_date(raw.get("signatureDate")) or self._iso_date(raw.get("publicationDate")),
            "url": doc.get("pdfUrl") or f"https://www.stjornartidindi.is/Advert.aspx?ID={raw.get('id')}",
            "law_number": full_num,
            "act_type": type_title or DEPT_LABEL.get(dep, dep),
            "series": DEPT_LABEL.get(dep, dep),
            "issuing_body": party,
            "publication_date": self._iso_date(raw.get("publicationDate")),
            "year": year,
            "jurisdiction": "IS",
            "language": "is",
        }

    def test(self) -> bool:
        try:
            data = self._get_page("a-deild", 1)
            if not data or not data.get("adverts"):
                logger.error("No adverts returned")
                return False
            advert = data["adverts"][0]
            advert["_department_slug"] = "a-deild"
            rec = self.normalize(advert)
            ok = bool(rec and len(rec["text"]) > 40)
            if ok:
                logger.info("Test OK: %s -> %d chars", rec["law_number"], len(rec["text"]))
            return ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    scraper = StjornartidindiScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            # sample across departments: take a few from each
            for dep in DEPARTMENTS:
                data = scraper._get_page(dep, 1)
                if not data:
                    continue
                for advert in data.get("adverts", []):
                    advert["_department_slug"] = dep
                    record = scraper.normalize(advert)
                    if not record:
                        continue
                    out_path = sample_dir / f"{count:04d}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)
                    count += 1
                    logger.info("[%d] %s %s — %d chars", count, dep,
                                record.get("law_number"), len(record["text"]))
                    if count % 5 == 0:
                        break  # move to next department after 5
                if count >= 15:
                    break
            logger.info("Done: %d sample records saved", count)
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            logger.info("bootstrap-fast done: %s", stats)
        else:
            stats = scraper.bootstrap()
            logger.info("bootstrap done: %s", stats)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else str(datetime.now().year)
        count = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                count += 1
                logger.info("[%d] %s", count, record.get("law_number"))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
