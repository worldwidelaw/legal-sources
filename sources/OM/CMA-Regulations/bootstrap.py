#!/usr/bin/env python3
"""
OM/CMA-Regulations — Oman Financial Services Authority (FSA) Legislation Encyclopedia

The Capital Market Authority (CMA) of Oman was merged into the Financial Services
Authority (FSA) by Royal Decree No. 20/2024. The FSA is the statutory regulator of
the capital market (securities) and insurance sectors. It publishes the full,
official text of the laws, regulations and decisions governing those sectors in its
online Legislation Encyclopedia (https://e.fsa.gov.om/LegislationEncyclopedia/).

The public cma.gov.om / fsa.gov.om "DecisionsCirculars" listing pages currently
return HTTP 500, but the Legislation Encyclopedia exposes a clean JSON + HTML
back-end that is fully functional:

  * List  (POST, JSON):  /LegislationEncyclopedia/GetPublishedLegislationList?type=<T>
        server-side DataTables endpoint; returns Id, LegislationNumberEn/Ar,
        HeaderEn/Ar, Type, Sector, IssueDate for every published instrument.
  * Detail (GET, HTML):  /LegislationEncyclopedia/DisplayLegislationDetails?id=<id>&type=<T>
        full enacted text inside <div class="contentDetails">.

Types: Law, Regulation, Decision, Circular.
Sectors: 1=General, 2=Insurance, 3=Capital Market.

Most instruments carry their full English text; where only Arabic is published the
Arabic body is captured and the language flagged accordingly. Each record's _type is
"legislation" (laws, regulations, and the executive decisions that enact them) per
the project typing rule; circulars — regulatory guidance — are "doctrine".

Free access, no authentication. The TLS chain on e.fsa.gov.om is incomplete, so
certificate verification is disabled for this host only.

Usage:
  python bootstrap.py bootstrap --sample   # sample records for validation
  python bootstrap.py bootstrap            # full pull
  python bootstrap.py update               # incremental (re-crawl)
  python bootstrap.py test-api             # connectivity / record-count check
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.OM.CMA-Regulations")

BASE_URL = "https://e.fsa.gov.om"
LIST_PATH = "/LegislationEncyclopedia/GetPublishedLegislationList"
DETAIL_PATH = "/LegislationEncyclopedia/DisplayLegislationDetails"
PDF_PATH = "/LegislationEncyclopedia/GetPDF"
SOURCE_ID = "OM/CMA-Regulations"

TYPES = ["Law", "Regulation", "Decision", "Circular"]
SECTORS = {1: "General", 2: "Insurance", 3: "Capital Market"}
# Laws, regulations and the executive decisions that enact them are legislation;
# circulars are regulatory guidance (doctrine).
TYPE_KIND = {"Law": "legislation", "Regulation": "legislation",
             "Decision": "legislation", "Circular": "doctrine"}

MIN_TEXT_CHARS = 200


def _clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _datatables_body() -> dict:
    """Minimal server-side DataTables request that returns every record."""
    cols = ["Id", "", "HeaderEn", "IssueDate"]
    columns = [{"data": c, "name": "", "searchable": True, "orderable": True,
                "search": {"value": "", "regex": False}} for c in cols]
    return {
        "draw": 1, "start": 0, "length": 1000,
        "search": {"value": "", "regex": False},
        "order": [{"column": 3, "dir": "desc"}],
        "columns": columns,
        "searchType": "SearchInTitle", "sector": "", "isExactMatch": "False",
    }


def _is_arabic(text: str) -> bool:
    arabic = sum(1 for ch in text if "؀" <= ch <= "ۿ")
    latin = sum(1 for ch in text if "a" <= ch.lower() <= "z")
    return arabic > latin


class FSAScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/json,*/*",
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/json; charset=utf-8",
            },
            timeout=90,
            verify=False,            # e.fsa.gov.om ships an incomplete TLS chain
            respect_robots=False,
        )
        # Prefer the English rendering of each document where available.
        self.http.session.cookies.set("_culture", "en")

    # ── listing ──────────────────────────────────────────────────────────

    def _list_type(self, leg_type: str) -> list[dict]:
        url = f"{LIST_PATH}?type={leg_type.replace(' ', '%20')}"
        try:
            resp = self.http.post(url, json_data=_datatables_body(), timeout=90)
        except Exception as e:
            logger.warning("List fetch error (%s): %s", leg_type, e)
            return []
        if resp.status_code != 200:
            logger.warning("List %s returned %s", leg_type, resp.status_code)
            return []
        try:
            rows = resp.json().get("data", [])
        except Exception as e:
            logger.warning("List %s JSON error: %s", leg_type, e)
            return []
        for r in rows:
            r["_leg_type"] = leg_type
        logger.info("  %s: %d records", leg_type, len(rows))
        return rows

    def _list_all(self) -> list[dict]:
        out: list[dict] = []
        seen: set[tuple] = set()
        for leg_type in TYPES:
            for r in self._list_type(leg_type):
                key = (leg_type, r.get("Id"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(r)
            time.sleep(1.0)
        logger.info("Total legislation records discovered: %d", len(out))
        return out

    # ── detail / full text ───────────────────────────────────────────────

    def _fetch_text(self, doc_id: int, leg_type: str) -> Optional[str]:
        params = {
            "isHome": "False", "type": leg_type, "id": doc_id,
            "searchType": "SearchInTitle", "sector": "",
            "isExactMatch": "False", "searchWord": "",
        }
        try:
            resp = self.http.get(DETAIL_PATH, params=params, timeout=90)
        except Exception as e:
            logger.warning("Detail fetch error (id=%s): %s", doc_id, e)
            return None
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        node = soup.find(id=f"content_{doc_id}") or soup.find(class_="contentDetails")
        if node is None:
            return None
        for tag in node(["script", "style"]):
            tag.decompose()
        return _clean_text(node.get_text("\n"))

    def _build(self, row: dict) -> Optional[dict]:
        doc_id = row.get("Id")
        leg_type = row.get("_leg_type", "Regulation")
        if doc_id is None:
            return None
        text = self._fetch_text(doc_id, leg_type)
        if not text or len(text) < MIN_TEXT_CHARS:
            return None

        header_en = (row.get("HeaderEn") or "").strip()
        header_ar = (row.get("HeaderAr") or "").strip()
        title = header_en or header_ar
        number = (row.get("LegislationNumberEn") or row.get("LegislationNumberAr") or "").strip()

        issue = row.get("IssueDate") or ""
        date = issue[:10] if re.match(r"\d{4}-\d{2}-\d{2}", issue) else None

        sector = SECTORS.get(row.get("Sector"), None)
        lang = "ar" if _is_arabic(text) else "en"

        rec = {
            "_id": f"om-fsa-{leg_type.lower()}-{doc_id}",
            "_source": SOURCE_ID,
            "_type": TYPE_KIND.get(leg_type, "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}{DETAIL_PATH}?id={doc_id}&type={leg_type}",
            "language": lang,
            "legislation_type": leg_type,
            "legislation_number": number or None,
            "sector": sector,
            "publisher": "Financial Services Authority of Oman (formerly Capital Market Authority)",
            "pdf_url": f"{BASE_URL}{PDF_PATH}/{doc_id}?type={leg_type}",
        }
        if header_ar:
            rec["title_ar"] = header_ar
        return rec

    # ── BaseScraper interface ────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for row in self._list_all():
            rec = self._build(row)
            if rec:
                yield rec
            time.sleep(1.2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        # The encyclopedia carries a small, slowly-changing corpus; re-crawl in
        # IssueDate-descending order and stop once we pass `since`.
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        for row in self._list_all():
            issue = (row.get("IssueDate") or "")[:10]
            if since and issue and issue < since[:10]:
                continue
            rec = self._build(row)
            if rec:
                yield rec
            time.sleep(1.2)

    def normalize(self, raw: dict) -> dict:
        return raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="OM/CMA-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = FSAScraper()

    if args.command == "test-api":
        rows = scraper._list_all()
        for r in rows[:15]:
            logger.info("  [%s/%s] %s — %s", r.get("_leg_type"), r.get("Id"),
                        r.get("LegislationNumberEn"), (r.get("HeaderEn") or r.get("HeaderAr"))[:55])
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    limit = 15 if args.sample else None
    count = 0
    for record in scraper.fetch_all():
        count += 1
        if args.sample or count <= 15:
            with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("[%d] %s — %d chars (%s, %s)", count,
                    record["title"][:50], len(record["text"]),
                    record.get("date"), record.get("language"))
        if limit and count >= limit:
            break
    logger.info("Done: %d records", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
