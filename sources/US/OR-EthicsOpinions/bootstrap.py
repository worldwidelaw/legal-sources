#!/usr/bin/env python3
"""
US/OR-EthicsOpinions -- Oregon Government Ethics Commission (OGEC) --
Advisory & Staff Opinions.

Fetches the full text of the formal ethics guidance issued by the Oregon
Government Ethics Commission (OGEC), the state agency created under ORS
chapter 244 to interpret and enforce Oregon Government Ethics law, the
public-meetings law and the lobbying-regulation law. The OGEC issues written
opinions construing the standards of conduct applicable to public officials:

  - Commission Advisory Opinion -- adopted by the Commission under ORS 244.280,
    binding guidance on which the requester (and the public) may rely.
  - Staff Advisory Opinion -- issued by the Executive Director, the staff's
    written assessment of how the ethics laws apply to stated facts.

Both are the agency's written interpretation of Oregon ethics statutes = doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  The OGEC public-records site is Microsoft SharePoint. The historical opinion
  corpus (issued prior to 2017) is published as a SharePoint list whose items
  are enumerated through the public, unauthenticated SharePoint REST API:

      https://www.oregon.gov/ogec/public-records/_api/web/lists
          /getbytitle('Advisory & Staff Opinions ~ Prior to 2017')/items

  Each list item carries the publication number (Title), issue date, opinion
  type, topic/jurisdiction/summary index metadata, and an HTML "View Details"
  anchor whose href points at the born-digital opinion PDF under

      https://www.oregon.gov/ogec/public-records/Documents/{PublicationNo}.pdf

  A single opinion is indexed under several subject rows, so the list has more
  rows than distinct opinions; records are deduped by PDF href and the subject
  topics are aggregated.

  (Opinions issued 2017-present live in the OGEC Case Management System at
  apps.oregon.gov/OGEC/CMS/Advice. Its metadata is public via a DataTables
  endpoint but the opinion FILES themselves are served only through an
  authenticated File/GetFile route (HTTP 500 / login-gated for anonymous
  callers), so full text is not retrievable for that set; this scraper covers
  the fully-public pre-2017 born-digital corpus.)

Strategy:
  Pull the SharePoint list in one REST call, dedup by PDF href, aggregate the
  per-subject topics/summaries, download each opinion PDF, verify the %PDF
  magic (SharePoint returns a 200 HTML soft-404 page for a missing file) and
  extract full text via the shared common.pdf_extract backend chain. All
  records are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OR-EthicsOpinions")

BASE_URL = "https://www.oregon.gov"
SITE = "https://www.oregon.gov/ogec/public-records"
LIST_TITLE = "Advisory & Staff Opinions ~ Prior to 2017"
LIST_ITEMS_URL = (
    SITE
    + "/_api/web/lists/getbytitle('"
    + quote(LIST_TITLE)
    + "')/items?$top=1000"
)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

HREF_RE = re.compile(r"href=['\"]([^'\"]+\.pdf)['\"]", re.I)

TYPE_NAMES = {
    "A": "Advisory Opinion",
    "S": "Staff Advisory Opinion",
}


def _iso_date(raw_date: str | None) -> str | None:
    """SharePoint dates are ISO-8601 (e.g. '1997-04-03T08:00:00Z')."""
    if not raw_date:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw_date)
    return m.group(0) if m else None


def _clean(text: str | None) -> str | None:
    if not text:
        return None
    text = text.replace("\xa0", " ").replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _classify(pub_no: str, opinion_type: str | None) -> str:
    """Map the OGEC publication number / type flag to a short type code.

    Publication numbers look like '97A-1001' (Advisory) or '05S-002' (Staff);
    the letter after the 2-digit year encodes the series.
    """
    m = re.search(r"\d{2}([A-Za-z])", pub_no or "")
    if m:
        letter = m.group(1).upper()
        if letter in TYPE_NAMES:
            return letter
    if opinion_type and opinion_type.strip().lower().startswith("staff"):
        return "S"
    return "A"


class OREthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str, as_json: bool = False):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                headers = {"Accept": "application/json;odata=nometadata"} if as_json else {}
                return self.session.get(url, headers=headers, timeout=60,
                                        allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _collect_index(self) -> list[dict]:
        """Read the SharePoint list, dedup by PDF href, aggregate subjects."""
        r = self._get(LIST_ITEMS_URL, as_json=True)
        if r is None or r.status_code != 200:
            logger.error(f"List fetch failed (status="
                         f"{getattr(r, 'status_code', None)})")
            return []
        try:
            items = r.json().get("value", [])
        except Exception as e:
            logger.error(f"List JSON parse failed: {e}")
            return []

        by_href: dict[str, dict] = {}
        for it in items:
            pub_no = _clean(it.get("Title")) or ""
            li = it.get("Op_x002e__x0020_Details_x0020_Li") or ""
            m = HREF_RE.search(li)
            href = m.group(1) if m else None
            if not href and pub_no:
                href = f"/ogec/public-records/Documents/{pub_no}.pdf"
            if not href:
                continue
            if href.startswith("/"):
                href = BASE_URL + href

            topic = _clean(it.get("Topic"))
            summary = _clean(it.get("Summary"))
            juris = _clean(it.get("Jurisdiction"))
            date = _iso_date(it.get("Date_x0020_Issued"))
            otype = _clean(it.get("Opinion_x0020_Type"))

            existing = by_href.get(href)
            if existing is None:
                by_href[href] = {
                    "pub_no": pub_no,
                    "url": href,
                    "type_code": _classify(pub_no, otype),
                    "date": date,
                    "opinion_type": otype,
                    "target_jurisdiction": juris,
                    "topics": [topic] if topic else [],
                    "summaries": [summary] if summary else [],
                }
            else:
                if topic and topic not in existing["topics"]:
                    existing["topics"].append(topic)
                if summary and summary not in existing["summaries"]:
                    existing["summaries"].append(summary)
                if not existing.get("date") and date:
                    existing["date"] = date

        ordered = sorted(
            by_href.values(),
            key=lambda x: (x.get("date") or "", x["pub_no"]),
            reverse=True,
        )
        logger.info(f"Index collected: {len(ordered)} distinct opinions "
                    f"(from {len(items)} subject rows)")
        return ordered

    def _fetch_one(self, row: dict) -> dict | None:
        """Download the opinion PDF and attach extracted full text."""
        r = self._get(row["url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        # SharePoint answers a missing file with a 200 HTML soft-404 page.
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row['pub_no']}: not a PDF (soft-404) — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row['pub_no']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['pub_no']} OK ({len(rec['text'])} chars, "
                            f"date={rec.get('date')})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Oregon OGEC ethics opinions...")
        idx = self._collect_index()
        if len(idx) < 20:
            logger.error(f"API test FAILED: index too small ({len(idx)})")
            return False
        ok = 0
        for row in idx[:5]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['pub_no']} OK ({len(rec['text'])} chars, "
                            f"date={rec.get('date')})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        code = raw["type_code"]
        type_name = TYPE_NAMES.get(code, "Advisory Opinion")
        pub_no = raw["pub_no"]
        slug = re.sub(r"[^A-Za-z0-9]+", "-", pub_no).strip("-") or "unknown"
        topics = raw.get("topics") or []
        summaries = raw.get("summaries") or []
        # Build a human title: publication number + first subject/summary.
        subject = (summaries[0] if summaries else (topics[0] if topics else None))
        title = f"OGEC {type_name} {pub_no}"
        if subject:
            title = f"{title}: {subject}"
        return {
            "_id": f"US/OR-EthicsOpinions/{slug}",
            "_source": "US/OR-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": pub_no,
            "document_type": type_name,
            "issuer": "Oregon Government Ethics Commission",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "topics": topics or None,
            "target_jurisdiction": raw.get("target_jurisdiction"),
            "jurisdiction": "US-OR",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/OR-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OREthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
