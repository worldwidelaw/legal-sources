#!/usr/bin/env python3
"""
AU/QLD-Legislation -- Queensland Legislation Fetcher

Fetches Queensland Acts and subordinate legislation from the OQPC portal.

Strategy:
  - Enumerate every document ID from the portal's own browse API
    (/projectdata, ds=OQPC-BrowseDataSource) rather than probing an ID space
  - Fetch full text XML via /view/whole/xml/{rendition}/{id}
  - Extract text from QuILLS DTD XML (act/part/clause/heading/txt elements)
  - No auth required; CC BY 4.0 license

Data (~21,000 documents across six renditions):
  - In force Acts (574) and subordinate legislation (433)
  - Repealed Acts (485) and subordinate legislation (1,408)
  - Acts as passed (6,629) and subordinate legislation as made (11,472)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Same (fleet wrapper entry point)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Check Atom feeds for updates
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import CappedRetry, request_with_deadline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.QLD-Legislation")

BASE_URL = "https://www.legislation.qld.gov.au"
BROWSE_URL = f"{BASE_URL}/browse/inforce"
PROJECTDATA_URL = f"{BASE_URL}/projectdata"
XML_URL_PATTERN = f"{BASE_URL}/view/whole/xml/{{rendition}}/{{doc_id}}"
HTML_URL_PATTERN = f"{BASE_URL}/view/whole/html/{{rendition}}/{{doc_id}}"
ATOM_FEEDS = [
    f"{BASE_URL}/feed?id=whatsnew",
    f"{BASE_URL}/feed?id=newinforce",
    f"{BASE_URL}/feed?id=newlegislation",
]

HEADERS = {
    # A browser User-Agent trips the site's Imperva JS challenge; the plain
    # crawler identity is served normally.
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "application/xml, text/xml, application/json, */*",
}

# Per-request socket timeouts, plus a wall-clock ceiling so a trickling
# response can never hold the crawl open silently (issue #1373).
TIMEOUT = (15, 120)
WALL_TIMEOUT = 300
LIST_WALL_TIMEOUT = 600

# Renditions to crawl, highest value first so a truncated run still lands
# current law. Each entry is (category, url path segment, DAL expression).
# ``{pit}`` is substituted with the portal's own server clock, which is what
# the browse UI passes to @pointInTime.
CATEGORIES = [
    ("inforce-act", "inforce/current",
     'Repealed=N AND PrintType=act.reprint AND PitValid=@pointInTime({pit})'),
    ("inforce-sl", "inforce/current",
     'Repealed=N AND PrintType=reprint AND PitValid=@pointInTime({pit})'),
    ("repealed-act", "repealed", 'Repealed=Y AND PrintType=act.reprint'),
    ("repealed-sl", "repealed", 'Repealed=Y AND PrintType=reprint'),
    ("aspassed-act", "asmade", 'Repealed=N AND PrintType=act.new'),
    ("asmade-sl", "asmade", 'Repealed=N AND PrintType=published'),
]

# Categories whose IDs can collide with an in-force/repealed reprint of the
# same instrument — the as-passed text is a distinct document, so its record
# ID carries a suffix.
SUFFIXED_CATEGORIES = {"aspassed-act", "asmade-sl"}

CHECKPOINT_FLUSH_EVERY = 25


def _dal_value(field: Any) -> str:
    """Unwrap a DAL-typed field ({"__type__":..., "__value__":...})."""
    if isinstance(field, dict):
        return str(field.get("__value__") or "")
    return str(field or "")


def _extract_text_from_xml(xml_bytes: bytes) -> tuple:
    """Extract title and full text from QuILLS XML.

    Returns (title, text, date, doc_type).
    """
    try:
        xml_str = xml_bytes.decode("utf-8", errors="replace")

        # Extract title from root element attribute (e.g. <act title="...">)
        title = ""
        title_match = re.search(r'<(?:act|sl|regulation)\s[^>]*title="([^"]+)"', xml_str[:3000])
        if title_match:
            title = title_match.group(1).strip()

        if not title:
            title_match = re.search(
                r'<heading[^>]*>\s*<txt[^>]*>(.*?)</txt>', xml_str[:5000], re.DOTALL
            )
            if title_match:
                title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()

        # Extract date from assent.date or publication.date attribute
        date = None
        date_match = re.search(r'assent\.date="(\d{4}-\d{2}-\d{2})', xml_str[:3000])
        if date_match:
            date = date_match.group(1)
        else:
            date_match = re.search(r'publication\.date="(\d{4}-\d{2}-\d{2})', xml_str[:3000])
            if date_match:
                date = date_match.group(1)

        # Determine doc type from root element
        doc_type = "act"
        root_match = re.match(r'.*?<(act|sl|regulation)\s', xml_str[:2000], re.DOTALL)
        if root_match:
            root_tag = root_match.group(1)
            if root_tag == "sl":
                doc_type = "subordinate_legislation"
            elif root_tag == "regulation":
                doc_type = "regulation"

        # Extract text: remove XML tags, comments, DOCTYPE, processing instructions
        text = re.sub(r'<!--.*?-->', ' ', xml_str, flags=re.DOTALL)
        text = re.sub(r'<\?.*?\?>', ' ', text)
        text = re.sub(r'<!DOCTYPE[^>]+>', ' ', text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return title, text, date, doc_type

    except Exception as e:
        logger.warning(f"XML parse error: {e}")
        return "", "", None, "unknown"


class QueenslandLegislationScraper(BaseScraper):
    """
    Scraper for AU/QLD-Legislation -- Queensland Legislation.
    Country: AU
    URL: https://www.legislation.qld.gov.au/

    Data types: legislation
    Auth: none (Open Data, CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        adapter = requests.adapters.HTTPAdapter(
            max_retries=CappedRetry(
                total=3,
                backoff_factor=2,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]),
            )
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._server_time: Optional[str] = None
        self.checkpoint_path = self.source_dir / "data" / "qld_checkpoint.txt"
        self._done: Set[str] = set()
        self._pending: List[str] = []

    # ── HTTP ─────────────────────────────────────────────────────────

    def _get(self, url: str, wall_timeout: int = WALL_TIMEOUT, **kwargs) -> Optional[requests.Response]:
        """GET with a hard wall-clock deadline; None on any failure."""
        try:
            resp = request_with_deadline(
                self.session, "GET", url,
                wall_timeout=wall_timeout, timeout=TIMEOUT, **kwargs,
            )
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"HTTP {resp.status_code} for {url}")
            return None
        return resp

    def _get_server_time(self) -> str:
        """The portal's own clock, needed for the @pointInTime browse filter."""
        if self._server_time:
            return self._server_time
        resp = self._get(BROWSE_URL)
        if resp is None:
            raise RuntimeError(
                f"Cannot reach {BROWSE_URL} — the portal is unreachable from this "
                "vantage (Imperva block or outage); refusing to report an empty crawl"
            )
        match = re.search(r'data-server-time="(\d+)"', resp.text)
        if not match:
            raise RuntimeError(
                "Browse page carried no data-server-time — page layout changed "
                "or an interstitial was served instead"
            )
        self._server_time = match.group(1)
        return self._server_time

    # ── Checkpoint ───────────────────────────────────────────────────

    def _load_checkpoint(self) -> None:
        """Read the set of already-yielded documents so restarts resume."""
        self._done = set()
        if self.checkpoint_path.exists():
            with self.checkpoint_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    key = line.strip()
                    if key:
                        self._done.add(key)
            logger.info(f"Checkpoint: {len(self._done)} documents already fetched")

    def _mark_done(self, key: str, force: bool = False) -> None:
        self._done.add(key)
        self._pending.append(key)
        if force or len(self._pending) >= CHECKPOINT_FLUSH_EVERY:
            self._flush_checkpoint()

    def _flush_checkpoint(self) -> None:
        if not self._pending:
            return
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        with self.checkpoint_path.open("a", encoding="utf-8") as fh:
            for key in self._pending:
                fh.write(key + "\n")
        self._pending = []

    # ── Discovery ────────────────────────────────────────────────────

    def _list_category(self, category: str, expression: str) -> List[Dict[str, str]]:
        """List every document in one rendition via the browse data API.

        ``/projectdata`` is the endpoint the browse UI itself calls. ``start``
        is 1-based; omitting the offset (or passing 0) returns the counts only.
        """
        expr = expression.format(pit=self._get_server_time())
        params = {
            "ds": "OQPC-BrowseDataSource",
            "collection": "OQPC.toc",
            "start": 1,
            "count": 1,
            "expression": expr,
        }

        resp = self._get(PROJECTDATA_URL, wall_timeout=LIST_WALL_TIMEOUT, params=params)
        if resp is None:
            raise RuntimeError(f"Browse API unreachable while listing {category}")
        try:
            total = int(json.loads(resp.text)["totalCount"]["__value__"])
        except Exception as e:
            raise RuntimeError(f"Unparseable browse API response for {category}: {e}")

        if total <= 0:
            raise RuntimeError(
                f"Browse API reported 0 documents for {category} — the expression "
                "or data source changed; failing loud rather than crawling nothing"
            )

        params["count"] = total
        resp = self._get(PROJECTDATA_URL, wall_timeout=LIST_WALL_TIMEOUT, params=params)
        if resp is None:
            raise RuntimeError(f"Browse API unreachable while paging {category}")
        payload = json.loads(resp.text)
        rows = payload.get("data") or []
        if isinstance(rows, dict):
            rows = [rows]

        docs = []
        for row in rows:
            doc_id = _dal_value(row.get("id"))
            if not doc_id:
                continue
            docs.append({
                "doc_id": doc_id,
                "title": _dal_value(row.get("title")).strip(),
                "publication_date": _dal_value(row.get("publication.date"))[:10] or None,
                "year": _dal_value(row.get("year")),
                "number": _dal_value(row.get("no")),
                "instrument_type": _dal_value(row.get("type")),
            })

        logger.info(f"{category}: {len(docs)} of {total} documents listed")
        return docs

    def _discover_ids_from_feeds(self) -> Set[str]:
        """Discover recently changed document IDs from the Atom feeds."""
        doc_ids = set()

        for feed_url in ATOM_FEEDS:
            logger.info(f"Fetching Atom feed: {feed_url}")
            resp = self._get(feed_url)
            if resp is None:
                continue
            for doc_id in re.findall(r'((?:act|sl)-\d{4}-\d{3,4})', resp.text, re.I):
                doc_ids.add(doc_id)
            time.sleep(1)

        logger.info(f"Discovered {len(doc_ids)} document IDs from Atom feeds")
        return doc_ids

    # ── Fetch ────────────────────────────────────────────────────────

    def _fetch_document(
        self, doc_id: str, rendition: str = "inforce/current",
        meta: Optional[Dict[str, str]] = None, category: str = "inforce-act",
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single document's full-text XML and return raw data."""
        url = XML_URL_PATTERN.format(rendition=rendition, doc_id=doc_id)
        resp = self._get(url)
        if resp is None:
            return None

        data = resp.content
        if len(data) < 500:
            return None
        head = data[:200].lower()
        if b'<!doctype html' in head or b'<html' in head:
            return None

        title, text, date, doc_type = _extract_text_from_xml(data)

        if not text or len(text) < 200:
            return None

        meta = meta or {}
        return {
            "doc_id": doc_id,
            "category": category,
            "title": meta.get("title") or title or doc_id,
            "text": text,
            "date": date or meta.get("publication_date"),
            "doc_type": doc_type,
            "year": meta.get("year"),
            "number": meta.get("number"),
            "url": HTML_URL_PATTERN.format(rendition=rendition, doc_id=doc_id),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        category = raw.get("category", "inforce-act")
        doc_id = raw["doc_id"]
        # As-passed / as-made texts share an instrument ID with their in-force
        # reprint but are a different document, so they get their own key.
        record_id = f"{doc_id}:asmade" if category in SUFFIXED_CATEGORIES else doc_id

        return {
            "_id": record_id,
            "_source": "AU/QLD-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", doc_id),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": doc_id,
            "doc_type": raw.get("doc_type", "act"),
            "category": category,
            "year": raw.get("year"),
            "number": raw.get("number"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield every QLD legislation document, resuming from the checkpoint."""
        self._load_checkpoint()

        try:
            for category, rendition, expression in CATEGORIES:
                docs = self._list_category(category, expression)

                for meta in docs:
                    doc_id = meta["doc_id"]
                    key = f"{category}/{doc_id}"
                    if key in self._done:
                        continue

                    doc = self._fetch_document(
                        doc_id, rendition=rendition, meta=meta, category=category,
                    )
                    # Mark either way: a document the portal will not render as
                    # XML must not stall every later restart on the same ID.
                    self._mark_done(key)
                    if doc:
                        yield doc
                    time.sleep(1)

                logger.info(f"{category}: complete")
        finally:
            self._flush_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently updated documents from the Atom feeds."""
        for doc_id in sorted(self._discover_ids_from_feeds()):
            doc = self._fetch_document(doc_id)
            if doc:
                yield doc
            time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/QLD-Legislation data fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"]
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = QueenslandLegislationScraper()

    if args.command == "test":
        logger.info("Testing XML full text access...")
        doc = scraper._fetch_document("act-1899-009")
        if doc:
            logger.info(f"OK — '{doc['title']}' ({len(doc['text'])} chars)")
        else:
            logger.error("FAILED — could not fetch act-1899-009")
            sys.exit(1)

        logger.info("Testing browse API enumeration...")
        for category, _rendition, expression in CATEGORIES:
            docs = scraper._list_category(category, expression)
            logger.info(f"OK — {category}: {len(docs)} IDs (first: {docs[0]['doc_id']})")

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
