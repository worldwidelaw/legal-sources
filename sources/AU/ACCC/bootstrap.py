#!/usr/bin/env python3
"""
AU/ACCC -- Australian Competition and Consumer Commission Fetcher

Fetches the full ACCC corpus (public register entries, media releases,
speeches, publications, guidance pages) from the site's Drupal JSON:API.

Strategy:
  - Every content type exposed at /jsonapi is enumerated.
  - The site rejects `page[offset]`/`page[limit]` ("Input value \"page\"
    contains a non-scalar value" -> HTTP 400), so the previous version
    stopped after the first 50 nodes of each type.  Pagination is therefore
    done with a KEYSET walk: sort by `drupal_internal__nid` ascending and
    filter `drupal_internal__nid > last_seen_nid` on every request.
  - Requesting the full attribute set makes several types (accc_news,
    accc_page, ...) return HTTP 503 from the render pipeline, so a sparse
    fieldset (`fields[node--TYPE]=...`) is always used.  Unknown field names
    are silently ignored by Drupal, so one union field list works for all
    types.
  - Full text lives inline in the body/summary fields; no detail fetch needed.
  - A checkpoint (data/checkpoint.json) records the last nid completed per
    type so an interrupted fleet run resumes instead of re-appending the
    first N records.

Data:
  - ~14k documents across ~15 content types, full text inline
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Full pull, concurrent normalize
  python bootstrap.py update             # Fetch recently changed nodes
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.ACCC")

BASE_URL = "https://www.accc.gov.au"
JSONAPI_BASE = f"{BASE_URL}/jsonapi"

PAGE_SIZE = 50  # server-enforced default; page[limit] is rejected

# Content types worth crawling, mapped to the document type we record.
# Types that carry no body text at all (register index rows such as
# acccgov_acquisition / acccgov_infringement_notice, whose substance sits in
# attached PDFs) are deliberately excluded.
NODE_TYPES = [
    ("acccgov_authorisation", "authorisation"),
    ("acccgov_notification", "notification"),
    ("acccgov_undertaking", "undertaking"),
    ("acccgov_informal_merger_review", "merger_review"),
    ("acccgov_merger_authorisation", "merger_authorisation"),
    ("acccgov_class_exemption", "class_exemption"),
    ("acccgov_other_public_register", "public_register"),
    ("acccgov_foi_disclosure", "foi_disclosure"),
    ("accc_news", "media_release"),
    ("acccgov_speech", "speech"),
    ("accc_publication", "publication"),
    ("accc_serial_publication", "publication"),
    ("accc_document", "document"),
    ("accc_page", "guidance"),
    ("acccgov_update", "update"),
    ("acccgov_project", "project"),
    ("acccgov_project_section", "project"),
    ("acccgov_project_stage", "project"),
]

# Union of the text-bearing field names used across ACCC content types.
# Drupal ignores field names that do not exist on a given type, so one list
# can be sent for every request.
TEXT_FIELDS = [
    "field_accc_body",
    "field_acccgov_body",
    "body",
    "field_acccgov_summary",
    "field_accc_summary",
    "field_acccgov_speech_summary",
    "field_accc_description",
    "field_acccgov_description",
]

META_FIELDS = ["drupal_internal__nid", "title", "created", "changed", "path"]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "application/vnd.api+json",
}

MIN_TEXT_CHARS = 200


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    # Drop script/style blocks entirely
    clean = re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', ' ', text)
    # Turn block boundaries into whitespace before stripping tags
    clean = re.sub(r'(?i)<(br|/p|/div|/li|/h[1-6]|/tr)[^>]*>', '\n', clean)
    clean = re.sub(r'<[^>]+>', ' ', clean)
    clean = html_module.unescape(clean)
    clean = re.sub(r'[ \t\r\f\v]+', ' ', clean)
    clean = re.sub(r'\n\s*\n\s*', '\n\n', clean)
    return clean.strip()


class AustraliaACCCScraper(BaseScraper):
    """
    Scraper for AU/ACCC -- Australian Competition and Consumer Commission.
    Country: AU
    URL: https://www.accc.gov.au/

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._ckpt_path = source_dir / "data" / "checkpoint.json"

    # ── HTTP ─────────────────────────────────────────────────────────

    def _get_json(self, url: str, tries: int = 6) -> Optional[Dict[str, Any]]:
        """GET a JSON:API URL, retrying the site's frequent 503/429 blips."""
        last_err = None
        for attempt in range(tries):
            try:
                req = Request(url, headers=HEADERS)
                with urlopen(req, timeout=90) as resp:
                    return json.loads(resp.read())
            except HTTPError as e:
                last_err = e
                if e.code in (429, 500, 502, 503, 504):
                    time.sleep(min(60, 3 * (attempt + 1)))
                    continue
                logger.warning(f"HTTP {e.code} for {url}")
                return None
            except (URLError, TimeoutError, json.JSONDecodeError) as e:
                last_err = e
                time.sleep(min(60, 3 * (attempt + 1)))
        logger.warning(f"Giving up on {url}: {last_err}")
        return None

    # ── Checkpoint ───────────────────────────────────────────────────

    def _load_checkpoint(self) -> Dict[str, Any]:
        try:
            return json.loads(self._ckpt_path.read_text())
        except Exception:
            return {}

    def _save_checkpoint(self, ckpt: Dict[str, Any]) -> None:
        try:
            self._ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            self._ckpt_path.write_text(json.dumps(ckpt))
        except Exception as e:  # never let checkpointing break a crawl
            logger.debug(f"Checkpoint write failed: {e}")

    # ── Enumeration ──────────────────────────────────────────────────

    @staticmethod
    def _list_url(node_type: str, after_nid: int, since: Optional[str] = None) -> str:
        fields = ",".join(META_FIELDS + TEXT_FIELDS)
        url = (
            f"{JSONAPI_BASE}/node/{node_type}"
            f"?sort=drupal_internal__nid"
            f"&fields%5Bnode--{node_type}%5D={quote(fields, safe=',')}"
            f"&filter%5Bks%5D%5Bcondition%5D%5Bpath%5D=drupal_internal__nid"
            f"&filter%5Bks%5D%5Bcondition%5D%5Boperator%5D=%3E"
            f"&filter%5Bks%5D%5Bcondition%5D%5Bvalue%5D={after_nid}"
        )
        if since:
            url += (
                "&filter%5Bch%5D%5Bcondition%5D%5Bpath%5D=changed"
                "&filter%5Bch%5D%5Bcondition%5D%5Boperator%5D=%3E%3D"
                f"&filter%5Bch%5D%5Bcondition%5D%5Bvalue%5D={quote(since)}"
            )
        return url

    @staticmethod
    def _extract_text(attrs: Dict[str, Any]) -> str:
        """Concatenate every text-bearing field present on the node."""
        parts: List[str] = []
        for field in TEXT_FIELDS:
            value = attrs.get(field)
            raw = ""
            if isinstance(value, dict):
                raw = value.get("processed") or value.get("value") or ""
            elif isinstance(value, str):
                raw = value
            text = strip_html(raw)
            if text and text not in parts:
                parts.append(text)
        return "\n\n".join(parts)

    def _iter_type(
        self,
        node_type: str,
        doc_type: str,
        after_nid: int = 0,
        since: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Keyset-walk every node of one content type."""
        last_nid = after_nid
        fetched = 0
        while True:
            payload = self._get_json(self._list_url(node_type, last_nid, since))
            if payload is None:
                logger.warning(f"{node_type}: aborting after nid={last_nid}")
                return
            rows = payload.get("data", [])
            if not rows:
                break

            for node in rows:
                attrs = node.get("attributes", {})
                nid = attrs.get("drupal_internal__nid")
                if isinstance(nid, int):
                    last_nid = max(last_nid, nid)

                text = self._extract_text(attrs)
                if len(text) < MIN_TEXT_CHARS:
                    continue

                path_alias = ""
                path_data = attrs.get("path")
                if isinstance(path_data, dict):
                    path_alias = path_data.get("alias") or ""

                changed = attrs.get("changed")
                if isinstance(changed, dict):
                    changed = changed.get("value", "")

                fetched += 1
                yield {
                    "uuid": node.get("id", ""),
                    "nid": nid,
                    "title": attrs.get("title", ""),
                    "text": text,
                    "created": attrs.get("created", ""),
                    "changed": changed or "",
                    "path_alias": path_alias,
                    "doc_type": doc_type,
                    "node_type": node_type,
                }

            if len(rows) < PAGE_SIZE:
                break
            time.sleep(0.3)

        logger.info(f"{node_type}: {fetched} documents with full text")

    # ── Public API ───────────────────────────────────────────────────

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601 (date only)."""
        if not date_str:
            return None
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_str)
        if match:
            return match.group(0)
        try:
            return datetime.strptime(date_str.strip(), "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        path_alias = raw.get("path_alias", "")
        url = f"{BASE_URL}{path_alias}" if path_alias else BASE_URL

        return {
            "_id": raw.get("uuid", ""),
            "_source": "AU/ACCC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": self._parse_date(raw.get("created", "")),
            "url": url,
            "doc_type": raw.get("doc_type", "unknown"),
            "uuid": raw.get("uuid", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield every ACCC document, resuming from the checkpoint if present."""
        ckpt = self._load_checkpoint()
        done = set(ckpt.get("completed_types", []))
        progress = ckpt.get("last_nid", {})

        for node_type, doc_type in NODE_TYPES:
            if node_type in done:
                logger.info(f"{node_type}: already complete (checkpoint), skipping")
                continue
            start = int(progress.get(node_type, 0))
            if start:
                logger.info(f"{node_type}: resuming after nid={start}")
            for record in self._iter_type(node_type, doc_type, after_nid=start):
                progress[node_type] = record["nid"] or progress.get(node_type, 0)
                yield record
            done.add(node_type)
            self._save_checkpoint(
                {"completed_types": sorted(done), "last_nid": progress}
            )
            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Yield nodes whose `changed` timestamp is at or after `since`."""
        stamp = since.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        logger.info(f"Fetching ACCC nodes changed since {stamp}")
        for node_type, doc_type in NODE_TYPES:
            yield from self._iter_type(node_type, doc_type, since=stamp)
            time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/ACCC data fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"]
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()

    scraper = AustraliaACCCScraper()

    if args.command == "test":
        logger.info("Testing JSON:API keyset pagination...")
        url = scraper._list_url("acccgov_authorisation", 0)
        payload = scraper._get_json(url)
        if not payload or not payload.get("data"):
            logger.error("FAILED — could not reach JSON:API")
            sys.exit(1)
        logger.info(f"OK — {len(payload['data'])} authorisation node(s)")

        url = scraper._list_url("accc_news", 0)
        payload = scraper._get_json(url)
        if not payload or not payload.get("data"):
            logger.error("FAILED — could not reach media releases")
            sys.exit(1)
        sizes = [
            len(scraper._extract_text(n.get("attributes", {})))
            for n in payload["data"]
        ]
        logger.info(f"OK — {len(sizes)} media releases, max text {max(sizes)} chars")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast(max_workers=args.workers)
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
