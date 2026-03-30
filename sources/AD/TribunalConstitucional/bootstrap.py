#!/usr/bin/env python3
"""
Andorra Constitutional Court (Tribunal Constitucional) Data Fetcher

Fetches 2,273 decisions (541 sentencies + 1,732 autes) from the
Andorra Constitutional Court via Drupal JSON:API.

Endpoints:
  - /jsonapi/node/sentencia  (judgments)
  - /jsonapi/node/aute       (orders/interlocutory decisions)
"""

import html as html_mod
import json
import logging
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.tribunalconstitucional.ad"
JSONAPI_SENTENCIA = BASE_URL + "/jsonapi/node/sentencia"
JSONAPI_AUTE = BASE_URL + "/jsonapi/node/aute"
PAGE_LIMIT = 50
DELAY = 1.0
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0",
    "Accept": "application/vnd.api+json",
}


def strip_html(text: str) -> str:
    """Remove HTML tags and clean up whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|tr|li|h[1-6])>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


def jsonapi_fetch(url: str) -> Optional[Dict]:
    """Fetch a JSON:API endpoint."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(f"JSON:API fetch failed: {url[:100]} - {e}")
        return None


class TribunalConstitucionalFetcher:
    """Fetcher for Andorra Constitutional Court decisions."""

    def __init__(self):
        self.delay = DELAY

    def _parse_node(self, node: Dict, node_type: str) -> Optional[Dict[str, Any]]:
        """Parse a JSON:API node into a normalized record."""
        attrs = node.get("attributes", {})
        drupal_id = node.get("id", "")
        title = attrs.get("title", "")

        # Full text from field_contingut
        contingut = attrs.get("field_contingut") or {}
        raw_html = contingut.get("value", "") if isinstance(contingut, dict) else ""
        if not raw_html:
            return None

        text = strip_html(raw_html)
        if len(text) < 50:
            return None

        # Date
        date_field = attrs.get("field_data")
        date = None
        if date_field and isinstance(date_field, list) and date_field:
            date = date_field[0][:10]  # YYYY-MM-DD
        elif isinstance(date_field, str):
            date = date_field[:10]

        # Summary
        resum = attrs.get("field_resum_contingut") or {}
        summary = ""
        if isinstance(resum, dict):
            summary = strip_html(resum.get("value", ""))

        # BOPA reference
        bopa = attrs.get("field_bopa") or {}
        bopa_ref = ""
        if isinstance(bopa, dict):
            bopa_ref = strip_html(bopa.get("value", ""))

        # URL
        path = attrs.get("path", {})
        alias = path.get("alias", "") if isinstance(path, dict) else ""
        doc_url = f"{BASE_URL}{alias}" if alias else f"{BASE_URL}/{node_type}/{title}"

        decision_type = "sentencia" if node_type == "sentencia" else "aute"

        return {
            "_id": f"AD-TC-{decision_type}-{title}",
            "_source": "AD/TribunalConstitucional",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": doc_url,
            "case_number": title,
            "decision_type": decision_type,
            "summary": summary,
            "bopa_reference": bopa_ref,
            "drupal_uuid": drupal_id,
        }

    def _fetch_node_type(self, endpoint: str, node_type: str) -> Iterator[Dict[str, Any]]:
        """Paginate through a JSON:API node type."""
        offset = 0
        while True:
            url = f"{endpoint}?page[limit]={PAGE_LIMIT}&page[offset]={offset}&sort=-field_data"
            logger.info(f"Fetching {node_type} offset={offset}")

            data = jsonapi_fetch(url)
            if not data:
                break

            items = data.get("data", [])
            if not items:
                break

            for node in items:
                doc = self._parse_node(node, node_type)
                if doc:
                    yield doc

            offset += PAGE_LIMIT
            time.sleep(self.delay)

            # Check if there's a next page
            links = data.get("links", {})
            if "next" not in links:
                break

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all decisions (sentencies + autes)."""
        logger.info("Fetching sentencies (judgments)...")
        yield from self._fetch_node_type(JSONAPI_SENTENCIA, "sentencia")

        logger.info("Fetching autes (orders)...")
        yield from self._fetch_node_type(JSONAPI_AUTE, "aute")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch decisions since a date."""
        since_dt = datetime.fromisoformat(since)
        for doc in self.fetch_all():
            if doc.get("date"):
                try:
                    doc_dt = datetime.fromisoformat(doc["date"])
                    if doc_dt >= since_dt:
                        yield doc
                    else:
                        # Sorted by date desc, so stop when we're past
                        break
                except (ValueError, TypeError):
                    yield doc
            else:
                yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Already normalized during fetch."""
        return raw


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample decisions."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    fetcher = TribunalConstitucionalFetcher()

    saved = 0
    # Fetch some sentencies and some autes
    for node_type, endpoint in [("sentencia", JSONAPI_SENTENCIA), ("aute", JSONAPI_AUTE)]:
        target = count // 2 if node_type == "sentencia" else count - saved
        type_saved = 0

        url = f"{endpoint}?page[limit]={target + 5}&page[offset]=0&sort=-field_data"
        data = jsonapi_fetch(url)
        if not data:
            continue

        for node in data.get("data", []):
            if type_saved >= target:
                break

            doc = fetcher._parse_node(node, node_type)
            if not doc:
                continue

            text_len = len(doc.get("text", ""))
            logger.info(f"  {doc['decision_type']}: {doc['title']} - {text_len} chars")

            out_file = sample_dir / f"{doc['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

            saved += 1
            type_saved += 1

        logger.info(f"Saved {type_saved} {node_type} documents")
        time.sleep(fetcher.delay)

    logger.info(f"Bootstrap complete: {saved} documents saved to {sample_dir}")
    return saved


if __name__ == "__main__":
    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_flag = "--sample" in sys.argv
        count = 15 if sample_flag else 50
        saved = bootstrap_sample(sample_dir, count)
        if saved < 10:
            logger.error(f"Only {saved} documents saved, expected at least 10")
            sys.exit(1)
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")
