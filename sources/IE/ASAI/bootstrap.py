#!/usr/bin/env python3
"""
IE/ASAI -- Advertising Standards Authority for Ireland (ASAI) — Complaint Adjudications

Fetches the full text of complaint adjudications ("Decisions") issued by the
Advertising Standards Authority for Ireland (ASAI, adstandards.ie), the
independent self-regulatory body for advertising in Ireland. Each adjudication
determines whether a specific advertisement breached the ASAI Code and is
published in full = quasi-judicial case_law.

Strategy:
  1. Enumerate every adjudication via the site's WordPress REST API custom post
     type `complaint`:
         /wp-json/wp/v2/complaint?per_page=100&page=N
     Each record gives id, publish date, slug, canonical link, category title,
     and a class_list encoding the advertiser / medium / bulletin taxonomy slugs.
  2. Fetch each adjudication's canonical page and extract the born-digital full
     text from the Elementor single-post region (isolated between
     data-elementor-type="single-post" and the footer) — no OCR/PDF needed.
  3. Parse the structured meta header (Agency, Reference, Product, Advertiser,
     Influencer, Medium, Codes) and the outcome from the Conclusion.

Usage:
  python bootstrap.py bootstrap            # Full pull (all adjudications)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # Full pull (runner alias)
  python bootstrap.py update               # Incremental (recent adjudications)
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.ASAI")

BASE_URL = "https://adstandards.ie"
REST_PATH = "/wp-json/wp/v2/complaint"
PER_PAGE = 100
MAX_PAGES = 60  # safety ceiling; real corpus is ~11 pages

# Ordered meta labels rendered at the top of every adjudication page. The value
# for each label runs up to the next label; the "Codes" value terminates at the
# section-header tab list that begins with the (site-spelled) "Advertisment".
META_LABELS = ["Agency", "Reference", "Product", "Advertiser",
               "Influencer", "Medium", "Codes"]


def _flatten(fragment: str) -> str:
    """Strip tags, decode entities, collapse whitespace -> clean text."""
    text = re.sub(r"<script.*?</script>", " ", fragment, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_body(detail_html: str) -> str:
    """Isolate the Elementor single-post region (decision body) from a page."""
    start = detail_html.find('data-elementor-type="single-post"')
    if start == -1:
        return ""
    end = detail_html.find('data-elementor-type="footer"', start)
    region = detail_html[start:end] if end > start else detail_html[start:]
    # Drop the opening tag's own attributes (they precede the first '>').
    gt = region.find(">")
    if gt != -1:
        region = region[gt + 1:]
    return _flatten(region)


def _parse_meta(body: str) -> Dict[str, str]:
    """Extract the ordered meta header values from the flattened body text."""
    meta: Dict[str, str] = {}
    for i, label in enumerate(META_LABELS):
        # Terminator = the next label, or "Advertisment" after the last (Codes).
        nxt = META_LABELS[i + 1] if i + 1 < len(META_LABELS) else "Advertisment"
        m = re.search(rf"\b{re.escape(label)}:\s*(.*?)\s*\b{re.escape(nxt)}\b", body)
        meta[label] = (m.group(1).strip() if m else "")
    return meta


def _derive_outcome(body: str) -> str:
    """Best-effort outcome from the Conclusion section.

    "Conclusion" appears twice — first in the section-header tab list, then as
    the actual verdict section — so anchor on the LAST occurrence.
    """
    idx = body.rfind("Conclusion")
    tail = (body[idx:] if idx != -1 else body).lower()
    if "complaint upheld" in tail or "complaints upheld" in tail or "upheld." in tail:
        if "not upheld" in tail:
            return "Complaint partially upheld / not upheld"
        return "Complaint upheld"
    if "not upheld" in tail:
        return "Complaint not upheld"
    if "resolved" in tail:
        return "Complaint resolved"
    if "no case to answer" in tail:
        return "No case to answer"
    return ""


def _slug_value(class_list: List[str], prefix: str) -> str:
    """Human-readable value from a taxonomy class slug (e.g. complaint_medium-x)."""
    for c in class_list or []:
        if c.startswith(prefix):
            return c[len(prefix):].replace("-", " ").strip().title()
    return ""


class ASAIScraper(BaseScraper):
    """Scraper for ASAI advertising complaint adjudications."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _list_page(self, page: int) -> Optional[list]:
        params = {
            "per_page": PER_PAGE,
            "page": page,
            "_fields": "id,date,slug,link,title,class_list",
            "orderby": "date",
            "order": "desc",
        }
        try:
            self.rate_limiter.wait()
            resp = self.client.get(REST_PATH, params=params)
            if resp.status_code == 400:
                # WordPress returns 400 for pages past the last one.
                return []
            if resp.status_code != 200:
                logger.warning(f"list page {page}: HTTP {resp.status_code}")
                return None
            return resp.json()
        except Exception as e:
            logger.warning(f"Error listing page {page}: {e}")
            return None

    def _detail(self, link: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(link)
            if resp.status_code != 200:
                logger.warning(f"detail {link}: HTTP {resp.status_code}")
                return None
            return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"Error fetching detail {link}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        any_item = False
        for page in range(1, MAX_PAGES + 1):
            items = self._list_page(page)
            if items is None:
                # Transient error on a page — stop rather than silently truncate
                # if we have not yielded anything at all.
                break
            if not items:
                break
            for item in items:
                link = item.get("link")
                if not link:
                    continue
                detail_html = self._detail(link)
                if not detail_html:
                    continue
                body = _extract_body(detail_html)
                if len(body) < 120:
                    continue
                any_item = True
                yield {"item": item, "body": body}
            logger.info(f"page {page}: {len(items)} adjudications")
        if not any_item:
            raise RuntimeError(
                "ASAI complaint REST API returned 0 usable adjudications — "
                "site blocked, REST disabled, or markup changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_iso = since.strftime("%Y-%m-%d")
        for page in range(1, MAX_PAGES + 1):
            items = self._list_page(page)
            if not items:
                break
            stop = False
            for item in items:
                # Listing is date-desc; stop once we pass the cutoff.
                if (item.get("date") or "")[:10] < since_iso:
                    stop = True
                    break
                link = item.get("link")
                if not link:
                    continue
                detail_html = self._detail(link)
                if not detail_html:
                    continue
                body = _extract_body(detail_html)
                if len(body) < 120:
                    continue
                yield {"item": item, "body": body}
            if stop:
                break

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        item = raw.get("item", {})
        body = raw.get("body", "")
        if not item or len(body) < 120:
            return None

        wp_id = item.get("id")
        date_iso = (item.get("date") or "")[:10] or None
        category = _flatten(item.get("title", {}).get("rendered", "")) if isinstance(
            item.get("title"), dict) else ""
        class_list = item.get("class_list", []) or []

        meta = _parse_meta(body)
        reference = meta.get("Reference", "")
        advertiser = meta.get("Advertiser", "") or _slug_value(
            class_list, "complaint_advertiser-")
        product = meta.get("Product", "")
        medium = meta.get("Medium", "") or _slug_value(class_list, "complaint_medium-")
        codes = meta.get("Codes", "")
        influencer = meta.get("Influencer", "")
        outcome = _derive_outcome(body)

        title_bits = ["ASAI Adjudication"]
        if reference:
            title_bits.append(reference)
        if advertiser:
            title_bits.append(advertiser)
        elif category:
            title_bits.append(category)
        title = " — ".join(title_bits)

        record = {
            "_id": f"IE-ASAI-{wp_id}",
            "_source": "IE/ASAI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": body,
            "date": date_iso,
            "url": item.get("link"),
            "case_reference": reference,
            "category": category,
            "advertiser": advertiser,
            "product": product,
            "medium": medium,
            "influencer": influencer,
            "codes": codes,
            "outcome": outcome,
            "authority": "Advertising Standards Authority for Ireland",
            "jurisdiction": "IE",
            "language": "en",
        }
        return record

    def test_connection(self):
        print("Testing ASAI complaint REST API...")
        items = self._list_page(1)
        if items is None:
            print("  REST page 1 FETCH FAILED")
            return
        print(f"  REST page 1: {len(items)} adjudications")
        if not items:
            return
        item = items[0]
        print(f"  first: id={item.get('id')} date={item.get('date')} slug={item.get('slug')}")
        detail_html = self._detail(item.get("link"))
        if not detail_html:
            print("  detail FETCH FAILED")
            return
        body = _extract_body(detail_html)
        print(f"  body text: {len(body)} chars")
        rec = self.normalize({"item": item, "body": body})
        if rec:
            print(f"    title:     {rec['title']}")
            print(f"    reference: {rec['case_reference']}")
            print(f"    advertiser:{rec['advertiser']}")
            print(f"    medium:    {rec['medium']}")
            print(f"    outcome:   {rec['outcome']}")
            print(f"    text:      {len(rec['text'])} chars")


def main():
    scraper = ASAIScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
