#!/usr/bin/env python3
"""
Vietnam National Legal Document Database (VBPL) Data Fetcher

Fetches ~46,273 central-level Vietnamese legal documents from vbpl.vn,
the Ministry of Justice's official legal document database.

Endpoints:
  - Search/listing: pKetQuaTimKiem.aspx (paginated, 50 per page)
  - Metadata: pLoadAjaxVN.aspx?ItemID=X (HTML table with properties)
  - Full text: vbpq-toanvan.aspx?ItemID=X (server-rendered HTML with #toanvancontent)
"""

import html as html_mod
import json
import logging
import os
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

BASE_URL = "https://vbpl.vn"
SEARCH_URL = BASE_URL + "/VBQPPL_UserControls/Publishing/TimKiem/pKetQuaTimKiem.aspx"
METADATA_URL = BASE_URL + "/VBQPPL_UserControls/Publishing_22/pLoadAjaxVN.aspx"
FULLTEXT_URL = BASE_URL + "/TW/Pages/vbpq-toanvan.aspx"
DVID = 13  # Central government
ROWS_PER_PAGE = 50
DELAY = 1.5
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"}


def http_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch a URL and return decoded text, or None on failure."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"HTTP GET failed for {url[:120]}: {e}")
        return None


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


def parse_vn_date(date_str: str) -> Optional[str]:
    """Parse Vietnamese date format DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class VBPLFetcher:
    """Fetcher for Vietnamese legislation from vbpl.vn."""

    def __init__(self):
        self.delay = DELAY

    def search_page(self, page: int = 1) -> List[Dict[str, str]]:
        """Fetch one page of search results. Returns list of {item_id, title_hint}."""
        params = {
            "dvid": str(DVID),
            "dvid_old": str(DVID),
            "IsVietNamese": "True",
            "Page": str(page),
            "RowPerPage": str(ROWS_PER_PAGE),
        }
        url = SEARCH_URL + "?" + urllib.parse.urlencode(params)
        data = http_get(url)
        if not data:
            return []

        results = []
        seen = set()
        # Extract ItemIDs from full-text links
        for m in re.finditer(
            r'href="[^"]*vbpq-toanvan\.aspx\?ItemID=(\d+)[^"]*"[^>]*>\s*'
            r'(.*?)</a>',
            data,
            re.DOTALL | re.IGNORECASE,
        ):
            item_id = m.group(1)
            if item_id not in seen:
                seen.add(item_id)
                title_hint = strip_html(m.group(2)).strip()[:200]
                results.append({"item_id": item_id, "title_hint": title_hint})
        return results

    def get_total_count(self) -> int:
        """Get total document count from search results."""
        params = {
            "dvid": str(DVID),
            "dvid_old": str(DVID),
            "IsVietNamese": "True",
            "Page": "1",
            "RowPerPage": "1",
        }
        url = SEARCH_URL + "?" + urllib.parse.urlencode(params)
        data = http_get(url)
        if not data:
            return 0
        m = re.search(r"<b>(\d+)</b>", data)
        return int(m.group(1)) if m else 0

    def fetch_metadata(self, item_id: str) -> Dict[str, Optional[str]]:
        """Fetch structured metadata for a document."""
        url = f"{METADATA_URL}?IsVietNamese=true&ItemID={item_id}"
        data = http_get(url)
        if not data:
            return {}

        meta = {}
        # Title
        m = re.search(r'class="title"[^>]*>(.*?)</td>', data, re.DOTALL)
        if m:
            meta["title"] = strip_html(m.group(1)).strip()

        # Extract label-value pairs from the table
        pairs = re.findall(
            r'class="label"[^>]*>\s*(.*?)</td>\s*<td[^>]*>(.*?)</td>',
            data,
            re.DOTALL,
        )
        for label_raw, value_raw in pairs:
            label = strip_html(label_raw).strip().lower()
            value = strip_html(value_raw).strip()
            if not value or value == "-":
                continue

            if "ký hiệu" in label or "số" in label:
                meta["document_number"] = value
            elif "ngày ban hành" in label:
                meta["issue_date"] = parse_vn_date(value)
            elif "loại văn bản" in label:
                meta["document_type"] = value
            elif "phạm vi" in label:
                meta["scope"] = value
            elif "ngày đăng công báo" in label:
                meta["gazette_date"] = parse_vn_date(value)
            elif "ngành" in label:
                meta["sector"] = value
            elif "lĩnh vực" in label:
                meta["field"] = value
            elif "cơ quan ban hành" in label or "chức danh" in label:
                meta["issuing_authority"] = value

        # Effective date from fulltext page info div
        eff_m = re.search(r"Ngày có hiệu lực.*?(\d{2}/\d{2}/\d{4})", data)
        if eff_m:
            meta["effective_date"] = parse_vn_date(eff_m.group(1))

        # Status
        status_m = re.search(r"Hiệu lực.*?</span>\s*(.*?)</", data, re.DOTALL)
        if status_m:
            meta["status"] = strip_html(status_m.group(1)).strip()

        return meta

    def fetch_fulltext(self, item_id: str) -> Optional[str]:
        """Fetch the full text content of a document."""
        url = f"{FULLTEXT_URL}?ItemID={item_id}"
        data = http_get(url, timeout=60)
        if not data:
            return None

        # Extract from toanvancontent div
        idx = data.find('id="toanvancontent"')
        if idx == -1:
            # Fallback: look for the content div inside fulltext
            idx = data.find('class="fulltext"')
            if idx == -1:
                logger.warning(f"No content div found for ItemID={item_id}")
                return None

        # Go back to find the opening div tag
        start = data.rfind("<div", 0, idx)
        if start == -1:
            start = idx

        # Extract a large chunk and strip HTML
        # Find a reasonable end boundary - the footer area
        end_markers = [
            "File đính kèm:",
            "CƠ SỞ DỮ LIỆU",
            'class="footer"',
            'id="footer"',
            "Gửi phản hồi",
        ]
        end = len(data)
        for marker in end_markers:
            m_idx = data.find(marker, start)
            if m_idx != -1 and m_idx < end:
                end = m_idx

        chunk = data[start:end]
        text = strip_html(chunk)

        # Remove the metadata header that sometimes appears at the top
        text = re.sub(
            r"^.*?(?:Hiệu lực:.*?(?:\d{2}/\d{2}/\d{4}))\s*",
            "",
            text,
            count=1,
            flags=re.DOTALL,
        )

        return text.strip() if len(text) > 50 else None

    def fetch_document(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Fetch complete document: metadata + full text."""
        meta = self.fetch_metadata(item_id)
        time.sleep(self.delay)

        text = self.fetch_fulltext(item_id)
        time.sleep(self.delay)

        if not text:
            logger.warning(f"No full text for ItemID={item_id}")
            return None

        title = meta.get("title", "")
        issue_date = meta.get("issue_date")

        return {
            "_id": f"VN-VBPL-{item_id}",
            "_source": "VN/PhapLuatGov",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": issue_date,
            "url": f"{FULLTEXT_URL}?ItemID={item_id}",
            "document_number": meta.get("document_number"),
            "document_type": meta.get("document_type"),
            "issuing_authority": meta.get("issuing_authority"),
            "effective_date": meta.get("effective_date"),
            "gazette_date": meta.get("gazette_date"),
            "status": meta.get("status"),
            "scope": meta.get("scope"),
            "sector": meta.get("sector"),
            "field": meta.get("field"),
            "item_id": item_id,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Already normalized during fetch."""
        return raw

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all documents from vbpl.vn central database."""
        total = self.get_total_count()
        logger.info(f"Total documents: {total}")
        total_pages = (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE

        for page in range(1, total_pages + 1):
            logger.info(f"Fetching search page {page}/{total_pages}")
            results = self.search_page(page)
            if not results:
                logger.warning(f"Empty page {page}, stopping")
                break

            for item in results:
                doc = self.fetch_document(item["item_id"])
                if doc:
                    yield doc
                time.sleep(self.delay)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date (ISO format)."""
        # vbpl.vn doesn't have a direct date-filtered endpoint,
        # so we paginate from page 1 (newest first) and stop when
        # documents are older than 'since'.
        since_dt = datetime.fromisoformat(since)
        for page in range(1, 100):
            results = self.search_page(page)
            if not results:
                break
            all_old = True
            for item in results:
                doc = self.fetch_document(item["item_id"])
                if doc and doc.get("date"):
                    try:
                        doc_dt = datetime.fromisoformat(doc["date"])
                        if doc_dt >= since_dt:
                            all_old = False
                            yield doc
                    except (ValueError, TypeError):
                        yield doc
                elif doc:
                    yield doc
                time.sleep(self.delay)
            if all_old:
                break


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample documents and save to sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    fetcher = VBPLFetcher()

    total = fetcher.get_total_count()
    logger.info(f"vbpl.vn reports {total} central-level documents")

    # Fetch first page of results
    results = fetcher.search_page(1)
    logger.info(f"Got {len(results)} results from page 1")

    saved = 0
    for item in results[:count]:
        item_id = item["item_id"]
        logger.info(f"Fetching document ItemID={item_id}")

        doc = fetcher.fetch_document(item_id)
        if not doc:
            logger.warning(f"Skipping ItemID={item_id} (no content)")
            continue

        text_len = len(doc.get("text", ""))
        logger.info(f"  Title: {doc.get('title', 'N/A')[:80]}")
        logger.info(f"  Text: {text_len} chars")

        out_file = sample_dir / f"{doc['_id']}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        saved += 1
        logger.info(f"  Saved ({saved}/{count})")

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
        print("  bootstrap --sample  Fetch 15 sample documents")
        print("  bootstrap           Fetch 50 sample documents")
