#!/usr/bin/env python3
"""
CN/STA-TaxPolicy -- State Taxation Administration Tax Policy Database

Fetches Chinese tax policy documents (laws, regulations, normative documents,
finance/tax documents, work notices) from fgk.chinatax.gov.cn.

Strategy:
  - POST to /getFileListByCodeId JSON API to get paginated document listings
  - Fetch individual content.html pages for full text
  - Extract text from <div class="arc_cont"> with <p> tags
  - Rich metadata from domainMetaList (doc number, tax type, effectiveness, etc.)

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CN/STA-TaxPolicy"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.STA-TaxPolicy")

LIST_API_URL = "https://www.chinatax.gov.cn/getFileListByCodeId"
CONTENT_BASE = "https://fgk.chinatax.gov.cn"

# Channel ID for all tax policy documents (~4977 total)
ALL_CHANNEL_ID = "29a88b67e4b149cfa9fac7919dfb08a5"

PAGE_SIZE = 100
DELAY = 2.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

_session = None


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _handle_cookie_challenge(resp):
    """Parse and set anti-bot cookie from JS challenge response."""
    if len(resp.text) < 1000:
        cookie_m = re.search(r'cookie="([^"]+)"', resp.text)
        if cookie_m:
            cookie_str = cookie_m.group(1)
            parts = cookie_str.split(";")
            nv = parts[0].split("=", 1)
            if len(nv) == 2:
                _get_session().cookies.set(
                    nv[0].strip(), nv[1].strip(),
                    domain="fgk.chinatax.gov.cn", path="/",
                )
                return True
    return False


def _get_with_retry(url, retries=3, backoff=5):
    """GET with cookie challenge handling and retries."""
    session = _get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = "utf-8"

            if _handle_cookie_challenge(resp):
                time.sleep(1)
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                resp.encoding = "utf-8"

            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries} for {url}: {e} (wait {wait}s)")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.HTTPError:
            if resp.status_code in (429, 500, 502, 503):
                if attempt < retries - 1:
                    wait = backoff * (attempt + 1)
                    logger.warning(f"HTTP {resp.status_code}, retry {attempt+1}/{retries} (wait {wait}s)")
                    time.sleep(wait)
                else:
                    raise
            else:
                raise


def _post_with_retry(url, data, retries=3, backoff=5):
    """POST with retries."""
    session = _get_session()
    for attempt in range(retries):
        try:
            resp = session.post(url, data=data, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{retries} for POST {url}: {e} (wait {wait}s)")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.HTTPError:
            if resp.status_code in (429, 500, 502, 503):
                if attempt < retries - 1:
                    wait = backoff * (attempt + 1)
                    logger.warning(f"HTTP {resp.status_code}, retry {attempt+1}/{retries} (wait {wait}s)")
                    time.sleep(wait)
                else:
                    raise
            else:
                raise


def _extract_meta(result):
    """Extract metadata dict from domainMetaList."""
    meta = {}
    for dm in result.get("domainMetaList", []):
        for rl in dm.get("resultList", []):
            key = rl.get("key", "")
            value = rl.get("value", "")
            if key and value:
                meta[key] = value
    return meta


def _clean_html(text):
    """Strip HTML tags and clean text."""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"&[a-z]+;", " ", text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines)


SKIP_PREFIXES = ("分享", "享到", "Share", "Home>", "首页", "当前位置", "【打印", "【关闭")


def _extract_full_text(html_text):
    """Extract full text from a content.html page."""
    # Find arc_cont div (Chinese content pages)
    idx = html_text.find('class="arc_cont"')
    if idx < 0:
        # Fallback: try zscont (English pages)
        idx = html_text.find('class="zscont"')
    if idx < 0:
        return ""

    content_region = html_text[idx:]

    # Extract <p> tags
    paras = re.findall(r"<p[^>]*>(.*?)</p>", content_region, re.DOTALL)
    parts = []
    for p in paras:
        clean = _clean_html(p)
        if not clean or len(clean) < 2:
            continue
        if any(clean.startswith(prefix) for prefix in SKIP_PREFIXES):
            break
        parts.append(clean)

    return "\n".join(parts)


def _extract_doc_id(url_str):
    """Extract article ID from URL like /zcfgk/c100012/c5249181/content.html."""
    parts = url_str.rstrip("/").split("/")
    for part in reversed(parts):
        if part.startswith("c") and part[1:].isdigit():
            return part
    return hashlib.md5(url_str.encode()).hexdigest()[:12]


def _make_content_url(raw_url):
    """Convert listing API URL to canonical fgk.chinatax.gov.cn URL."""
    # URLs come as http://www.chinatax.gov.cn/zcfgk/... but content is at fgk.chinatax.gov.cn
    path = raw_url
    for prefix in ("http://www.chinatax.gov.cn", "https://www.chinatax.gov.cn"):
        if path.startswith(prefix):
            path = path[len(prefix):]
            break
    return f"{CONTENT_BASE}{path}"


def fetch_listing(page=1, size=PAGE_SIZE):
    """Fetch a page of document listings from the API."""
    data = {
        "channelId": ALL_CHANNEL_ID,
        "page": str(page),
        "size": str(size),
        "codeId": "",
    }
    result = _post_with_retry(LIST_API_URL, data)
    if result.get("code") != 200:
        raise RuntimeError(f"API error: {result}")
    return result["results"]["data"]


def normalize(raw):
    """Transform raw document data into standard schema."""
    return {
        "_id": raw["doc_id"],
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw["text"],
        "date": raw["date"],
        "url": raw["url"],
        "doc_number": raw.get("doc_number", ""),
        "issuing_department": raw.get("issuing_department", ""),
        "tax_type": raw.get("tax_type", ""),
        "effectiveness": raw.get("effectiveness", ""),
        "effect_level": raw.get("effect_level", ""),
        "channel_name": raw.get("channel_name", ""),
        "language": "zh",
    }


def fetch_all(sample=False):
    """Yield all documents with full text."""
    # Get first page to determine total
    listing = fetch_listing(page=1)
    total = listing["total"]
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    logger.info(f"Total documents: {total}, pages: {total_pages}")

    if sample:
        total_pages = 1

    all_results = listing["results"]

    # Fetch remaining pages
    for page in range(2, total_pages + 1):
        time.sleep(DELAY)
        listing = fetch_listing(page=page)
        all_results.extend(listing["results"])
        if page % 10 == 0:
            logger.info(f"Listed page {page}/{total_pages} ({len(all_results)} docs)")

    logger.info(f"Total listed: {len(all_results)} documents")

    if sample:
        all_results = all_results[:15]

    for i, result in enumerate(all_results):
        title = _clean_html(result.get("titleHtml", "") or result.get("subTitleHtml", ""))
        raw_url = result.get("url", "")
        if not raw_url:
            continue

        doc_id = _extract_doc_id(raw_url)
        content_url = _make_content_url(raw_url)
        meta = _extract_meta(result)

        # Parse date
        written_date = meta.get("writtendate", "")
        pub_date = result.get("publishedTimeStr", "")
        date_str = written_date or pub_date
        iso_date = None
        if date_str:
            try:
                iso_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d").strftime("%Y-%m-%d")
            except (ValueError, IndexError):
                iso_date = date_str

        logger.info(f"[{i+1}/{len(all_results)}] Fetching: {title[:60]}...")

        try:
            resp = _get_with_retry(content_url)
            text = _extract_full_text(resp.text)
            time.sleep(DELAY)
        except Exception as e:
            logger.error(f"Failed to fetch {content_url}: {e}")
            continue

        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
            continue

        raw = {
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": iso_date,
            "url": content_url,
            "doc_number": meta.get("writtentext", ""),
            "issuing_department": meta.get("writtendepartment", ""),
            "tax_type": meta.get("taxpolicy", ""),
            "effectiveness": meta.get("aging", ""),
            "effect_level": meta.get("effectlevel", ""),
            "channel_name": result.get("channelName", ""),
        }

        yield normalize(raw)


def bootstrap(sample=False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(
            f"Saved {record['_id']}: {record['title'][:50]}... "
            f"({len(record['text'])} chars)"
        )

    logger.info(f"Bootstrap complete: {count} records saved to {SAMPLE_DIR}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CN/STA-TaxPolicy bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 docs)")
    parser.add_argument("--full", action="store_true", help="Fetch all documents")
    args = parser.parse_args()

    if args.command == "test-api":
        logger.info("Testing listing API...")
        try:
            data = fetch_listing(page=1, size=3)
            logger.info(f"API OK: {data['total']} total documents, got {len(data['results'])} results")
            for r in data["results"]:
                print(f"  - {_clean_html(r.get('titleHtml', ''))} ({r.get('publishedTimeStr', '')})")
        except Exception as e:
            logger.error(f"API test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample = args.sample and not args.full
        count = bootstrap(sample=sample)
        if count < 10 and sample:
            logger.error(f"Only {count} records — expected at least 10")
            sys.exit(1)
        logger.info(f"Success: {count} records")
