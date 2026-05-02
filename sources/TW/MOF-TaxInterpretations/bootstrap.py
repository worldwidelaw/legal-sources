#!/usr/bin/env python3
"""
TW/MOF-TaxInterpretations -- Taiwan Ministry of Finance Tax Interpretations

Fetches tax interpretive rulings (釋示函令) from the MOF Tax Law
Interpretation Search System (財政部各稅法令函釋檢索系統) at ttc.mof.gov.tw.

API: POST https://ttc.mof.gov.tw/Api/GetData (list/search)
     POST https://ttc.mof.gov.tw/Api/PostData (detail)
No authentication required.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("TW.MOF-TaxInterpretations")

SOURCE_ID = "TW/MOF-TaxInterpretations"
API_LIST = "https://ttc.mof.gov.tw/Api/GetData"
API_DETAIL = "https://ttc.mof.gov.tw/Api/PostData"
REQUEST_DELAY = 1.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Origin": "https://ttc.mof.gov.tw",
    "Referer": "https://ttc.mof.gov.tw/",
}

# Tax acts to fetch (covers all 18 categories)
TAX_ACTS = [
    "稅捐稽徵法",        # Tax Collection Act
    "所得稅法",          # Income Tax Act
    "所得基本稅額條例",    # Income Basic Tax Act
    "營業稅法",          # Business Tax Act
    "貨物稅條例",        # Commodity Tax Act
    "特種貨物及勞務稅條例",# Luxury Tax Act
    "菸酒稅法",          # Tobacco & Alcohol Tax Act
    "證券交易稅條例",     # Securities Transaction Tax Act
    "期貨交易稅條例",     # Futures Transaction Tax Act
    "遺產及贈與稅法",     # Estate & Gift Tax Act
    "土地稅法",          # Land Tax Act
    "房屋稅條例",        # House Tax Act
    "契稅條例",          # Deed Tax Act
    "娛樂稅法",          # Amusement Tax Act
    "印花稅法",          # Stamp Tax Act
    "使用牌照稅法",       # License Plate Tax Act
    "關稅法",            # Customs Act
    "海關緝私條例",       # Customs Anti-Smuggling Act
]


class HTMLStripper(HTMLParser):
    """Strip HTML tags."""
    def __init__(self):
        super().__init__()
        self.result = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip = True
        elif tag in ("br", "p", "div", "li"):
            self.result.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = False

    def handle_data(self, data):
        if not self.skip:
            self.result.append(data)

    def get_text(self):
        return re.sub(r'\n{3,}', '\n\n', ''.join(self.result)).strip()


def strip_html(html: str) -> str:
    if not html:
        return ""
    stripper = HTMLStripper()
    try:
        stripper.feed(html)
        return stripper.get_text()
    except Exception:
        return re.sub(r'<[^>]+>', ' ', html).strip()


def http_post(url: str, data: dict, retries: int = 3) -> Optional[dict]:
    """POST with form-urlencoded data."""
    encoded = urllib.parse.urlencode(data, doseq=True).encode("utf-8")
    headers = {**HEADERS, "Content-Type": "application/x-www-form-urlencoded"}
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=encoded, headers=headers, method="POST")
            resp = urllib.request.urlopen(req, timeout=60)
            return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            code = getattr(e, "code", None)
            if code and code < 500:
                logger.warning(f"Client error {code} for {url}")
                return None
            logger.warning(f"Attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def get_tax_versions() -> dict:
    """Get the latest version for each tax act."""
    result = http_post(API_LIST, {"FunctionID": "FB10051"})
    if not result or result.get("Code") != "1":
        return {}
    # Parse from a search result that returns Table1 with all versions
    # Use a dummy search to get Table1
    search = http_post(API_LIST, {
        "FunctionID": "FB10001",
        "ObjParams[TaxAct]": TAX_ACTS[0],
        "ObjParams[TaxVer]": "請選擇",
        "ObjParams[Chapter]": "",
        "ObjParams[Article]": "",
        "ObjParams[Content]": "",
        "ObjParams[start]": "0",
        "ObjParams[length]": "1",
    })
    if not search:
        return {}
    versions = {}
    for item in search.get("Data", {}).get("Table1", []):
        versions[item["TaxAct"]] = item["TaxVer"]
    return versions


def search_interpretations(tax_act: str, tax_ver: str, start: int = 0, length: int = 100) -> Optional[dict]:
    """Search for interpretations under a given tax act and version."""
    return http_post(API_LIST, {
        "FunctionID": "FB10001",
        "ObjParams[TaxAct]": tax_act,
        "ObjParams[TaxVer]": tax_ver,
        "ObjParams[Chapter]": "請選擇",
        "ObjParams[Article]": "請選擇",
        "ObjParams[Content]": "",
        "ObjParams[Operator01]": "且",
        "ObjParams[Content01]": "",
        "ObjParams[start]": str(start),
        "ObjParams[length]": str(length),
    })


def get_detail(tax_sn: int) -> Optional[dict]:
    """Fetch full detail for a single interpretation."""
    result = http_post(API_DETAIL, {
        "FunctionID": "FB12001",
        "ObjParams[TaxSN]": str(tax_sn),
    })
    if result and result.get("Code") == "1":
        tables = result.get("Data", {}).get("Table", [])
        return tables[0] if tables else None
    return None


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all interpretations across all tax acts."""
    versions = get_tax_versions()
    if not versions:
        logger.error("Failed to get tax act versions")
        return

    logger.info(f"Found {len(versions)} tax acts with versions")
    total_yielded = 0
    max_per_act = 5 if sample else 999999
    max_total = 30 if sample else 999999

    for tax_act in TAX_ACTS:
        if total_yielded >= max_total:
            break

        tax_ver = versions.get(tax_act)
        if not tax_ver:
            logger.warning(f"No version found for {tax_act}, skipping")
            continue

        # Get total count first
        result = search_interpretations(tax_act, tax_ver, start=0, length=1)
        if not result or result.get("Code") != "1":
            logger.warning(f"Failed to search {tax_act}")
            continue

        items = result.get("Data", {}).get("Table", [])
        total_count = items[0].get("TotalCount", 0) if items else 0
        logger.info(f"{tax_act} ({tax_ver}): {total_count} interpretations")

        if total_count == 0:
            continue

        # Fetch in pages
        act_yielded = 0
        start = 0
        page_size = 100

        while start < total_count and act_yielded < max_per_act and total_yielded < max_total:
            result = search_interpretations(tax_act, tax_ver, start=start, length=page_size)
            if not result or result.get("Code") != "1":
                break

            items = result.get("Data", {}).get("Table", [])
            if not items:
                break

            for item in items:
                if act_yielded >= max_per_act or total_yielded >= max_total:
                    break

                tax_sn = item.get("TaxSN")
                cate = item.get("Cate", "")
                content = item.get("Content", "")

                # Skip "僅法條" (just law text) entries — they have no interpretation
                if not cate and item.get("Title") == "僅法條":
                    continue

                # If content is short/empty in list, fetch detail
                if len(str(content)) < 50 and tax_sn:
                    time.sleep(REQUEST_DELAY)
                    detail = get_detail(tax_sn)
                    if detail:
                        item = {**item, **detail}

                yield {**item, "_tax_act": tax_act, "_tax_ver": tax_ver}
                act_yielded += 1
                total_yielded += 1

            start += page_size
            time.sleep(REQUEST_DELAY)

        logger.info(f"  {tax_act}: yielded {act_yielded} records")


def normalize(raw: dict) -> Optional[dict]:
    """Normalize a raw interpretation record."""
    content = strip_html(str(raw.get("Content", "") or ""))
    article_content = strip_html(str(raw.get("ArticleContent", "") or ""))
    title = str(raw.get("Title", "") or "")
    tax_act = raw.get("_tax_act", raw.get("TaxAct", ""))
    tax_sn = raw.get("TaxSN", "")

    # Combine content with article context
    full_text = content
    if article_content:
        full_text = f"[法條 / Statutory Article]\n{article_content}\n\n[釋示函令 / Interpretation]\n{content}"

    if len(full_text.strip()) < 30:
        return None

    return {
        "_id": f"TW-MOF-TaxInterp-{tax_sn}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": None,  # Date embedded in content text (e.g., "財政部61/02/04...")
        "url": f"https://ttc.mof.gov.tw/?TaxSN={tax_sn}",
        "tax_act": tax_act,
        "tax_version": raw.get("_tax_ver", raw.get("TaxVer", "")),
        "chapter": raw.get("Chapter", ""),
        "article": raw.get("Article", ""),
        "category": raw.get("Cate", ""),
        "language": "zh-TW",
    }


def test_connectivity():
    """Quick connectivity test."""
    logger.info("Testing ttc.mof.gov.tw API...")
    result = http_post(API_LIST, {"FunctionID": "FB10051"})
    if not result or result.get("Code") != "1":
        logger.error("API test failed")
        return False
    cats = result.get("Data", {}).get("Table", [])
    logger.info(f"  Categories: {len(cats)} tax acts found - OK")

    versions = get_tax_versions()
    logger.info(f"  Versions: {len(versions)} acts with versions - OK")
    return True


def bootstrap(sample: bool = False):
    """Run bootstrap."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    total = 0
    for raw in fetch_all(sample=sample):
        record = normalize(raw)
        if not record:
            continue

        filename = f"{record['_id']}.json"
        filepath = sample_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        total += 1

    logger.info(f"\n=== Bootstrap Summary ===")
    logger.info(f"Total records saved: {total}")
    logger.info(f"Sample directory: {sample_dir}")
    return total


def main():
    parser = argparse.ArgumentParser(description="TW/MOF-TaxInterpretations bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    if args.command == "test":
        sys.exit(0 if test_connectivity() else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        logger.info(f"Bootstrap complete: {count} records")


if __name__ == "__main__":
    main()
