#!/usr/bin/env python3
"""
CN/MEE -- China Ministry of Ecology and Environment (生态环境部)

Fetches environmental laws, departmental regulations, standards, and
policy documents from mee.gov.cn.

Strategy:
  Three data channels combined:
  1. Laws (fl) + Admin Regulations (xzfg) — static listing pages, ~90 items
     Full text from detail page HTML (Custom_UnionStyle / TRS_Editor divs)
  2. Departmental Rules (gzk) — WAS5 search API, 87 items
     Full text from detail page HTML + DOCX download fallback
  3. Regulations & Standards (chnls=5) — WAS5 site-wide search, ~4000 items
     Summary from detail pages + PDF full text extraction

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test-api
"""

import argparse
import html as html_module
import io
import json
import logging
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

try:
    from common.pdf_extract import extract_pdf_markdown
    HAS_PDF_EXTRACT = True
except ImportError:
    HAS_PDF_EXTRACT = False

SOURCE_ID = "CN/MEE"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.MEE")

BASE_URL = "https://www.mee.gov.cn"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# Static listing pages for laws and admin regulations
LISTING_SECTIONS = [
    {"path": "/ywgz/fgbz/fl/", "category": "法律", "data_type": "legislation"},
    {"path": "/ywgz/fgbz/xzfg/", "category": "行政法规", "data_type": "legislation"},
]

# WAS5 gzk endpoint for departmental rules
GZK_URL = BASE_URL + "/was5/web/search"
GZK_CHANNEL = "295523"

# WAS5 site-wide search for regulations & standards
SITE_SEARCH_URL = BASE_URL + "/was5/web/search"
SITE_SEARCH_CHANNEL = "270514"

session = requests.Session()
session.headers.update(HEADERS)


def _request_with_retry(url, retries=3, backoff=3, **kwargs):
    """GET request with retry logic."""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30, **kwargs)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                logger.warning("Retry %d/%d for %s: %s (wait %ds)",
                               attempt + 1, retries, url, e, wait)
                time.sleep(wait)
            else:
                logger.error("Failed after %d retries: %s — %s", retries, url, e)
                raise


def strip_html(text: str) -> str:
    """Remove HTML tags and clean whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_module.unescape(text)
    text = text.replace("\xa0", " ").replace("&nbsp;", " ")
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(line for line in lines if line)
    return text.strip()


def extract_text_from_docx(docx_bytes: bytes) -> str:
    """Extract text from DOCX bytes using stdlib only."""
    try:
        with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
            xml_content = zf.read("word/document.xml")
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        root = ET.fromstring(xml_content)
        paragraphs = []
        for p in root.iter(f"{{{ns['w']}}}p"):
            texts = [t.text for t in p.iter(f"{{{ns['w']}}}t") if t.text]
            line = "".join(texts).strip()
            if line:
                paragraphs.append(line)
        return "\n".join(paragraphs)
    except Exception as e:
        logger.warning("DOCX extraction failed: %s", e)
        return ""


def extract_full_text(html_content: str) -> str:
    """Extract full text from an MEE detail page HTML."""
    # Try multiple content container patterns (order: most specific first)
    for pattern in [
        r'class="Custom_UnionStyle">(.*?)(?:</div>\s*</div>\s*</div>)',
        r'class="TRS_Editor"[^>]*>(.*?)(?:</div>\s*</div>)',
        r'class="gz_content_txt">(.*?)(?:</div>\s*</div>)',
    ]:
        m = re.search(pattern, html_content, re.DOTALL)
        if m:
            raw = m.group(1)
            text = strip_html(raw)
            if len(text) > 100:
                return text

    # Fallback: try the stbzXq div for standards (usually just summary)
    m = re.search(r'class="stbzXq">(.*?)(?:相关阅读推荐|</div>\s*<div)', html_content, re.DOTALL)
    if m:
        text = strip_html(m.group(1))
        if len(text) > 50:
            return text

    return ""


def parse_date(date_str: str) -> str:
    """Parse various date formats to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ["%Y-%m-%d", "%Y年%m月%d日", "%Y.%m.%d", "%Y/%m/%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try to extract YYYY-MM-DD from string
    m = re.search(r"(\d{4})[-年./](\d{1,2})[-月./](\d{1,2})", date_str)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None


def normalize(raw: dict) -> dict:
    """Normalize a raw record to standard schema."""
    return {
        "_id": raw.get("id", ""),
        "_source": SOURCE_ID,
        "_type": raw.get("data_type", "legislation"),
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "category": raw.get("category", ""),
        "standard_number": raw.get("standard_number"),
        "enactment_info": raw.get("enactment_info"),
    }


# ── Channel 1: Laws & Admin Regulations (static listing pages) ──

def _scrape_listing_pages(section: dict, max_pages: int = 50) -> Generator[dict, None, None]:
    """Scrape a static listing section for document links."""
    base_path = section["path"]
    category = section["category"]
    data_type = section["data_type"]

    for page_num in range(max_pages):
        if page_num == 0:
            page_url = BASE_URL + base_path + "index.shtml"
        else:
            page_url = BASE_URL + base_path + f"index_{page_num}.shtml"

        try:
            resp = _request_with_retry(page_url)
        except requests.RequestException:
            break

        html = resp.text
        # Extract document links
        links = re.findall(
            r'<a[^>]+href="(\./[^"]+\.shtml)"[^>]*>([^<]+)</a>',
            html
        )
        if not links:
            break

        for rel_url, title in links:
            abs_url = urljoin(page_url, rel_url)
            title = title.strip()
            if not title or len(title) < 2:
                continue
            yield {
                "url": abs_url,
                "title": title,
                "category": category,
                "data_type": data_type,
            }
            time.sleep(0.3)

        # Check if there's a next page
        next_page = f'index_{page_num + 1}.shtml'
        if next_page not in html:
            break

        time.sleep(0.5)


def _fetch_listing_detail(item: dict) -> dict:
    """Fetch full text from a listing detail page."""
    url = item["url"]
    try:
        resp = _request_with_retry(url)
        text = extract_full_text(resp.text)

        # Extract date from page content
        date = None
        date_patterns = [
            r'(\d{4})年(\d{1,2})月(\d{1,2})日',
            r'发布日期[：:]\s*(\d{4}-\d{2}-\d{2})',
        ]
        for pat in date_patterns:
            m = re.search(pat, resp.text)
            if m:
                if len(m.groups()) == 3:
                    date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                else:
                    date = m.group(1)
                break

        # Extract date from URL pattern tYYYYMMDD
        if not date:
            m = re.search(r'/t(\d{4})(\d{2})(\d{2})_', url)
            if m:
                date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        doc_id = re.search(r't\d+_(\d+)', url)
        doc_id = doc_id.group(1) if doc_id else url.split("/")[-1].replace(".shtml", "")

        return {
            "id": f"MEE-{item['category']}-{doc_id}",
            "title": item["title"],
            "text": text,
            "date": date,
            "url": url,
            "category": item["category"],
            "data_type": item["data_type"],
        }
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return None


# ── Channel 2: Departmental Rules (gzk WAS5 search) ──

def _fetch_gzk_regulations(max_pages: int = 10) -> Generator[dict, None, None]:
    """Fetch departmental rules from the gzk WAS5 search endpoint."""
    for page in range(1, max_pages + 1):
        params = {"channelid": GZK_CHANNEL, "page": str(page)}
        try:
            resp = _request_with_retry(GZK_URL, params=params)
        except requests.RequestException:
            break

        html = resp.text

        # Extract total pages
        total_match = re.search(r'id="countPage"[^>]*value="(\d+)"', html)
        total_pages = int(total_match.group(1)) if total_match else 1

        # Parse regulation entries
        entries = re.findall(
            r'<div class="title"><a href="([^"]+)"[^>]*>([^<]+)</a>'
            r'<p>([^<]*)</p></div>.*?'
            r'(?:href="([^"]*\.(?:doc|docx))")?.*?'
            r'(?:href="([^"]*\.pdf)")?',
            html, re.DOTALL
        )

        if not entries:
            # Try simpler pattern
            entries = re.finditer(
                r'<div class="title">\s*<a href="([^"]+)"[^>]*>([^<]+)</a>\s*<p>([^<]*)</p>',
                html
            )
            for m in entries:
                detail_url = m.group(1).strip()
                title = m.group(2).strip()
                enactment = m.group(3).strip()

                if detail_url.startswith("http://"):
                    detail_url = detail_url.replace("http://", "https://")

                yield {
                    "url": detail_url,
                    "title": title,
                    "enactment_info": enactment,
                    "category": "部门规章",
                    "data_type": "legislation",
                }
                time.sleep(0.3)
        else:
            for entry in entries:
                detail_url = entry[0].strip()
                title = entry[1].strip()
                enactment = entry[2].strip()

                if detail_url.startswith("http://"):
                    detail_url = detail_url.replace("http://", "https://")

                yield {
                    "url": detail_url,
                    "title": title,
                    "enactment_info": enactment,
                    "docx_url": entry[3] if len(entry) > 3 else None,
                    "pdf_url": entry[4] if len(entry) > 4 else None,
                    "category": "部门规章",
                    "data_type": "legislation",
                }
                time.sleep(0.3)

        if page >= total_pages:
            break
        time.sleep(0.5)


def _fetch_gzk_detail(item: dict) -> dict:
    """Fetch full text for a gzk regulation."""
    url = item["url"]
    try:
        resp = _request_with_retry(url)
        text = extract_full_text(resp.text)

        # If HTML text is too short, try DOCX download
        if len(text) < 200 and item.get("docx_url"):
            docx_url = item["docx_url"]
            if docx_url.startswith("http://"):
                docx_url = docx_url.replace("http://", "https://")
            try:
                docx_resp = _request_with_retry(docx_url)
                docx_text = extract_text_from_docx(docx_resp.content)
                if len(docx_text) > len(text):
                    text = docx_text
            except Exception as e:
                logger.warning("DOCX download failed for %s: %s", docx_url, e)

        date = parse_date(item.get("enactment_info", ""))
        if not date:
            m = re.search(r'/t(\d{4})(\d{2})(\d{2})_', url)
            if m:
                date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        doc_id = re.search(r't\d+_(\d+)', url)
        doc_id = doc_id.group(1) if doc_id else url.split("/")[-1].replace(".shtml", "")

        return {
            "id": f"MEE-GZK-{doc_id}",
            "title": item["title"],
            "text": text,
            "date": date,
            "url": url,
            "category": item["category"],
            "data_type": item["data_type"],
            "enactment_info": item.get("enactment_info"),
        }
    except Exception as e:
        logger.warning("Failed to fetch gzk detail %s: %s", url, e)
        return None


# ── Channel 3: Standards & Regulations via site-wide search ──

def _fetch_site_search(chnls: str = "5", max_pages: int = 500) -> Generator[dict, None, None]:
    """Fetch documents from WAS5 site-wide search."""
    for page in range(1, max_pages + 1):
        params = {
            "channelid": SITE_SEARCH_CHANNEL,
            "chnls": chnls,
            "page": str(page),
            "orderby": "-docreltime",
            "perpage": "10",
        }
        try:
            resp = _request_with_retry(SITE_SEARCH_URL, params=params)
        except requests.RequestException:
            break

        html = resp.text

        total_match = re.search(r'id="countPage"[^>]*value="(\d+)"', html)
        total_pages = int(total_match.group(1)) if total_match else 1

        # Parse search result entries
        items = re.finditer(
            r'<li class="li">\s*<a href="([^"]+)"[^>]*>\s*'
            r'<h2 class="h2"><em[^>]*>[^<]*</em>([^<]+)</h2>\s*'
            r'</a>\s*'
            r'<span class="span">\s*([^<\n]+)',
            html, re.DOTALL
        )

        found = False
        for m in items:
            found = True
            url = m.group(1).strip()
            title = m.group(2).strip()
            date_str = m.group(3).strip()

            if url.startswith("http://"):
                url = url.replace("http://", "https://")

            # Determine data type from URL path
            data_type = "legislation"
            if "/bz/" in url or "标准" in title:
                data_type = "doctrine"

            yield {
                "url": url,
                "title": title,
                "date_str": date_str,
                "category": "法规标准",
                "data_type": data_type,
            }
            time.sleep(0.3)

        if not found or page >= total_pages:
            break
        time.sleep(0.5)


def _fetch_search_detail(item: dict) -> dict:
    """Fetch full text from a site-wide search result detail page."""
    url = item["url"]
    try:
        resp = _request_with_retry(url)
        text = extract_full_text(resp.text)

        # Try PDF extraction if text is too short
        if len(text) < 200 and HAS_PDF_EXTRACT:
            pdf_links = re.findall(r'href="([^"]*\.pdf)"', resp.text)
            for pdf_link in pdf_links[:1]:
                pdf_url = urljoin(url, pdf_link)
                try:
                    pdf_resp = _request_with_retry(pdf_url)
                    pdf_text = extract_pdf_markdown(
                        SOURCE_ID, SOURCE_ID, pdf_bytes=pdf_resp.content
                    )
                    if pdf_text and len(pdf_text) > len(text):
                        text = pdf_text
                        break
                except Exception as e:
                    logger.warning("PDF extraction failed for %s: %s", pdf_url, e)

        # Use title as fallback text marker (record will be skipped if too short)
        if len(text) < 50:
            text = ""

        date = parse_date(item.get("date_str", ""))
        if not date:
            m = re.search(r'/t(\d{4})(\d{2})(\d{2})_', url)
            if m:
                date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # Extract standard number if present
        std_num = None
        m = re.search(r'[（(]([A-Z]{1,3}\s*(?:/T\s*)?\d[\d.—–-]+\d)[）)]', item.get("title", "") + " " + text[:500])
        if m:
            std_num = m.group(1).strip()

        doc_id = re.search(r't\d+_(\d+)', url)
        doc_id = doc_id.group(1) if doc_id else url.split("/")[-1].replace(".shtml", "")

        return {
            "id": f"MEE-FGBZ-{doc_id}",
            "title": item["title"],
            "text": text,
            "date": date,
            "url": url,
            "category": item["category"],
            "data_type": item["data_type"],
            "standard_number": std_num,
        }
    except Exception as e:
        logger.warning("Failed to fetch search detail %s: %s", url, e)
        return None


# ── Main fetch functions ──

def fetch_all() -> Generator[dict, None, None]:
    """Yield all normalized documents from all channels."""
    seen_urls = set()

    # Channel 1: Laws & Admin Regulations
    for section in LISTING_SECTIONS:
        logger.info("Fetching %s from listing pages...", section["category"])
        for item in _scrape_listing_pages(section):
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            record = _fetch_listing_detail(item)
            if record and record.get("text"):
                yield normalize(record)
            time.sleep(1.0)

    # Channel 2: Departmental Rules (gzk)
    logger.info("Fetching departmental rules from gzk...")
    for item in _fetch_gzk_regulations():
        if item["url"] in seen_urls:
            continue
        seen_urls.add(item["url"])
        record = _fetch_gzk_detail(item)
        if record and record.get("text"):
            yield normalize(record)
        time.sleep(1.0)

    # Channel 3: Standards & Regulations (site search chnls=5)
    logger.info("Fetching regulations & standards from site search...")
    for item in _fetch_site_search(chnls="5"):
        if item["url"] in seen_urls:
            continue
        seen_urls.add(item["url"])
        record = _fetch_search_detail(item)
        if record and record.get("text"):
            yield normalize(record)
        time.sleep(1.0)


def fetch_sample(count: int = 15) -> list:
    """Fetch a sample of documents from each channel."""
    samples = []
    seen_urls = set()

    # 5 from laws/admin regs
    logger.info("Fetching sample laws and admin regulations...")
    for section in LISTING_SECTIONS:
        for item in _scrape_listing_pages(section, max_pages=2):
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            record = _fetch_listing_detail(item)
            if record and record.get("text") and len(record["text"]) > 100:
                samples.append(normalize(record))
                logger.info("  [%d/%d] %s (%d chars)", len(samples), count, record["title"][:50], len(record["text"]))
            if len(samples) >= 5:
                break
            time.sleep(1.0)
        if len(samples) >= 5:
            break

    # 5 from gzk departmental rules
    logger.info("Fetching sample departmental rules...")
    for item in _fetch_gzk_regulations(max_pages=2):
        if item["url"] in seen_urls:
            continue
        seen_urls.add(item["url"])
        record = _fetch_gzk_detail(item)
        if record and record.get("text") and len(record["text"]) > 100:
            samples.append(normalize(record))
            logger.info("  [%d/%d] %s (%d chars)", len(samples), count, record["title"][:50], len(record["text"]))
        if len(samples) >= 10:
            break
        time.sleep(1.0)

    # 5 from site search
    logger.info("Fetching sample standards & regulations...")
    for item in _fetch_site_search(chnls="5", max_pages=2):
        if item["url"] in seen_urls:
            continue
        seen_urls.add(item["url"])
        record = _fetch_search_detail(item)
        if record and record.get("text") and len(record["text"]) > 100:
            samples.append(normalize(record))
            logger.info("  [%d/%d] %s (%d chars)", len(samples), count, record["title"][:50], len(record["text"]))
        if len(samples) >= count:
            break
        time.sleep(1.0)

    return samples


def bootstrap_sample():
    """Fetch sample data and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    samples = fetch_sample(15)

    for i, record in enumerate(samples, 1):
        out_path = SAMPLE_DIR / f"{i:03d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info("Saved %d sample records to %s", len(samples), SAMPLE_DIR)
    return samples


def test_api() -> bool:
    """Test connectivity to MEE endpoints."""
    ok = True

    # Test gzk
    try:
        resp = session.get(GZK_URL, params={"channelid": GZK_CHANNEL, "page": "1"}, timeout=15)
        total = re.search(r'id="count_temp"[^>]*value="(\d+)"', resp.text)
        count = int(total.group(1)) if total else 0
        logger.info("GZK regulations: %d items (HTTP %d)", count, resp.status_code)
    except Exception as e:
        logger.error("GZK endpoint failed: %s", e)
        ok = False

    # Test site search
    try:
        resp = session.get(SITE_SEARCH_URL, params={
            "channelid": SITE_SEARCH_CHANNEL, "chnls": "5", "page": "1"
        }, timeout=15)
        total = re.search(r'id="count_temp"[^>]*value="(\d+)"', resp.text)
        count = int(total.group(1)) if total else 0
        logger.info("Site search (chnls=5): %d items (HTTP %d)", count, resp.status_code)
    except Exception as e:
        logger.error("Site search failed: %s", e)
        ok = False

    # Test listing page
    try:
        resp = session.get(BASE_URL + "/ywgz/fgbz/fl/", timeout=15)
        links = re.findall(r'href="\./[^"]+\.shtml"', resp.text)
        logger.info("Laws listing: %d links (HTTP %d)", len(links), resp.status_code)
    except Exception as e:
        logger.error("Laws listing failed: %s", e)
        ok = False

    return ok


def main():
    parser = argparse.ArgumentParser(description="CN/MEE data source bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        ok = test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        if args.sample or not args.full:
            records = bootstrap_sample()
            total_text = sum(len(r.get("text", "")) for r in records)
            logger.info("Sample complete: %d records, %d total text chars",
                        len(records), total_text)
        else:
            count = 0
            for record in fetch_all():
                count += 1
                if count % 50 == 0:
                    logger.info("Fetched %d records...", count)
            logger.info("Full fetch complete: %d records", count)


if __name__ == "__main__":
    main()
