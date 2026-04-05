#!/usr/bin/env python3
"""
JP/NTA-Circulars -- Japanese National Tax Agency Circulars Fetcher

Fetches full text of administrative circulars (通達/tsutatsu) from the
Japanese National Tax Agency (NTA) website at nta.go.jp.

Strategy:
  - Start from known TOC (table-of-contents) pages for each tax category
  - Extract all links to individual section pages (pattern: /XX/YY.htm)
  - Fetch each section page and extract full text from HTML
  - Covers: income tax, corporate tax, inheritance/gift tax, consumption tax,
    stamp tax, collection, appeals, and other tax categories

URL pattern: https://www.nta.go.jp/law/tsutatsu/kihon/{category}/XX/YY.htm

Usage:
  python bootstrap.py bootstrap          # Full crawl
  python bootstrap.py bootstrap --sample # Fetch ~15 sample pages
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
from typing import Generator, Optional, Dict, Any, Set, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.NTA-Circulars")

BASE_URL = "https://www.nta.go.jp"
DELAY = 1.5

# TOC pages: (relative_url, category_id, category_name)
# Each links to individual section pages with full text
TOC_PAGES: List[Tuple[str, str, str]] = [
    # Income Tax (所得税)
    ("/law/tsutatsu/kihon/shotoku/01.htm", "shotoku", "Income Tax"),
    # Inheritance & Gift Tax (相続税・贈与税)
    ("/law/tsutatsu/kihon/sisan/sozoku2/01.htm", "sozoku", "Inheritance Tax"),
    # Asset Valuation (財産評価)
    ("/law/tsutatsu/kihon/sisan/hyoka_new/01.htm", "hyoka", "Asset Valuation"),
    # Corporate Tax (法人税)
    ("/law/tsutatsu/kihon/hojin/01.htm", "hojin", "Corporate Tax"),
    # Consolidated Tax (連結納税)
    ("/law/tsutatsu/kihon/renketsu/01.htm", "renketsu", "Consolidated Tax"),
    # Consumption Tax (消費税)
    ("/law/tsutatsu/kihon/shohi/01.htm", "shohi", "Consumption Tax"),
    # Stamp Tax (印紙税)
    ("/law/tsutatsu/kihon/inshi/mokuji.htm", "inshi", "Stamp Tax"),
    # National Tax General Law (国税通則法)
    ("/law/tsutatsu/kihon/tsusoku/00.htm", "tsusoku", "National Tax General Law"),
    # National Tax Collection (国税徴収法)
    ("/law/tsutatsu/kihon/chosyu/index.htm", "chosyu", "Tax Collection"),
    # Delinquency Disposition (滞納処分)
    ("/law/tsutatsu/kihon/tainoshobun/index.htm", "tainoshobun", "Delinquency"),
    # Appeals - Tax Agency (異議申立)
    ("/law/tsutatsu/kihon/igi/01.htm", "igi", "Appeals (Agency)"),
    # Appeals - Tax Appeals Board (審査請求)
    ("/law/tsutatsu/kihon/shinsaseikyu/00.htm", "shinsaseikyu", "Appeals (Board)"),
    # Petroleum Excise (揮発油税)
    ("/law/tsutatsu/kihon/kihatsu/01.htm", "kihatsu", "Petroleum Excise Tax"),
    # Petroleum Gas Tax (石油ガス税)
    ("/law/tsutatsu/kihon/sekiyugasu/01.htm", "sekiyugasu", "Petroleum Gas Tax"),
    # Aircraft Fuel Tax (航空機燃料税)
    ("/law/tsutatsu/kihon/kokuki/01.htm", "kokuki", "Aircraft Fuel Tax"),
    # Power Development Tax (電源開発促進税)
    ("/law/tsutatsu/kihon/dengenkaihatsu/01.htm", "dengenkaihatsu", "Power Development Tax"),
    # Petroleum/Coal Tax (石油石炭税)
    ("/law/tsutatsu/kihon/sekiyusekitan/01.htm", "sekiyusekitan", "Petroleum Coal Tax"),
    # Tobacco Tax (たばこ税)
    ("/law/tsutatsu/kihon/tabako/01.htm", "tabako", "Tobacco Tax"),
    # Liquor Tax (酒税)
    ("/law/tsutatsu/kihon/sake/01.htm", "sake", "Liquor Tax"),
    # International Tourism Tax (国際観光旅客税)
    ("/law/tsutatsu/kihon/kanko/01.htm", "kanko", "Tourism Tax"),
    # Tax Attorney Law (税理士法)
    ("/law/tsutatsu/kihon/zeirishi/01.htm", "zeirishi", "Tax Attorney Law"),
]

# Individual (kobetsu) circular index pages
KOBETSU_PAGES: List[Tuple[str, str, str]] = [
    ("/law/tsutatsu/kobetsu/shotoku/shinkoku/sinkoku.htm", "kobetsu-shotoku-shinkoku", "Individual: Income Tax Filing"),
    ("/law/tsutatsu/kobetsu/shotoku/gensen/gensen.htm", "kobetsu-shotoku-gensen", "Individual: Withholding Tax"),
    ("/law/tsutatsu/kobetsu/shotoku/joto-sanrin/sanrin.htm", "kobetsu-shotoku-joto", "Individual: Transfer/Forest Income"),
    ("/law/tsutatsu/kobetsu/shotoku/sochiho/sotihou.htm", "kobetsu-shotoku-sochiho", "Individual: Income Special Measures"),
    ("/law/tsutatsu/kobetsu/sozoku/souzoku.htm", "kobetsu-sozoku", "Individual: Inheritance"),
    ("/law/tsutatsu/kobetsu/hyoka/zaisan.htm", "kobetsu-hyoka", "Individual: Asset Valuation"),
    ("/law/tsutatsu/kobetsu/sozoku/sochiho/sotihou.htm", "kobetsu-sozoku-sochiho", "Individual: Inheritance Special Measures"),
    ("/law/tsutatsu/kobetsu/hojin/houzin.htm", "kobetsu-hojin", "Individual: Corporate Tax"),
    ("/law/tsutatsu/kobetsu/hojin/sochiho/sotihou.htm", "kobetsu-hojin-sochiho", "Individual: Corporate Special Measures"),
    ("/law/tsutatsu/kobetsu/kansetsu/syouhi.htm", "kobetsu-kansetsu", "Individual: Indirect Tax"),
    ("/law/tsutatsu/kobetsu/kansetsu/sochiho/sotihou.htm", "kobetsu-kansetsu-sochiho", "Individual: Indirect Special Measures"),
    ("/law/tsutatsu/kobetsu/chosyu/chosyu.htm", "kobetsu-chosyu", "Individual: Collection"),
    ("/law/tsutatsu/kobetsu/zeimuchosa/zeimuchosa.htm", "kobetsu-zeimuchosa", "Individual: Tax Audit"),
    ("/law/tsutatsu/kobetsu/hotei/shiryo.htm", "kobetsu-hotei", "Individual: Statutory Materials"),
    ("/law/tsutatsu/kobetsu/zeirishi/zeirishi2.htm", "kobetsu-zeirishi", "Individual: Tax Attorney"),
    ("/law/tsutatsu/kobetsu/denshichoubo/index.htm", "kobetsu-denshi", "Individual: Electronic Records"),
    ("/law/tsutatsu/kobetsu/sonota/sonota.htm", "kobetsu-sonota", "Individual: Other"),
    ("/law/tsutatsu/kobetsu/sonota/sochiho/sotihou.htm", "kobetsu-sonota-sochiho", "Individual: Other Special Measures"),
]


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr|dt|dd)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class NTACirculars(BaseScraper):
    SOURCE_ID = "JP/NTA-Circulars"

    def __init__(self):
        self.http = HttpClient(base_url=BASE_URL)
        self.visited: Set[str] = set()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw

    def fetch_page(self, path: str) -> Optional[str]:
        """Fetch a page by path, return raw HTML."""
        try:
            resp = self.http.get(f"{BASE_URL}{path}")
            time.sleep(DELAY)
            if resp is None or resp.status_code != 200:
                return None
            # NTA pages use Shift_JIS encoding
            resp.encoding = resp.apparent_encoding or "shift_jis"
            return resp.text
        except Exception as e:
            logger.warning("Error fetching %s: %s", path, e)
            return None

    def extract_content_links(self, html_text: str, toc_path: str) -> List[str]:
        """Extract links to content pages from a TOC or index page."""
        links = []
        # Get the base directory of the TOC page
        base_dir = toc_path.rsplit("/", 1)[0]

        # Find all href links
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html_text, re.IGNORECASE)

        for href in hrefs:
            # Skip external, anchor-only, and non-htm links
            if href.startswith(("http", "mailto:", "javascript:", "#")):
                continue
            if not href.endswith(".htm"):
                continue

            # Resolve relative paths
            if href.startswith("/"):
                full_path = href
            else:
                full_path = f"{base_dir}/{href}"

            # Normalize path (resolve ../)
            parts = full_path.split("/")
            resolved = []
            for p in parts:
                if p == "..":
                    if resolved:
                        resolved.pop()
                elif p and p != ".":
                    resolved.append(p)
            full_path = "/" + "/".join(resolved)

            # Only follow links within /law/tsutatsu/
            if "/law/tsutatsu/" in full_path:
                links.append(full_path)

        return links

    def is_content_page(self, path: str, toc_path: str) -> bool:
        """Check if a path looks like a content page (not a TOC/menu page)."""
        # Content pages are typically one level deeper than the TOC
        # e.g., TOC: /kihon/shotoku/01.htm -> Content: /kihon/shotoku/01/01.htm
        # Or content pages for kobetsu link to specific circular docs
        name = path.rsplit("/", 1)[-1]
        # Skip the TOC page itself and known menu pages
        if path == toc_path:
            return False
        if name in ("menu.htm", "index.htm", "mokuji.htm"):
            return False
        # Amendment history pages
        if "/kaisei/" in path:
            return False
        return True

    def parse_content(self, html_text: str) -> Dict[str, Any]:
        """Extract title and full text from a content page."""
        result = {"title": "", "text": "", "date": None}

        # Title from <title> tag
        m = re.search(r'<title>([^<]+)</title>', html_text, re.IGNORECASE)
        if m:
            title = html_module.unescape(m.group(1).strip())
            # Clean common suffixes
            for suffix in ["｜国税庁", "| 国税庁", "- 国税庁"]:
                title = title.replace(suffix, "").strip()
            result["title"] = title

        # Extract main content from body, stripping navigation
        m = re.search(r'<body[^>]*>(.*?)</body>', html_text, re.DOTALL | re.IGNORECASE)
        if m:
            body = m.group(1)
            # Remove script/style/nav/header/footer blocks
            body = re.sub(r'<(script|style|nav|header|footer)[^>]*>.*?</\1>', '', body, flags=re.DOTALL | re.IGNORECASE)
            # Remove breadcrumb navigation
            body = re.sub(r'<[^>]*class="[^"]*breadcrumb[^"]*"[^>]*>.*?</[^>]+>', '', body, flags=re.DOTALL | re.IGNORECASE)
            body = re.sub(r'<div[^>]*id="topicpath"[^>]*>.*?</div>', '', body, flags=re.DOTALL | re.IGNORECASE)
            text = strip_html(body)
            # Remove common boilerplate
            text = re.sub(r'すべての機能をご利用いただくにはJavascriptを有効にしてください。\s*', '', text)
            text = re.sub(r'^法令等\s*法令解釈通達\s*', '', text)
            result["text"] = text.strip()

        # Try to find date
        m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', html_text)
        if m:
            try:
                result["date"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            except ValueError:
                pass

        return result

    def crawl_toc(self, toc_path: str, category_id: str, category_name: str,
                  sample_limit: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """Crawl a TOC page and yield content records."""
        count = 0
        html_text = self.fetch_page(toc_path)
        if not html_text:
            logger.warning("Could not fetch TOC: %s", toc_path)
            return

        links = self.extract_content_links(html_text, toc_path)
        content_links = [l for l in links if self.is_content_page(l, toc_path)]

        # For kobetsu pages, we may need a second level of crawling
        # (the index links to sub-index pages, which link to actual circulars)
        if "kobetsu" in toc_path:
            deeper_links = []
            for link in content_links:
                if link in self.visited:
                    continue
                self.visited.add(link)
                sub_html = self.fetch_page(link)
                if not sub_html:
                    continue
                sub_links = self.extract_content_links(sub_html, link)
                sub_content = [l for l in sub_links if self.is_content_page(l, link)]
                if sub_content:
                    deeper_links.extend(sub_content)
                else:
                    # This might be a content page itself
                    deeper_links.append(link)
                if sample_limit and len(deeper_links) > sample_limit * 2:
                    break
            content_links = deeper_links

        # Remove duplicates while preserving order
        seen = set()
        unique_links = []
        for l in content_links:
            if l not in seen and l not in self.visited:
                seen.add(l)
                unique_links.append(l)
        content_links = unique_links

        logger.info("  Found %d content pages in %s (%s)", len(content_links), category_name, toc_path)

        for link in content_links:
            if sample_limit and count >= sample_limit:
                break
            if link in self.visited:
                continue
            self.visited.add(link)

            page_html = self.fetch_page(link)
            if not page_html:
                continue

            parsed = self.parse_content(page_html)
            if not parsed["text"] or len(parsed["text"]) < 50:
                continue

            # Build a clean ID from the URL path
            doc_id = link.replace("/law/tsutatsu/", "").replace("/", "-").replace(".htm", "")

            yield {
                "_id": f"nta-{doc_id}",
                "_source": self.SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": parsed["title"],
                "text": parsed["text"],
                "date": parsed["date"],
                "url": f"{BASE_URL}{link}",
                "language": "ja",
                "category": category_id,
                "category_name": category_name,
                "section_path": link,
            }
            count += 1

            if count % 20 == 0:
                logger.info("    Progress: %d pages fetched from %s", count, category_name)

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Crawl all TOC pages and yield content records."""
        total = 0
        sample_per_category = 3 if sample else None
        total_sample_limit = 15 if sample else None

        # Crawl basic (kihon) circulars
        for toc_path, cat_id, cat_name in TOC_PAGES:
            if total_sample_limit and total >= total_sample_limit:
                break
            logger.info("Crawling %s: %s", cat_id, cat_name)
            remaining = (total_sample_limit - total) if total_sample_limit else None
            limit = min(sample_per_category, remaining) if sample_per_category and remaining else remaining
            for record in self.crawl_toc(toc_path, cat_id, cat_name, sample_limit=limit):
                yield record
                total += 1
                if total_sample_limit and total >= total_sample_limit:
                    break

        # Also crawl kobetsu (individual circulars) unless in sample mode
        if not sample:
            for toc_path, cat_id, cat_name in KOBETSU_PAGES:
                logger.info("Crawling %s: %s", cat_id, cat_name)
                for record in self.crawl_toc(toc_path, cat_id, cat_name):
                    yield record
                    total += 1

        logger.info("Fetch complete: %d total records from %d pages visited", total, len(self.visited))

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all — circulars are updated in place."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            html_text = self.fetch_page("/law/tsutatsu/kihon/shotoku/01/01.htm")
            if html_text and len(html_text) > 1000:
                logger.info("Test passed: content page accessible, %d bytes", len(html_text))
                return True
            logger.error("Test failed: unexpected content")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="JP/NTA-Circulars bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = NTACirculars()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s | text=%d chars", count, record["title"][:60], text_len)

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        count = sum(1 for _ in scraper.fetch_all())
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
