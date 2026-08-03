#!/usr/bin/env python3
"""
CN/CICC -- China International Commercial Court (国际商事法庭), full judgment text

The CICC is a special division of the Supreme People's Court of China hearing
cross-border commercial disputes. It publishes full judgments (判决书 / 裁判文书)
and guiding/typical cases (典型案例) as server-rendered HTML at cicc.court.gov.cn.

Distinct from AE/DIFC-Courts, AE/ADGM-Courts, SG/SICC (other international
commercial courts already in the manifest). Small but high-value corpus.

Access recipe (the site is behind the Chinese-government "WZWS" WAF):
  - The first request to any URL 302-redirects and sets a ``wzws_cid`` cookie.
  - Immediately re-requesting the SAME URL with that cookie returns HTTP 200
    with the real HTML. The scraper does this two-step transparently.

Full text of each decision lives in the article's <p> paragraphs (rich text);
nav/footer boilerplate lines are stripped.

Usage:
  python bootstrap.py bootstrap --sample   # sample records
  python bootstrap.py bootstrap            # full corpus
  python bootstrap.py bootstrap-fast       # full corpus (threaded)
  python bootstrap.py test                 # connectivity test
"""

import sys
import re
import time
import html as H
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.CICC")

BASE = "https://cicc.court.gov.cn"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Case-law category index pages (裁判文书 / 判决书 / 典型案例).
CATEGORY_INDEXES = [
    "/html/1/218/180/316/index.html",         # 判决书 (judgments — full text)
    "/html/1/218/347/329/428/434/index.html",  # 裁判文书 (judgment documents)
    "/html/1/218/62/163/422/index.html",       # “一带一路”典型案例 (BRI typical cases)
    "/html/1/218/62/163/425/index.html",       # 其他案例 (other cases)
    "/html/1/218/62/163/index.html",           # 指导性案例与典型案例 (parent)
]

REQUEST_TIMEOUT = 40
MIN_TEXT_CHARS = 300

# Boilerplate lines to drop from the extracted <p> body.
_NAV_LINES = {
    "微博", "微信", "Facebook", "微博 微信 Facebook",
    "调解服务", "仲裁服务", "诉讼服务", "辅助服务",
}
_FOOTER_RE = re.compile(
    r"(地址：北京市东城区东交民巷|版权所有|京ICP备|总机：|举报电话：)"
)


class CICCScraper(BaseScraper):
    """Scraper for CN/CICC -- full judgment text from cicc.court.gov.cn."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session: Optional[requests.Session] = None

    def _new_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Referer": BASE + "/"})
        return s

    def _get(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        if self._session is None:
            self._session = self._new_session()
        for attempt in range(retries):
            try:
                r = self._session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False)
                # WZWS: first hit 302s + sets wzws_cid; retry same URL with cookie.
                if r.status_code in (301, 302):
                    r = self._session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False)
                if r.status_code == 200 and len(r.content) > 800:
                    return r
            except requests.RequestException as e:
                logger.warning("request error %s (attempt %s): %s", url, attempt, e)
                self._session = self._new_session()
            time.sleep(1 + attempt)
        return None

    @staticmethod
    def _abs(href: str) -> str:
        if href.startswith("http"):
            return href
        return BASE + href

    def _list_articles(self, index_path: str) -> List[str]:
        r = self._get(self._abs(index_path))
        if r is None:
            return []
        html = r.content.decode("utf-8", "ignore")
        arts = re.findall(
            r'href=["\']((?:https?://cicc\.court\.gov\.cn)?/html/1/[\d/]+/\d+\.html)',
            html,
        )
        out, seen = [], set()
        for a in arts:
            if a.endswith("index.html"):
                continue
            u = self._abs(a)
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    @staticmethod
    def _extract_body(html: str) -> str:
        ps = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html)
        lines = []
        for p in ps:
            line = H.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", p))).strip()
            if not line or line in _NAV_LINES:
                continue
            if _FOOTER_RE.search(line):
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    @staticmethod
    def _title(html: str) -> str:
        m = re.search(r"<title>(.*?)</title>", html, re.S)
        if not m:
            return ""
        t = H.unescape(m.group(1)).strip()
        # Strip the site prefix "国际商事法庭 | CICC - ".
        t = re.sub(r"^国际商事法庭\s*\|\s*CICC\s*-\s*", "", t)
        return t.strip()

    @staticmethod
    def _pub_date(html: str) -> Optional[str]:
        m = re.search(r"发布时间[:：]\s*(\d{4})-(\d{1,2})-(\d{1,2})", html)
        if m:
            y, mo, d = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"
        return None

    @staticmethod
    def _case_number(text: str) -> str:
        m = re.search(r"[（(]\s*\d{4}\s*[）)][^\n，。]{0,20}?号", text)
        return m.group(0).strip() if m else ""

    def _fetch_article(self, url: str) -> Optional[dict]:
        r = self._get(url)
        if r is None:
            return None
        html = r.content.decode("utf-8", "ignore")
        body = self._extract_body(html)
        if len(body) < MIN_TEXT_CHARS:
            return None
        return {
            "url": url,
            "title": self._title(html),
            "text": body,
            "pub_date": self._pub_date(html),
            "case_number": self._case_number(body),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        seen = set()
        for idx in CATEGORY_INDEXES:
            articles = self._list_articles(idx)
            logger.info("category %s -> %d articles", idx, len(articles))
            for url in articles:
                if url in seen:
                    continue
                seen.add(url)
                rec = self._fetch_article(url)
                if rec:
                    yield rec
                time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # Small corpus; re-scan everything and let the loader dedup.
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if not text:
            return None
        url = raw.get("url", "")
        title = (raw.get("title") or "").strip()
        if not title:
            title = text.split("\n", 1)[0][:150]

        # Stable id from the article numeric path segment.
        m = re.search(r"/(\d+)\.html$", url)
        art_id = m.group(1) if m else hashlib.sha256(url.encode()).hexdigest()[:12]
        doc_id = f"CN-CICC-{art_id}"

        return {
            "_id": doc_id,
            "_source": "CN/CICC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("pub_date"),
            "url": url,
            "process_number": raw.get("case_number", ""),
            "court": "China International Commercial Court (国际商事法庭)",
            "language": "zh",
        }


if __name__ == "__main__":
    scraper = CICCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        print("Testing CICC connectivity (WZWS two-step)...")
        arts = scraper._list_articles(CATEGORY_INDEXES[0])
        print(f"  judgments index -> {len(arts)} articles")
        if arts:
            rec = scraper._fetch_article(arts[0])
            if rec:
                print(f"  sample: {rec['title'][:60]}")
                print(f"  text chars: {len(rec['text'])} | case_no: {rec['case_number']} | date: {rec['pub_date']}")
            else:
                print("  FAIL: could not extract article body")
                sys.exit(1)
        else:
            print("  FAIL: no articles / WAF blocked")
            sys.exit(1)

    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif cmd == "bootstrap-fast":
        workers = 5
        batch_size = 100
        for i, arg in enumerate(sys.argv):
            if arg == "--workers" and i + 1 < len(sys.argv):
                workers = int(sys.argv[i + 1])
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        fetched = stats.get("records_fetched", 0)
        logger.info(f"Bootstrap-fast complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
