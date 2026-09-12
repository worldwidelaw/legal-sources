#!/usr/bin/env python3
"""
CN/NNSA -- National Nuclear Safety Administration of China (国家核安全局)

Fetches the full text of China's nuclear-safety legal corpus published by the
National Nuclear Safety Administration (NNSA, 国家核安全局), the nuclear
regulator that operates inside the Ministry of Ecology and Environment:

  * 法规标准文件库 (Laws / Regulations / Standards document library) —
    国家法律 (national laws), 行政法规 (State Council administrative
    regulations), 规章 (NNSA departmental rules), 规范性文件 (normative
    documents), 核安全导则 (nuclear-safety guides, 8 series),
    标准 (national nuclear/radiation standards, 7 series) and
    国际公约 (international conventions China is party to).
  * 核安全局文件 — 文件 (NNSA decisions: licence grants and renewals for
    nuclear installations, civil nuclear-safety equipment designers /
    manufacturers / installers, operator qualifications, approval of
    refuelling programmes, corrective-action orders) and 函 (official
    letters: licence-information changes, inspection reports, rectification
    orders addressed to a named licensee).
  * 部文件 — MEE ministerial documents on nuclear and radiation safety
    (standard-promulgation announcements, radiation-licence notices).
  * 其他 / 解读 — other nuclear-safety policy documents and official
    interpretations of newly issued guides and standards.

Strategy (three enumeration channels, all plain static HTML):

  1. Document library. ``nnsa.mee.gov.cn/ztzl/fgbzwjk/`` embeds an iframe at
     ``/govsearch/haqj.jsp?Stype=2&type=1``. Each left-menu category is a
     ``channelid`` (harvested from the ``getChannel(NNNNN)`` handlers) and
     the listing pages are ``&channelid={cid}&page={n}`` with 20 rows/page;
     the embedded ``m_nRecordCount`` gives the per-channel total.

  2. NNSA/MEE document listings. ``/zcwj/{path}/`` is a classic
     ``index.html`` + ``index_{n}.html`` pager whose page count comes from
     the ``createPageHTML(total_pages, current, "index", "html")`` call.
     Rows are ``<li><span class="date">…</span><a href=…>title</a></li>``.

  3. Detail pages live on several MEE/NNSA templates; the body sits in
     ``Custom_UnionStyle`` / ``TRS_Editor`` (current MEE), ``content_body_box``
     (legacy ``/gkml/`` pages) or ``neiright_JPZGK`` (NNSA-hosted pages), all
     extracted with a balanced-``<div>`` walk.

  Regulations, guides and standards are usually published as a short
  promulgation notice with the instrument itself in an attached PDF/DOCX, so
  attachments are downloaded and appended whenever the page body is thin or
  the title announces a document (发布 / 印发 / 批准 …). PDFs go through
  ``common.pdf_extract`` (PyMuPDF + OCR fallback), DOCX through stdlib zip+XML.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import html as html_mod
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
from typing import Generator, Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.base_scraper import BaseScraper  # noqa: E402

try:
    from common.pdf_extract import extract_pdf_markdown
except Exception:  # pragma: no cover
    extract_pdf_markdown = None

logger = logging.getLogger("legal-data-hunter")

SOURCE_ID = "CN/NNSA"
BASE = "https://nnsa.mee.gov.cn"
LIB_URL = BASE + "/govsearch/haqj.jsp"

# ── Document library: left-menu category → (channelid, label, doc type) ──
# channelids harvested from the getChannel(...) handlers on /ztzl/fgbzwjk/.
LIB_CHANNELS = [
    (33636, "国家法律", "legislation"),
    (33637, "行政法规", "legislation"),
    (33638, "规章", "legislation"),
    (33639, "规范性文件", "legislation"),
    (33642, "国际公约", "legislation"),
    (33640, "核安全导则", "doctrine"),
    (33736, "核安全导则 / 通用系列导则", "doctrine"),
    (33643, "核安全导则 / 核动力厂系列导则", "doctrine"),
    (33644, "核安全导则 / 研究堆系列导则", "doctrine"),
    (33742, "核安全导则 / 非堆核燃料循环设施系列导则", "doctrine"),
    (33741, "核安全导则 / 放射性废物管理系列导则", "doctrine"),
    (33740, "核安全导则 / 核材料管制系列导则", "doctrine"),
    (33739, "核安全导则 / 民用核安全设备监督管理系列导则", "doctrine"),
    (33743, "核安全导则 / 放射性物品运输管理系列导则", "doctrine"),
    (33641, "标准", "doctrine"),
    (33729, "标准 / 通用系列", "doctrine"),
    (33730, "标准 / 核动力厂系列", "doctrine"),
    (33731, "标准 / 研究堆系列", "doctrine"),
    (33732, "标准 / 放射性废物管理系列", "doctrine"),
    (33733, "标准 / 放射性物品运输管理系列", "doctrine"),
    (33734, "标准 / 放射性同位素和射线装置监督管理系列", "doctrine"),
    (33735, "标准 / 辐射环境系列", "doctrine"),
]
LIB_PAGE_SIZE = 20

# A library listing row. The 成文日期 cell is empty for guides/standards (they
# are published as bare PDFs and carry a HAD/GB/HJ number instead of a date),
# so the date group must stay optional.
LIB_ROW_RE = re.compile(
    r'<td class="td-date"[^>]*>\s*<span[^>]*>([^<]*)</span>\s*</td>\s*'
    r'<td>\s*<a href="([^"]+)"[^>]*>(.*?)</a>\s*</td>'
    r'(?:\s*<td class="td-right"[^>]*>(.*?)</td>)?',
    re.S)

# Library rows that point straight at a PDF/DOC (guides + standards).
DIRECT_DOC_RE = re.compile(r"\.(pdf|docx?)$", re.I)

# ── Paged listings under /zcwj/ : (path, label, default doc type) ────────
# Ordered newest-content-first channels first so sample mode fills fast.
DOC_LISTINGS = [
    ("zcwj/haqjwj/wj", "核安全局文件 / 文件", "case_law"),
    ("zcwj/haqjwj/han", "核安全局文件 / 函", "case_law"),
    ("zcwj/bwj", "部文件", "doctrine"),
    ("zcwj/qt", "其他", "doctrine"),
    ("zcwj/jd", "解读", "doctrine"),
]

# A 文件/函 that grants, varies, renews or enforces against a NAMED licensee is
# an administrative adjudication of a specific case; anything else in those
# channels (e.g. 关于发布《…》的通知) is general guidance.
CASE_TITLE_RE = re.compile(
    r"(颁发|换发|延续|注销|吊销|批准|同意|准予|责令|处罚|整改|不予)"
)

# Body containers, most specific first.
CONTENT_CLASSES = [
    "Custom_UnionStyle",
    "TRS_Editor",
    "TRS_UEDITOR",
    "content_body_box",
    "neiright_JPZGK",
    "gz_content_txt",
]

ATTACH_RE = re.compile(
    r'href="([^"]+\.(?:pdf|doc|docx|PDF|DOC|DOCX))"'
)
MIN_BODY_CHARS = 80          # below this a record is dropped as empty
THIN_BODY_CHARS = 1200       # below this, pull attachments to complete the text
MAX_ATTACHMENTS = 3
MAX_ATTACH_BYTES = 40 * 1024 * 1024
ATTACH_TITLE_RE = re.compile(r"(发布|印发|颁布|批准|公布|转发)")


def _strip_html(fragment: str) -> str:
    fragment = re.sub(r"<script.*?</script>", "", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<style.*?</style>", "", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"</(p|div|tr|h[1-6]|li)>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"<[^>]+>", "", fragment)
    text = html_mod.unescape(fragment).replace("　", " ").replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def _balanced_div(page: str, start: int) -> str:
    """Return the substring of `page` covering the <div> starting at `start`."""
    depth = 0
    for m in re.finditer(r"<div\b|</div>", page[start:]):
        if m.group(0) == "</div>":
            depth -= 1
            if depth == 0:
                return page[start:start + m.end()]
        else:
            depth += 1
    return page[start:]


_CJK_RE = re.compile(r"[一-鿿]")


def _is_garbled(text: str) -> bool:
    """True for PDFs whose CJK font has no usable ToUnicode map.

    Several pre-2010 guides are typeset with embedded subset fonts, so the
    text layer decodes to ``(cid:213)`` runs and stray Latin glyphs rather
    than Chinese. Such output is unusable, so it is dropped instead of being
    stored as if it were the document body.
    """
    stripped = re.sub(r"\s+", "", text)
    if len(stripped) < 200:
        return False
    if stripped.count("(cid:") > 20:
        return True
    return len(_CJK_RE.findall(stripped)) / len(stripped) < 0.20


def _docx_text(blob: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            xml = zf.read("word/document.xml")
        ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        root = ET.fromstring(xml)
        out = []
        for p in root.iter(ns + "p"):
            line = "".join(t.text for t in p.iter(ns + "t") if t.text).strip()
            if line:
                out.append(line)
        return "\n".join(out)
    except Exception as e:
        logger.debug("DOCX extraction failed: %s", e)
        return ""


def _parse_date(value: str) -> Optional[str]:
    if not value:
        return None
    m = re.search(r"(\d{4})\s*[-年./]\s*(\d{1,2})\s*[-月./]\s*(\d{1,2})", value)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None


# Guides and standards carry no listing date; their cover page states the
# promulgation date as "2025-03-15 发布" or "二〇二五年三月十五日发布".
ISSUE_DATE_RE = re.compile(
    r"(\d{4})\s*[-年./]\s*(\d{1,2})\s*[-月./]\s*(\d{1,2})\s*日?\s*"
    r"(?:发\s*布|批\s*准|公\s*布|实\s*施)")


def _issue_date_from_text(text: str) -> Optional[str]:
    m = ISSUE_DATE_RE.search(text[:6000])
    if not m:
        return None
    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1950 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _clean_doc_number(value: str) -> Optional[str]:
    """The 发文字号 cell repeats the number twice on some templates."""
    text = _strip_html(value or "").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    half = len(text) // 2
    if text and len(text) % 2 == 0 and text[:half] == text[half:]:
        text = text[:half]
    return text[:200] or None


def _doc_id(url: str) -> str:
    """Stable id from the CMS document filename, e.g. t20230303_1018320."""
    stem = url.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
    stem = re.sub(r"\.s?html?$", "", stem, flags=re.I)
    return stem or url


class NNSAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
        self.delay = 0.5

    # ── low-level fetch ──────────────────────────────────────────────

    def _get(self, url: str, binary: bool = False, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90)
                if r.status_code == 200:
                    if binary:
                        return r.content
                    body = r.content
                    for enc in ("utf-8", "gb18030"):
                        try:
                            return body.decode(enc)
                        except UnicodeDecodeError:
                            continue
                    return body.decode("utf-8", "replace")
                if r.status_code in (403, 404, 410):
                    return None
                logger.warning("HTTP %s for %s", r.status_code, url)
            except Exception as e:
                logger.warning("GET error %s (attempt %d): %s", url, attempt + 1, e)
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ── enumeration ──────────────────────────────────────────────────

    def _library_channel(self, cid: int, label: str,
                         doc_type: str) -> Generator[dict, None, None]:
        first = self._get(f"{LIB_URL}?Stype=2&type=1&orderby=date&channelid={cid}")
        if first is None:
            logger.warning("library channel %s (%s) unreachable", cid, label)
            return
        m = re.search(r"m_nRecordCount\s*=\s*(\d+)", first)
        total = int(m.group(1)) if m else 0
        pages = max(1, -(-total // LIB_PAGE_SIZE))
        logger.info("library %s (%s): %d records / %d pages", cid, label, total, pages)

        for page in range(1, pages + 1):
            html_page = first if page == 1 else self._get(
                f"{LIB_URL}?Stype=2&type=1&orderby=date&channelid={cid}&page={page}"
            )
            if html_page is None:
                continue
            rows = LIB_ROW_RE.findall(html_page)
            if not rows:
                logger.debug("library %s page %d: no rows", cid, page)
            for date_txt, url, title_html, docnum in rows:
                title = _strip_html(title_html).replace("\n", " ").strip()
                if not title:
                    continue
                yield {
                    "url": url.split("?")[0],
                    "title": title,
                    "date": _parse_date(date_txt),
                    "category": label,
                    "doc_type": doc_type,
                    "doc_number": _clean_doc_number(docnum),
                }

    def _doc_listing(self, path: str, label: str,
                     doc_type: str) -> Generator[dict, None, None]:
        first = self._get(f"{BASE}/{path}/")
        if first is None:
            logger.warning("listing %s unreachable", path)
            return
        m = re.search(r"createPageHTML\(\s*(\d+)", first)
        pages = int(m.group(1)) if m else 1
        logger.info("listing %s (%s): %d pages", path, label, pages)

        for page in range(pages):
            url = (f"{BASE}/{path}/" if page == 0
                   else f"{BASE}/{path}/index_{page}.html")
            html_page = first if page == 0 else self._get(url)
            if html_page is None:
                continue
            rows = re.findall(
                r'<li><span class="date">([\d-]{8,10})</span>\s*'
                r'<a href="([^"]+)"[^>]*>(.*?)</a>',
                html_page, re.S)
            for date_txt, href, title_html in rows:
                title = _strip_html(title_html).replace("\n", " ").strip()
                if not title:
                    continue
                if href.startswith("./"):
                    href = f"{BASE}/{path}/" + href[2:]
                elif href.startswith("/"):
                    href = BASE + href
                kind = doc_type
                if doc_type == "case_law" and not CASE_TITLE_RE.search(title):
                    kind = "doctrine"
                yield {
                    "url": href.split("?")[0],
                    "title": title,
                    "date": _parse_date(date_txt),
                    "category": label,
                    "doc_type": kind,
                    "doc_number": None,
                }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Round-robin across every channel.

        Channels differ wildly in size (4 national laws vs ~3,000 NNSA
        decisions) and in shape (HTML notice vs bare standard PDF). Draining
        them in sequence would make a truncated run — or a sample — cover only
        the first category, so they are interleaved instead.
        """
        # Decision listings first so that even a short run carries all three
        # data types; there are 22 library channels but only 5 listings.
        streams = [self._doc_listing(path, label, dt)
                   for path, label, dt in DOC_LISTINGS]
        streams += [self._library_channel(cid, label, dt)
                    for cid, label, dt in LIB_CHANNELS]
        seen = set()
        while streams:
            for stream in list(streams):
                try:
                    item = next(stream)
                except StopIteration:
                    streams.remove(stream)
                    continue
                key = _doc_id(item["url"])
                if key in seen:
                    continue
                seen.add(key)
                yield item

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Newest page of every channel; the loader dedups on _id."""
        for cid, label, doc_type in LIB_CHANNELS[:5]:
            yield from self._library_channel(cid, label, doc_type)
        for path, label, doc_type in DOC_LISTINGS:
            first = self._get(f"{BASE}/{path}/")
            if first is None:
                continue
            rows = re.findall(
                r'<li><span class="date">([\d-]{8,10})</span>\s*'
                r'<a href="([^"]+)"[^>]*>(.*?)</a>',
                first, re.S)
            for date_txt, href, title_html in rows:
                title = _strip_html(title_html).replace("\n", " ").strip()
                if href.startswith("./"):
                    href = f"{BASE}/{path}/" + href[2:]
                elif href.startswith("/"):
                    href = BASE + href
                kind = doc_type
                if doc_type == "case_law" and not CASE_TITLE_RE.search(title):
                    kind = "doctrine"
                yield {"url": href.split("?")[0], "title": title,
                       "date": _parse_date(date_txt), "category": label,
                       "doc_type": kind, "doc_number": None}

    # ── full text ────────────────────────────────────────────────────

    def _page_body(self, page: str) -> str:
        best = ""
        for cls in CONTENT_CLASSES:
            m = re.search(r'<div[^>]*class="[^"]*\b%s\b[^"]*"' % re.escape(cls), page)
            if not m:
                continue
            text = _strip_html(_balanced_div(page, m.start()))
            if len(text) > len(best):
                best = text
            if len(best) > 400:
                break
        return best

    def _attachment_text(self, page: str, page_url: str) -> str:
        base = page_url.rsplit("/", 1)[0] + "/"
        chunks, done = [], set()
        for href in ATTACH_RE.findall(page)[: MAX_ATTACHMENTS * 2]:
            if href.startswith("./"):
                href = base + href[2:]
            elif href.startswith("/"):
                href = "https://www.mee.gov.cn" + href
            elif not href.startswith("http"):
                href = base + href
            if href in done:
                continue
            done.add(href)
            blob = self._get(href, binary=True)
            if not blob or len(blob) > MAX_ATTACH_BYTES:
                continue
            if href.lower().endswith((".docx", ".doc")):
                text = _docx_text(blob)
            elif extract_pdf_markdown is not None:
                try:
                    text = extract_pdf_markdown(
                        page_url, SOURCE_ID, pdf_url=href,
                        pdf_bytes=blob, table="legislation") or ""
                except Exception as e:
                    logger.debug("PDF extraction failed for %s: %s", href, e)
                    text = ""
            else:
                text = ""
            if len(text) > 200 and not _is_garbled(text):
                chunks.append(text.strip())
            if len(chunks) >= MAX_ATTACHMENTS:
                break
        return "\n\n".join(chunks)

    def _direct_document_text(self, url: str) -> str:
        """Guides and standards are published as bare PDF/DOC files."""
        blob = self._get(url, binary=True)
        if not blob or len(blob) > MAX_ATTACH_BYTES:
            return ""
        if url.lower().endswith((".doc", ".docx")):
            return _docx_text(blob)
        if extract_pdf_markdown is None:
            return ""
        try:
            return extract_pdf_markdown(
                url, SOURCE_ID, pdf_url=url, pdf_bytes=blob,
                table="legislation") or ""
        except Exception as e:
            logger.debug("PDF extraction failed for %s: %s", url, e)
            return ""

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        title = raw.get("title") or ""
        date = raw.get("date")

        if DIRECT_DOC_RE.search(url.split("?")[0]):
            body = re.sub(r"\n{3,}", "\n\n", self._direct_document_text(url)).strip()
            if len(body) < MIN_BODY_CHARS:
                return None
            if _is_garbled(body):
                logger.info("unusable text layer (no ToUnicode map): %s", url)
                return None
            return self._record(raw, url, title, body,
                                date or _issue_date_from_text(body))

        page = self._get(url)
        if page is None:
            logger.debug("detail page unreachable: %s", url)
            return None

        body = self._page_body(page)

        want_attachments = (
            len(body) < THIN_BODY_CHARS or bool(ATTACH_TITLE_RE.search(title))
        )
        if want_attachments:
            extra = self._attachment_text(page, url)
            if extra and extra not in body:
                body = (body + "\n\n" + extra).strip() if body else extra

        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        if len(body) < MIN_BODY_CHARS:
            return None

        if not date:
            m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", page)
            if m:
                date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return self._record(raw, url, title, body, date)

    def _record(self, raw: dict, url: str, title: str,
                body: str, date: Optional[str]) -> dict:

        return {
            "_id": _doc_id(url),
            "_source": SOURCE_ID,
            "_type": raw.get("doc_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": body,
            "date": date,
            "url": url,
            "category": raw.get("category"),
            "document_number": raw.get("doc_number"),
            "language": "zh",
            "issuing_body": "国家核安全局 (National Nuclear Safety Administration)",
        }

    # ── connectivity test ────────────────────────────────────────────

    def test_api(self) -> bool:
        ok = True
        page = self._get(f"{LIB_URL}?Stype=2&type=1&orderby=date")
        if page and re.search(r"m_nRecordCount\s*=\s*(\d+)", page):
            total = re.search(r"m_nRecordCount\s*=\s*(\d+)", page).group(1)
            logger.info("test-api: document library reports %s records", total)
        else:
            logger.error("test-api: document library unreachable")
            ok = False

        listing = self._get(f"{BASE}/zcwj/haqjwj/wj/")
        if listing:
            pages = re.search(r"createPageHTML\(\s*(\d+)", listing)
            rows = re.findall(r'<li><span class="date">', listing)
            logger.info("test-api: 核安全局文件/文件 %s pages, %d rows on page 1",
                        pages.group(1) if pages else "?", len(rows))
        else:
            logger.error("test-api: 核安全局文件 listing unreachable")
            ok = False

        sample = {
            "url": "https://www.mee.gov.cn/gzk/gz/202408/t20240802_1083222.shtml",
            "title": "研究堆营运单位核安全报告规定",
            "date": "2024-08-02", "category": "规章",
            "doc_type": "legislation", "doc_number": "部令 第34号",
        }
        rec = self.normalize(sample)
        if rec and rec["text"]:
            logger.info("test-api normalize: %d chars of full text", len(rec["text"]))
        else:
            logger.error("test-api: normalize produced no full text")
            ok = False
        return ok


def main():
    parser = argparse.ArgumentParser(description="CN/NNSA bootstrap")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    scraper = NNSAScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info("bootstrap-fast complete: %s", json.dumps(stats, default=str))
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    logger.info("bootstrap complete: %s", json.dumps(stats, default=str))


if __name__ == "__main__":
    main()
