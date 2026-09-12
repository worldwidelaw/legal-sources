#!/usr/bin/env python3
"""
KR/NSSC -- Nuclear Safety and Security Commission (원자력안전위원회)

The NSSC is Korea's independent nuclear regulator: a standing commission that
licenses reactors and radiation facilities, sets the binding safety standards
under the Nuclear Safety Act (원자력안전법), and imposes administrative
sanctions (행정처분) on licensees. Two of its boards on www.nssc.go.kr carry
the primary material, and both are collected here:

  1. **최근개정법령 — recently amended law** (BOARD_SEQ 39, MENU_ID 2280).
     The promulgation PDF of every instrument in the nuclear-safety stack as
     it is amended: 원자력안전법 and its Enforcement Decree/Rules, the
     Presidential and Prime Minister's Ordinances, and — the part that exists
     nowhere else in this form — the **원자력안전위원회고시**, the NSSC
     notices that state the actual binding technical requirements (radiation
     protection standards, reactor design criteria, transport and packaging,
     radioactive-waste classification …). Each PDF is the promulgated text of
     the amendment plus its 부칙 (supplementary provisions) and the
     개정이유 (statement of reasons). Typed ``legislation``.

  2. **일정/회의결과/회의록 — commission proceedings** (BOARD_SEQ 14,
     MENU_ID 170). For every sitting of the commission: the agenda with the
     disposition of each item (의결안건 / 보고안건, 원안의결·수정의결·보고),
     and, once cleared for release, the **의사록** (formal record of
     decisions) and **회의록** (verbatim transcript) as PDFs running to
     hundreds of pages. This is where licence grants, licence amendments and
     administrative sanctions against operators are argued and decided, so it
     is the only public record of the commission's reasoning. Typed
     ``doctrine``.

Access notes:

  * The site is a standard Korean-government CMS. The rendered board page is
    an empty shell; the rows come from ``POST /ajaxf/FR_BBS_SVC/BBSViewList.do``
    as JSON, and — usefully — that response already carries the full board
    body in ``CONTENTS``, so listing and detail are one request. Attachments
    come from ``POST /ajaxf/FR_BBS_SVC/BBSViewAttachList.do`` and are fetched
    from ``GET /ajaxfile/FR_SVC/FileDown.do``.
  * ``pagePerCnt`` is accepted but ignored — the server always returns 15 rows
    — so paging walks ``pageNo`` until ``totalRecordCount`` is exhausted.
  * Board 39's PDFs are born-digital and extract cleanly. The 회의록
    transcripts are exported from HWP and carry no intra-sentence spaces;
    that is how the source publishes them and the text is left as issued.
  * Every board row has a permalink at ``/ko/cms/FR_BBS_CON/BoardView.do``.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import html as html_mod
import io
import json
import logging
import re
import sys
import time
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from urllib.parse import unquote, urlencode

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.hwp_extract import extract_hwp_any
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.NSSC")

SOURCE_ID = "KR/NSSC"
BASE = "https://www.nssc.go.kr"
LIST_URL = f"{BASE}/ajaxf/FR_BBS_SVC/BBSViewList.do"
ATTACH_URL = f"{BASE}/ajaxf/FR_BBS_SVC/BBSViewAttachList.do"
FILE_URL = f"{BASE}/ajaxfile/FR_SVC/FileDown.do"
VIEW_URL = f"{BASE}/ko/cms/FR_BBS_CON/BoardView.do"

SITE_NO = 2

# The two boards that carry primary regulatory material. Everything else on
# the site is press releases, monitoring bulletins or recruitment notices.
BOARDS = [
    {
        "board_seq": 39,
        "menu_id": 2280,
        "label": "최근개정법령",
        "label_en": "Recently amended law",
        "doc_type": "legislation",
        "table": "legislation",
    },
    {
        "board_seq": 14,
        "menu_id": 170,
        "label": "일정/회의결과/회의록",
        "label_en": "Commission proceedings",
        "doc_type": "doctrine",
        "table": "doctrine",
    },
]

# A body of fewer than this many characters is a one-line pointer ("the minutes
# of the Nth sitting are released herewith"), not a document.
MIN_TEXT = 200

# The commission's proceedings board links the agenda papers and the 의사록
# straight out of the posting body rather than attaching them, so the body HTML
# has to be mined for them as well as the attachment list.
INLINE_FILE_RE = re.compile(
    r'<a[^>]*href="(https?://(?:www\.)?nssc\.go\.kr/attach/[^"]+)"[^>]*>(.*?)</a>',
    re.S | re.I,
)

# Preference order when a posting ships the same document in several formats.
# The PDF is what the commission treats as the published copy; the HWP/HWPX is
# the editable original and extracts just as well, so it is the fallback rather
# than a second copy of the same text.
FORMAT_RANK = {"pdf": 0, "hwpx": 1, "hwp": 2}
DOC_EXTENSIONS = ("pdf", "hwpx", "hwp")

DATE_RE = re.compile(r"^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})")

# 원자력안전위원회고시 제2026-12호 / 총리령 제2132호 / 대통령령 제34567호 ...
INSTRUMENT_RE = re.compile(
    r"(원자력안전위원회고시|총리령|대통령령|법률|훈령|예규|고시)\s*제\s*([0-9]+(?:-[0-9]+)?)\s*호"
)
# 제2026-11회 / 제226회 — the sitting number of a commission meeting.
SITTING_RE = re.compile(r"제\s*(\d{4}-\d+|\d+)\s*회")


def clean_text(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\x00", " ").replace("­", "")
    text = re.sub(r"[ \t 　]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def strip_tags(fragment: str) -> str:
    """Board bodies are stored as WYSIWYG HTML with heavy inline styling."""
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", fragment, flags=re.S | re.I)
    text = re.sub(r"</(p|div|tr|li|h[1-6]|table)>", "\n", text, flags=re.I)
    text = re.sub(r"</t[dh]>", "\t", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = html_mod.unescape(text)
    return clean_text(text)


def iso_date(value: str | None) -> str | None:
    if not value:
        return None
    m = DATE_RE.match(value.strip())
    if not m:
        return None
    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1950 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


class NSSCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            }
        )
        self.delay = 0.8

    # ---- low-level fetch ----------------------------------------------------

    def _request(
        self,
        url: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
        retries: int = 3,
        timeout: int = 120,
    ) -> requests.Response | None:
        headers = {"Referer": f"{BASE}/"}
        if data is not None:
            headers["X-Requested-With"] = "XMLHttpRequest"
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                if data is None:
                    resp = self.session.get(
                        url, params=params, headers=headers, timeout=timeout
                    )
                else:
                    resp = self.session.post(
                        url, data=data, headers=headers, timeout=timeout
                    )
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (404, 410):
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _post_json(self, url: str, payload: dict) -> dict | list | None:
        resp = self._request(url, data=payload)
        if resp is None:
            return None
        try:
            return resp.json()
        except ValueError:
            logger.warning(f"Non-JSON response from {url}")
            return None

    # ---- board listing ------------------------------------------------------

    def _list_page(self, board: dict, page_no: int) -> tuple[list[dict], int]:
        payload = {
            "pageNo": page_no,
            # Accepted but ignored — the server pins the page at 15 rows.
            "pagePerCnt": 15,
            "MENU_ID": board["menu_id"],
            "CONTENTS_NO": "",
            "SITE_NO": SITE_NO,
            "BOARD_SEQ": board["board_seq"],
            "BBS_SEQ": "",
            "SEARCH_FLD": "",
            "SEARCH": "",
        }
        data = self._post_json(LIST_URL, payload)
        if not isinstance(data, dict) or "data" not in data:
            return [], 0
        block = data["data"] or {}
        rows = block.get("list") or []
        total = int(block.get("totalRecordCount") or 0)
        return rows, total

    def _attachments(self, board: dict, bbs_seq: int) -> list[dict]:
        payload = {
            "MENU_ID": board["menu_id"],
            "SITE_NO": SITE_NO,
            "BOARD_SEQ": board["board_seq"],
            "BBS_SEQ": bbs_seq,
        }
        data = self._post_json(ATTACH_URL, payload)
        if isinstance(data, dict):
            data = data.get("data") or []
        if not isinstance(data, list):
            return []
        return [f for f in data if (f.get("USE_YN") or "Y") == "Y"]

    def _download(self, url: str, params: dict | None = None) -> bytes | None:
        resp = self._request(url, params=params)
        if resp is None:
            return None
        body = resp.content
        # The CMS answers a missing or access-controlled file with an HTML
        # error page under a 200, so sniff the container rather than trusting
        # the status line.
        if body[:4] == b"%PDF" or body[:2] == b"PK" or body[:4] == b"\xd0\xcf\x11\xe0":
            return body
        return None

    def _attachment_bytes(self, board: dict, bbs_seq: int, file_seq: int) -> bytes | None:
        return self._download(
            FILE_URL,
            params={
                "GBN": "X01",
                "BOARD_SEQ": board["board_seq"],
                "SITE_NO": SITE_NO,
                "BBS_SEQ": bbs_seq,
                "FILE_SEQ": file_seq,
            },
        )

    # ---- document extraction ------------------------------------------------

    def _extract(self, blob: bytes, name: str, doc_id: str, table: str) -> str | None:
        """Text of one published document, whichever container it arrived in."""
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        if blob[:4] == b"%PDF":
            text = extract_pdf_markdown(
                SOURCE_ID, doc_id, pdf_bytes=blob, table=table, force=True
            )
            return clean_text(text) if text else None
        if blob[:2] == b"PK" and ext != "hwpx":
            # A ZIP that is not an .hwpx: the commission occasionally bundles a
            # multi-part instrument (regulation plus its annexed forms).
            return self._extract_zip(blob, doc_id, table)
        text = extract_hwp_any(blob, name)
        return clean_text(text) if text else None

    def _extract_zip(self, blob: bytes, doc_id: str, table: str) -> str | None:
        try:
            zf = zipfile.ZipFile(io.BytesIO(blob))
        except Exception:
            return None
        parts: list[str] = []
        for info in sorted(zf.infolist(), key=lambda i: i.filename):
            inner = unicodedata.normalize("NFC", info.filename)
            if not inner.lower().endswith(DOC_EXTENSIONS):
                continue
            try:
                data = zf.read(info)
            except Exception:
                continue
            text = self._extract(data, inner, f"{doc_id}-{inner}", table)
            if text:
                parts.append(f"[{inner.rsplit('/', 1)[-1]}]\n{text}")
        return clean_text("\n\n".join(parts)) or None

    def _documents(self, board: dict, row: dict) -> list[dict]:
        """Every published document reachable from one board posting.

        Two channels: files registered with the CMS as attachments, and files
        the editor linked straight out of the posting body (which is how the
        proceedings board publishes agenda papers and 의사록). Both are keyed
        on the filename stem so an HWP/PDF pair of the same document is
        collected once, in the better format.
        """
        bbs_seq = row["BBS_SEQ"]
        candidates: dict[str, dict] = {}

        def offer(name: str, rank_ext: str, fetch) -> None:
            stem = re.sub(r"\.[^.]+$", "", unicodedata.normalize("NFC", name)).strip()
            rank = FORMAT_RANK.get(rank_ext, 9)
            existing = candidates.get(stem)
            if existing is None or rank < existing["rank"]:
                candidates[stem] = {"name": name, "rank": rank, "fetch": fetch}

        for att in self._attachments(board, bbs_seq):
            name = unicodedata.normalize("NFC", att.get("FILE_ORG_NM") or "")
            file_seq = att.get("FILE_SEQ")
            if not name or file_seq is None:
                continue
            ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext not in DOC_EXTENSIONS and ext != "zip":
                continue  # xlsx form tables etc. carry no running text
            offer(
                name,
                ext,
                lambda fs=file_seq: self._attachment_bytes(board, bbs_seq, fs),
            )

        for href, label in INLINE_FILE_RE.findall(row.get("CONTENTS") or ""):
            href = html_mod.unescape(href)
            file_name = unquote(href.rsplit("/", 1)[-1])
            ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
            if ext not in DOC_EXTENSIONS and ext != "zip":
                continue
            # The stored name is an opaque timestamp+token; the anchor text is
            # the document's real title, so key and label on that when present.
            title = strip_tags(label).lstrip("→ ").strip()
            name = f"{title}.{ext}" if title else file_name
            offer(name, ext, lambda u=href: self._download(u))

        documents = []
        for stem, cand in candidates.items():
            blob = cand["fetch"]()
            if blob is None:
                continue
            documents.append({"name": cand["name"], "blob": blob})
        return documents

    @staticmethod
    def _permalink(board: dict, bbs_seq: int) -> str:
        return f"{VIEW_URL}?" + urlencode(
            {
                "MENU_ID": board["menu_id"],
                "CONTENTS_NO": "",
                "SITE_NO": SITE_NO,
                "BOARD_SEQ": board["board_seq"],
                "BBS_SEQ": bbs_seq,
                "pageNo": 1,
            }
        )

    def _enumerate_board(self, board: dict) -> Generator[dict, None, None]:
        rows, total = self._list_page(board, 1)
        if not rows:
            raise RuntimeError(
                f"NSSC board {board['board_seq']} ({board['label']}) returned no "
                "rows — nssc.go.kr blocks this vantage or the board API changed; "
                "refusing to report an empty corpus"
            )
        logger.info(f"{board['label']}: {total} postings")

        seen: set[int] = set()
        page_no = 1
        while rows:
            for row in rows:
                bbs_seq = row.get("BBS_SEQ")
                if bbs_seq is None or bbs_seq in seen:
                    continue
                seen.add(bbs_seq)
                yield {"board": board, "row": row}
            if len(seen) >= total:
                break
            page_no += 1
            if page_no > (total // 15) + 3:  # paging guard
                break
            rows, _ = self._list_page(board, page_no)

    # ---- normalisation ------------------------------------------------------

    def _normalize_row(self, board: dict, row: dict) -> dict | None:
        bbs_seq = row.get("BBS_SEQ")
        subject = unicodedata.normalize("NFC", (row.get("SUBJECT") or "").strip())
        if not subject:
            return None
        date = iso_date(row.get("SORT") or row.get("WRITE_DATE"))
        url = self._permalink(board, bbs_seq)
        body = strip_tags(row.get("CONTENTS") or "")

        # The promulgated text / the agenda papers and minutes live in the
        # published documents; the board body is the covering note that states
        # why the instrument changed. Keep both, body first.
        parts: list[str] = []
        if body:
            parts.append(body)
        file_names: list[str] = []
        for idx, doc in enumerate(self._documents(board, row)):
            extracted = self._extract(
                doc["blob"],
                doc["name"],
                f"{board['board_seq']}-{bbs_seq}-{idx}",
                board["table"],
            )
            if not extracted:
                continue
            file_names.append(doc["name"])
            parts.append(clean_text(f"[{doc['name']}]\n{extracted}"))

        text = clean_text("\n\n".join(p for p in parts if p))
        if len(text) < MIN_TEXT:
            return None

        record = {
            "_id": f"{board['board_seq']}-{bbs_seq}",
            "_source": SOURCE_ID,
            "_type": board["doc_type"],
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": subject,
            "text": text,
            "date": date,
            "url": url,
            "language": "ko",
            "country": "KR",
            "authority": "Nuclear Safety and Security Commission (원자력안전위원회)",
            "document_type": board["doc_type"],
            "series": board["label"],
            "board": board["label_en"],
            "department": (row.get("DEPT_NM") or "").strip() or None,
            "attachments": file_names or None,
        }

        m = INSTRUMENT_RE.search(" ".join(file_names) or subject)
        if m:
            record["instrument_type"] = m.group(1)
            record["instrument_number"] = f"제{m.group(2)}호"
        if board["board_seq"] == 14:
            s = SITTING_RE.search(subject)
            if s:
                record["sitting"] = f"제{s.group(1)}회"
            record["has_transcript"] = any("회의록" in n for n in file_names)
            record["has_minutes"] = any("의사록" in n for n in file_names)
        return record

    def normalize(self, raw: dict) -> dict | None:
        return self._normalize_row(raw["board"], raw["row"])

    # ---- iteration ----------------------------------------------------------

    def _enumerate_documents(self) -> Generator[dict, None, None]:
        # Round-robin the boards rather than draining one then the other, so a
        # truncated run (--sample, or a fleet slot that times out) still covers
        # both the legislation and the proceedings side of the corpus.
        streams = [self._enumerate_board(board) for board in BOARDS]
        while streams:
            for stream in list(streams):
                try:
                    yield next(stream)
                except StopIteration:
                    streams.remove(stream)

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._enumerate_documents()

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        cutoff = since
        if isinstance(cutoff, datetime):
            cutoff = cutoff.date().isoformat()
        for raw in self._enumerate_documents():
            row = raw["row"]
            date = iso_date(row.get("SORT") or row.get("WRITE_DATE"))
            if not cutoff or not date or date >= str(cutoff)[:10]:
                yield raw

    # ---- connectivity -------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing nssc.go.kr board API ...")
        try:
            got = 0
            for board in BOARDS:
                rows, total = self._list_page(board, 1)
                logger.info(f"  {board['label']}: {total} postings, {len(rows)} on p1")
                for row in rows:
                    rec = self._normalize_row(board, row)
                    if rec:
                        got += 1
                        logger.info(
                            f"    {rec['title'][:60]!r} "
                            f"({len(rec['text'])} chars, date={rec['date']})"
                        )
                        break
            if got < len(BOARDS):
                logger.error("  Full-text extraction failed for at least one board")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KR/NSSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NSSCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
