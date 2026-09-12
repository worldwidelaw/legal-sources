#!/usr/bin/env python3
"""
KR/KINS -- Korea Institute of Nuclear Safety (한국원자력안전기술원)

Fetches the full text of the technical safety standards and regulatory guides
that KINS, the technical support organisation of the Nuclear Safety and
Security Commission (NSSC), writes to give effect to the Nuclear Safety Act
(원자력안전법). These documents are what a licensee's safety analysis report is
actually reviewed against: the NSSC notices state the requirement, the KINS
regulatory guide states the method the regulator accepts as satisfying it —
the Korean analogue of the US NRC Regulatory Guide series.

Everything is served from **NuSSAM** (원자력안전기준관리시스템, the nuclear
safety standards management system) at ``www.kins.re.kr/nussam``:

  1. **KINS 규제지침 — Regulatory Guides** (``/krs/KinsRgltManualReresvnSts.do``).
     ~210 guides for light-water reactors, organised as a chapter tree
     (site characteristics, radiological environment, design, materials,
     I&C, electrical systems, fuel, accident analysis, initial testing,
     technical specifications, radiation protection, quality assurance …).
     Each guide carries its **full revision history**, and every revision —
     제정 (enactment), 개정 (amendment), 폐지 (repeal) — is a separate born-
     digital PDF. Superseded revisions are kept: the version in force on a
     given date is what a licensing decision of that date was measured
     against.

  2. **KINS 규제기준 — Regulatory Standards** (``/krs/KinsRgltBassReresvnSts.do``).
     The older standards series. It is being wound down — following the
     2016-17 consistency review and the 2018 repeal-and-transfer plan almost
     every chapter has been folded into the guides — so only the chapters
     still awaiting transfer remain live, each again with its full history.

  3. **NuSSAM notices** (``/board/NotiMtrList.do``). The official
     announcements of enactment, amendment and repeal, and the public
     consultation notices on draft guides. These are the only place the
     *reasons* for a change are stated, and the only record of guides that
     have since been deleted from the tree.

Access notes:

  * ``kins.re.kr`` answers only on the ``www`` host (the apex has no A
    record) and rejects non-browser User-Agents outright, so the session
    sends a desktop UA.
  * The guide tree is not paginated — it ships inline in the page as a JSON
    blob in the ``#treeStringValue`` hidden input. The per-guide revision
    table comes from ``/krs/getAjaxLawDetlList.do``, and the PDFs are served
    by ``/common/NussamFileDownLoad.do``, which is **POST-only** (a GET
    returns 405), keyed by the opaque stored filename rather than a URL.
    There is therefore no per-document permalink; ``url`` points at the
    listing the document is reached from.

  KINS guidance interprets and supplements binding law rather than being
  binding law itself, so every record is typed ``doctrine``. The binding
  layer — 원자력안전법, its Enforcement Decree/Rules and the NSSC notices —
  lives in KR/LawGoKr and is deliberately not duplicated here.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import html as html_mod
import json
import logging
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.KINS")

SOURCE_ID = "KR/KINS"
BASE = "https://www.kins.re.kr/nussam"

# The two standards trees. lawTypeCd is carried by the tree nodes themselves;
# the label is what NuSSAM calls the series in its own navigation.
TREES = [
    ("regulatory_guide", "KINS 규제지침", f"{BASE}/krs/KinsRgltManualReresvnSts.do"),
    ("regulatory_standard", "KINS 규제기준", f"{BASE}/krs/KinsRgltBassReresvnSts.do"),
]

DETAIL_URL = f"{BASE}/krs/getAjaxLawDetlList.do"
DOWNLOAD_URL = f"{BASE}/common/NussamFileDownLoad.do"
NOTICE_LIST_URL = f"{BASE}/board/NotiMtrList.do"
NOTICE_DETAIL_URL = f"{BASE}/board/NotiMtrDetl.do"

TREE_VALUE_RE = re.compile(r'id="treeStringValue"[^>]*value="(.*?)"\s*/?>', re.S)
ROW_RE = re.compile(r"<tr>(.*?)</tr>", re.S)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
FILEDOWN_RE = re.compile(r"f_nussamFileDown\('(.*?)','(.*?)','(.*?)'\)")
ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
NOTICE_ID_RE = re.compile(r"f_goDetail\('(\d+)'\)")

# The revision label NuSSAM stores in 개정구분.
CHANGE_KIND = {
    "제정": "enactment",
    "개정": "amendment",
    "폐지": "repeal",
    "삭제": "deletion",
    "전부개정": "full_amendment",
}

# Guide numbers are written "2.1", "18.1" … in the tree label and
# "KINS/RG-N02.01" in the document itself.
RG_CODE_RE = re.compile(r"KINS\s*/\s*(RG|RS)\s*-\s*([A-Z]?\d+\.\d+)", re.I)
GUIDE_NO_RE = re.compile(r"^(\d+\.\d+)\s+(.*)$")

MIN_TEXT = 300  # below this the PDF is a cover sheet, not a guide


def clean_text(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\x00", " ").replace("­", "")
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def strip_tags(fragment: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    # NuSSAM double-encodes board bodies: every space is an &nbsp; entity and
    # punctuation arrives as &#47;/&#40; — one unescape pass leaves the raw
    # numeric forms behind, so run it again.
    text = html_mod.unescape(text)
    text = text.replace(" ", " ")
    return clean_text(text)


def iso_date(value: str) -> str | None:
    value = value.strip()
    m = ISO_DATE_RE.match(value)
    if not m:
        m = re.match(r"^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", value)
    if not m:
        return None
    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1950 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


class KINSScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                # kins.re.kr rejects non-browser agents with a connection reset.
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            }
        )
        self.delay = 0.6

    # ---- low-level fetch ----------------------------------------------------

    def _request(
        self,
        url: str,
        *,
        data: dict | None = None,
        referer: str | None = None,
        retries: int = 3,
        timeout: int = 90,
    ) -> requests.Response | None:
        headers = {}
        if referer:
            headers["Referer"] = referer
        if data is not None:
            headers["X-Requested-With"] = "XMLHttpRequest"
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                if data is None:
                    resp = self.session.get(url, headers=headers, timeout=timeout)
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

    def _get_text(self, url: str) -> str | None:
        resp = self._request(url)
        if resp is None:
            return None
        resp.encoding = resp.encoding or "utf-8"
        return resp.text

    # ---- standards trees ----------------------------------------------------

    def _tree_nodes(self, url: str) -> list[dict]:
        """Every node of a NuSSAM standards tree, inline in the page HTML."""
        page = self._get_text(url)
        if not page:
            raise RuntimeError(
                f"NuSSAM tree page unreachable: {url} — kins.re.kr blocks this "
                "vantage or the page moved; refusing to report an empty corpus"
            )
        m = TREE_VALUE_RE.search(page)
        if not m:
            raise RuntimeError(f"No #treeStringValue on {url} — page layout changed")
        nodes = json.loads(html_mod.unescape(m.group(1)))
        if not nodes:
            raise RuntimeError(f"Empty standards tree on {url}")
        return nodes

    @staticmethod
    def _chapter_path(node: dict, by_seq: dict[str, dict]) -> list[str]:
        """Chapter labels from the tree root down to (but excluding) the node."""
        path: list[str] = []
        seen = {node["lawSeq"]}
        parent = node.get("parent")
        while parent and parent not in seen:
            seen.add(parent)
            up = by_seq.get(parent)
            if up is None:
                break
            path.append(unicodedata.normalize("NFC", up["lawname"]).strip())
            parent = up.get("parent")
        return list(reversed(path))

    def _revisions(self, node: dict, referer: str) -> list[dict]:
        """The revision history table for one guide/standard."""
        resp = self._request(
            DETAIL_URL,
            data={
                "lawSeq": node["lawSeq"],
                "parent": node.get("parent", ""),
                "lawTypeCd": node.get("lawTypeCd", ""),
                "folder": "N",
                "lawname": node["lawname"],
            },
            referer=referer,
        )
        if resp is None:
            return []
        try:
            payload = resp.json()
        except ValueError:
            logger.warning(f"Non-JSON revision list for lawSeq={node['lawSeq']}")
            return []
        table = payload.get("lawDetlList") or ""

        revisions = []
        for row in ROW_RE.findall(table):
            cells = [strip_tags(c) for c in CELL_RE.findall(row)]
            files = FILEDOWN_RE.findall(row)
            if not files:
                continue  # history entry with no document attached
            org_name, stored_name, folder = files[0]
            dates = [d for d in (iso_date(c) for c in cells[2:]) if d]
            revisions.append(
                {
                    "rev_no": cells[0].strip() if cells else "",
                    "change_kind": cells[2].strip() if len(cells) > 2 else "",
                    "date": dates[0] if dates else None,
                    # The guide table carries one date (개정일자); the standard
                    # table adds 공포일자. Only emit what the row actually has.
                    "promulgation_date": dates[1] if len(dates) > 1 else None,
                    "org_name": html_mod.unescape(org_name),
                    "stored_name": html_mod.unescape(stored_name),
                    "folder": html_mod.unescape(folder) or "pdf",
                }
            )
        return revisions

    def _enumerate_standards(self) -> Generator[dict, None, None]:
        for series, series_label, url in TREES:
            nodes = self._tree_nodes(url)
            by_seq = {n["lawSeq"]: n for n in nodes}
            documents = [n for n in nodes if n.get("folder") == "N"]
            logger.info(f"{series_label}: {len(documents)} documents in tree")
            for node in documents:
                chapters = self._chapter_path(node, by_seq)
                for rev in self._revisions(node, url):
                    yield {
                        "kind": "standard",
                        "series": series,
                        "series_label": series_label,
                        "list_url": url,
                        "law_seq": node["lawSeq"],
                        "law_name": unicodedata.normalize(
                            "NFC", node["lawname"]
                        ).strip(),
                        "law_type_cd": node.get("lawTypeCd", ""),
                        "abolished": node.get("abolition") == "Y",
                        "chapters": chapters,
                        **rev,
                    }

    # ---- notices ------------------------------------------------------------

    def _notice_ids(self) -> list[str]:
        ids: list[str] = []
        seen: set[str] = set()
        page_index = 1
        while page_index <= 50:
            resp = self._request(
                NOTICE_LIST_URL,
                data={"currentPage": str(page_index)},
                referer=NOTICE_LIST_URL,
            )
            if resp is None:
                break
            found = [i for i in NOTICE_ID_RE.findall(resp.text) if i not in seen]
            if not found:
                break
            seen.update(found)
            ids.extend(found)
            page_index += 1
        return ids

    def _enumerate_notices(self) -> Generator[dict, None, None]:
        ids = self._notice_ids()
        logger.info(f"NuSSAM notices: {len(ids)} posts")
        for notice_id in ids:
            resp = self._request(
                NOTICE_DETAIL_URL,
                data={"bltnthSeq": notice_id},
                referer=NOTICE_LIST_URL,
            )
            if resp is None:
                continue
            yield {"kind": "notice", "notice_id": notice_id, "html": resp.text}

    @staticmethod
    def _parse_notice(page: str) -> dict | None:
        """Title, date and body out of a NuSSAM board detail page."""
        # The detail view is a definition table: 제목 / 작성자 / 등록일 / 조회수 /
        # 내용 / 첨부파일. Reading it by label survives column reordering.
        rows = {}
        for row in ROW_RE.findall(page):
            cells = CELL_RE.findall(row)
            headers = re.findall(r"<th[^>]*>(.*?)</th>", row, re.S)
            for label, value in zip(headers, cells):
                key = strip_tags(label)
                if key and key not in rows:
                    rows[key] = strip_tags(value)
        title = rows.get("제목")
        body = rows.get("내용")
        if not title or not body:
            return None
        return {
            "title": title,
            "date": iso_date(rows.get("등록일", "")),
            "author": rows.get("작성자"),
            "body": body,
        }

    # ---- normalisation ------------------------------------------------------

    def _download(self, raw: dict) -> bytes | None:
        resp = self._request(
            DOWNLOAD_URL,
            data={
                "saveFileNm": raw["stored_name"],
                "orgFileNm": raw["org_name"],
                "folderNm": raw["folder"],
            },
            referer=raw["list_url"],
            timeout=180,
        )
        if resp is None:
            return None
        body = resp.content
        if not body.startswith(b"%PDF"):
            logger.warning(
                f"Not a PDF for {raw['stored_name']} "
                f"({resp.headers.get('Content-Type')}, {len(body)} bytes)"
            )
            return None
        return body

    def normalize(self, raw: dict) -> dict | None:
        if raw.get("kind") == "notice":
            return self._normalize_notice(raw)
        return self._normalize_standard(raw)

    def _normalize_standard(self, raw: dict) -> dict | None:
        doc_id = f"{raw['series']}-{raw['law_seq']}-r{raw['rev_no'] or '1'}"

        pdf = self._download(raw)
        if pdf is None:
            return None
        text = extract_pdf_markdown(
            SOURCE_ID,
            doc_id,
            pdf_bytes=pdf,
            table="doctrine",
            force=True,
        )
        if not text or len(text.strip()) < MIN_TEXT:
            logger.warning(f"Insufficient text for {doc_id} ({len(text or '')} chars)")
            return None
        text = clean_text(text)

        law_name = raw["law_name"]
        m = GUIDE_NO_RE.match(law_name)
        guide_no, subject = (m.group(1), m.group(2)) if m else (None, law_name)

        # The document states its own code ("KINS/RG-N02.01, Rev. 2") on the
        # first page; prefer it over the tree's short number.
        code_match = RG_CODE_RE.search(text[:2000])
        code = (
            f"KINS/{code_match.group(1).upper()}-{code_match.group(2)}"
            if code_match
            else None
        )

        kind = CHANGE_KIND.get(raw["change_kind"], raw["change_kind"] or None)
        label_bits = [raw["series_label"]]
        if guide_no:
            label_bits.append(guide_no)
        label_bits.append(subject)
        title = " ".join(label_bits)
        if raw["rev_no"]:
            title += f" (Rev. {raw['rev_no']})"

        record = {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw["date"],
            "url": raw["list_url"],
            "language": "ko",
            "country": "KR",
            "authority": "Korea Institute of Nuclear Safety (한국원자력안전기술원)",
            "document_type": raw["series"],
            "series": raw["series_label"],
            "code": code,
            "guide_number": guide_no,
            "subject": subject,
            "revision": raw["rev_no"] or None,
            "change_type": kind,
            "abolished": raw["abolished"],
            "chapter_path": raw["chapters"] or None,
            "file_name": raw["org_name"],
        }
        if raw.get("promulgation_date"):
            record["promulgation_date"] = raw["promulgation_date"]
        return record

    def _normalize_notice(self, raw: dict) -> dict | None:
        parsed = self._parse_notice(raw["html"])
        if not parsed:
            return None
        body = parsed["body"]
        if len(body) < 40:
            return None
        text = clean_text(f"{parsed['title']}\n\n{body}")
        return {
            "_id": f"notice-{raw['notice_id']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"NuSSAM 공지사항: {parsed['title']}",
            "text": text,
            "date": parsed["date"],
            "url": NOTICE_LIST_URL,
            "language": "ko",
            "country": "KR",
            "authority": "Korea Institute of Nuclear Safety (한국원자력안전기술원)",
            "document_type": "notice",
            "series": "NuSSAM 공지사항",
            "author": parsed["author"],
        }

    # ---- iteration ----------------------------------------------------------

    def _enumerate_documents(self) -> Generator[dict, None, None]:
        yield from self._enumerate_standards()
        yield from self._enumerate_notices()

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._enumerate_documents()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self._enumerate_documents():
            if raw.get("kind") == "notice":
                yield raw
                continue
            if not since or not raw.get("date") or raw["date"] >= since:
                yield raw

    # ---- connectivity -------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing NuSSAM connectivity ...")
        try:
            nodes = self._tree_nodes(TREES[0][2])
            documents = [n for n in nodes if n.get("folder") == "N"]
            logger.info(f"  regulatory guides in tree: {len(documents)}")
            got = 0
            for raw in self._enumerate_standards():
                rec = self.normalize(raw)
                if rec:
                    got += 1
                    logger.info(
                        f"  {rec['title'][:70]!r} "
                        f"({len(rec['text'])} chars, date={rec['date']})"
                    )
                if got >= 3:
                    break
            if got < 3:
                logger.error("  Full-text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KR/KINS bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KINSScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
