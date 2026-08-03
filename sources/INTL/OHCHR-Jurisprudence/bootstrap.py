#!/usr/bin/env python3
"""
INTL/OHCHR-Jurisprudence -- UN Treaty Body Jurisprudence
                            (Individual Complaints — Views & Decisions)

Fetches the quasi-judicial output of the UN human-rights treaty bodies deciding
individual communications (complaints) against States: the "Views" (merits) and
"Decisions" (inadmissibility / discontinuance) adopted by the Human Rights
Committee (CCPR), Committee against Torture (CAT), CEDAW, CERD, CRPD, CRC, CED,
CESCR and CMW under their respective Optional Protocols / complaint procedures.
Each is an authoritative interpretation of a core human-rights treaty = case_law.
(Distinct from INTL/OHCHR-TBInternet, which is doctrine: UHRI recommendations /
concluding observations.)

Strategy — the "juris.ohchr.org" front-end is a Blazor Server SPA with no REST
API, BUT the same corpus is fully enumerable through the older, server-rendered
Treaty Body Database search (an ASP.NET WebForms + Telerik RadGrid page):

    https://tbinternet.ohchr.org/_layouts/15/TreatyBodyExternal/TBSearch.aspx

  1. GET TBSearch.aspx to collect the WebForms ViewState / EventValidation.
  2. POST with the "Jurisprudence" DocTypeCategory checked (RadListBox index 5)
     → a RadGrid of decisions (title, committee, State party, UN symbol, date,
     language, filename).
  3. Paginate the RadGrid via its numeric page-link __doPostBack targets, using
     the forward "..." link to advance across page windows; the current page is
     read back from the rgCurrentPage marker.
  4. For each unique decision symbol, GET Download.aspx?symbolno=<symbol> — an
     interstitial that lists every available language/format file with a
     DownloadDraft.aspx?key=<enc> link — pick the English file (prefer .docx,
     then .pdf), download it and extract the full text (docx: word/document.xml;
     pdf: PyMuPDF/fitz).

All endpoints are reachable and unauthenticated. UN treaty-body documents are
public official documents of an intergovernmental organization.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records only
  python bootstrap.py bootstrap-fast     # Alias for full bootstrap (VPS pipeline)
  python bootstrap.py update             # Re-scan (idempotent via storage dedup)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import zipfile
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OHCHR-Jurisprudence")

BASE = "https://tbinternet.ohchr.org"
SEARCH_URL = BASE + "/_layouts/15/TreatyBodyExternal/TBSearch.aspx?Lang=en"
DOWNLOAD_URL = BASE + "/_layouts/15/treatybodyexternal/Download.aspx"
DRAFT_URL = BASE + "/_layouts/15/TreatyBodyExternal/DownloadDraft.aspx"

# "Jurisprudence" is index 5 in the DocTypeCategory RadListBox.
JURIS_CATEGORY_INDEX = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
}

COMMITTEES = {
    "CCPR": "Human Rights Committee",
    "CAT": "Committee against Torture",
    "CAT-OP": "Subcommittee on Prevention of Torture",
    "CED": "Committee on Enforced Disappearances",
    "CEDAW": "Committee on the Elimination of Discrimination against Women",
    "CERD": "Committee on the Elimination of Racial Discrimination",
    "CESCR": "Committee on Economic, Social and Cultural Rights",
    "CMW": "Committee on Migrant Workers",
    "CRC": "Committee on the Rights of the Child",
    "CRPD": "Committee on the Rights of Persons with Disabilities",
}

_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
     "Nov", "Dec"], start=1)}

MIN_TEXT_CHARS = 400
MAX_PAGES = 600  # safety cap (well above the ~200 expected pages)


class OHCHRJurisprudenceScraper(BaseScraper):
    """
    Scraper for INTL/OHCHR-Jurisprudence.
    Country: INTL
    URL: https://juris.ohchr.org/  (enumerated via tbinternet.ohchr.org)
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── WebForms helpers ───────────────────────────────────────────
    @staticmethod
    def _form_fields(html: str) -> dict:
        """Collect all non-submit <input> name/value pairs (ViewState etc.)."""
        d = {}
        for m in re.finditer(r"<input\b[^>]*>", html):
            tag = m.group(0)
            nm = re.search(r'name="([^"]*)"', tag)
            if not nm:
                continue
            ty = re.search(r'type="([^"]*)"', tag)
            if ty and ty.group(1) in ("submit", "button", "image", "reset"):
                continue
            val = re.search(r'value="([^"]*)"', tag)
            d[unescape(nm.group(1))] = unescape(val.group(1)) if val else ""
        return d

    def _search_first_page(self) -> str:
        """Run the Jurisprudence search and return the first results page HTML."""
        html = self.session.get(SEARCH_URL, timeout=60).text
        data = self._form_fields(html)
        cs = json.dumps({
            "logEntries": [], "value": "", "text": "", "enabled": True,
            "checkedIndices": [JURIS_CATEGORY_INDEX],
            "checkedItemsTextOverflows": False,
        })
        data["ctl00_ContentPlaceHolder1_rlbDocTypeCategory_ClientState"] = cs
        data["ctl00$ContentPlaceHolder1$btnSearch"] = "Search"
        data["__EVENTTARGET"] = ""
        data["__EVENTARGUMENT"] = ""
        r = self.session.post(SEARCH_URL, data=data, timeout=120)
        r.raise_for_status()
        return r.text

    @staticmethod
    def _current_page(html: str) -> Optional[int]:
        m = re.search(r"rgPagerCell.{0,7000}", html, re.S)
        if not m:
            return None
        seg = unescape(m.group(0))
        cm = re.search(r"rgCurrentPage[^>]*>\s*(?:<[^>]*>\s*)*(\d+)", seg)
        return int(cm.group(1)) if cm else None

    @staticmethod
    def _page_targets(html: str) -> dict:
        """Map visible numeric page label -> __doPostBack target."""
        m = re.search(r"rgPagerCell.{0,7000}", html, re.S)
        if not m:
            return {}
        seg = unescape(m.group(0))
        out = {}
        for tgt, label in re.findall(
                r"__doPostBack\('([^']+)','[^']*'\)\">\s*(?:<[^>]*>\s*)*(\d+)", seg):
            out.setdefault(label, tgt)
        return out

    @staticmethod
    def _forward_ellipsis(html: str) -> Optional[str]:
        m = re.search(r"rgPagerCell.{0,7000}", html, re.S)
        if not m:
            return None
        seg = unescape(m.group(0))
        ells = re.findall(
            r"__doPostBack\('([^']+)','[^']*'\)\">\s*(?:<[^>]*>\s*)*\.\.\.", seg)
        return ells[-1] if ells else None

    def _goto_target(self, html: str, target: str) -> str:
        data = self._form_fields(html)
        data["__EVENTTARGET"] = target
        data["__EVENTARGUMENT"] = ""
        r = self.session.post(SEARCH_URL, data=data, timeout=120)
        r.raise_for_status()
        return r.text

    # ── result-grid parsing ────────────────────────────────────────
    def _parse_rows(self, html: str) -> list[dict]:
        """Parse the RadGrid data rows on one results page."""
        rows = []
        for rm in re.finditer(
                r'<tr[^>]*class="rg(?:Row|AltRow)[^"]*"[^>]*>(.*?)</tr>',
                html, re.S):
            row_html = rm.group(1)
            link = re.search(r'symbolno=([^"&]+)', row_html)
            if not link:
                continue
            symbol = unescape(urllib.parse.unquote(link.group(1)))
            cells = [re.sub(r"<[^>]+>", " ", unescape(c)).strip()
                     for c in re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.S)]
            cells = [re.sub(r"\s+", " ", c) for c in cells]

            def cell(i):
                return cells[i] if i < len(cells) else ""

            title = cell(0)
            committee = cell(2)
            state = cell(3)
            date_raw = cell(5)
            lang = cell(7)
            filename = next((c for c in cells
                             if re.search(r"\.(docx|pdf|doc)$", c, re.I)), "")
            rows.append({
                "symbol": symbol,
                "title": title,
                "committee": committee,
                "state": state,
                "date_raw": date_raw,
                "lang": lang,
                "filename": filename,
            })
        return rows

    @staticmethod
    def _parse_date(raw: str) -> Optional[str]:
        """'19 Jul 2024' -> '2024-07-19'."""
        if not raw:
            return None
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})", raw)
        if not m:
            return None
        day = int(m.group(1))
        mon = _MONTHS.get(m.group(2)[:3].title())
        year = int(m.group(3))
        if mon and 1 <= day <= 31 and 1980 <= year <= 2100:
            return f"{year:04d}-{mon:02d}-{day:02d}"
        return None

    # ── full-text download ─────────────────────────────────────────
    def _download_text(self, symbol: str) -> tuple[str, str, str]:
        """Return (text, file_url, filename) for the English file of a symbol."""
        url = (DOWNLOAD_URL + "?symbolno="
               + urllib.parse.quote(symbol, safe="") + "&Lang=en")
        try:
            r = self.session.get(url, timeout=90)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"  Download.aspx failed for {symbol}: {e}")
            return "", "", ""
        html = r.text

        files = []  # (language, fmt, filename, key)
        for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
            if "DownloadDraft" not in rm.group(1):
                continue
            keys = re.findall(r"DownloadDraft\.aspx\?key=([^\"]+)", rm.group(1))
            if not keys:
                continue
            cells = [re.sub(r"<[^>]+>", " ", unescape(c)).strip()
                     for c in re.findall(r"<td[^>]*>(.*?)</td>", rm.group(1), re.S)]
            cells = [c for c in cells if c]
            language = cells[0] if cells else ""
            fmt = next((c for c in cells if c.lower() in
                        (".docx", ".pdf", ".doc", ".html")), "")
            fname = next((c for c in cells
                          if re.search(r"\.(docx|pdf|doc|html)$", c, re.I)), "")
            files.append((language, fmt.lower(), fname, unescape(keys[0])))

        if not files:
            return "", "", ""

        fmt_rank = {".docx": 0, ".pdf": 1, ".doc": 2, ".html": 3}

        def score(f):
            lang_pref = 0 if f[0].strip().lower() == "english" else 1
            return (lang_pref, fmt_rank.get(f[1], 9))

        files.sort(key=score)
        language, fmt, fname, key = files[0]

        draft = DRAFT_URL + "?key=" + urllib.parse.quote(key, safe="")
        try:
            dr = self.session.get(draft, timeout=120)
            dr.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"  DownloadDraft failed for {symbol}: {e}")
            return "", draft, fname
        content = dr.content
        disp = dr.headers.get("content-disposition", "")
        dm = re.search(r'filename=([^\s;]+)', disp)
        if dm:
            fname = dm.group(1).strip('"')

        text = self._extract(content, fname or fmt)
        return text, draft, fname

    @staticmethod
    def _extract(content: bytes, hint: str) -> str:
        hint = (hint or "").lower()
        try:
            if hint.endswith(".docx") or content[:2] == b"PK":
                return OHCHRJurisprudenceScraper._extract_docx(content)
            if hint.endswith(".pdf") or content[:5] == b"%PDF-":
                return OHCHRJurisprudenceScraper._extract_pdf(content)
            if hint.endswith(".html") or hint.endswith(".htm"):
                t = re.sub(r"<[^>]+>", " ", content.decode("utf-8", "replace"))
                return re.sub(r"\s+", " ", unescape(t)).strip()
        except Exception as e:
            logger.warning(f"  extract failed ({hint}): {e}")
        return ""

    @staticmethod
    def _extract_docx(content: bytes) -> str:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            xml = z.read("word/document.xml").decode("utf-8", "replace")
        # paragraphs -> newlines, tabs -> spaces
        xml = re.sub(r"</w:p>", "\n", xml)
        xml = re.sub(r"<w:tab[^>]*/>", " ", xml)
        parts = re.findall(r"<w:t(?:\s[^>]*)?>(.*?)</w:t>", xml, re.S)
        text = unescape("".join(parts))
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _extract_pdf(content: bytes) -> str:
        import fitz
        parts = []
        with fitz.open(stream=content, filetype="pdf") as doc:
            for page in doc:
                parts.append(page.get_text())
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── schema ─────────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None
        symbol = raw.get("symbol", "").strip()
        if not symbol:
            return None
        committee_code = raw.get("committee", "").strip()
        return {
            "_id": "OHCHR-" + re.sub(r"[^0-9A-Za-z]+", "-", symbol).strip("-"),
            "_source": "INTL/OHCHR-Jurisprudence",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or symbol,
            "text": text,
            "date": self._parse_date(raw.get("date_raw", "")),
            "url": raw.get("file_url") or (
                "https://juris.ohchr.org/casedetails/"),
            "symbol": symbol,
            "committee": COMMITTEES.get(committee_code, committee_code),
            "committee_code": committee_code,
            "state_party": raw.get("state", "").strip(),
            "filename": raw.get("filename", ""),
            "language": "English",
            "jurisdiction": "United Nations",
        }

    # ── fetch ──────────────────────────────────────────────────────
    def _iter_rows(self) -> Generator[dict, None, None]:
        """Paginate the Jurisprudence search grid, yielding raw metadata rows."""
        html = self._search_first_page()
        page = 1
        while page <= MAX_PAGES:
            rows = self._parse_rows(html)
            logger.info(f"  page {page}: {len(rows)} rows")
            for r in rows:
                yield r
            # advance to next page
            targets = self._page_targets(html)
            nxt = str(page + 1)
            tgt = targets.get(nxt) or self._forward_ellipsis(html)
            if not tgt:
                break
            html = self._goto_target(html, tgt)
            time.sleep(1.0)
            cur = self._current_page(html)
            if cur is None or cur <= page:
                break  # did not advance — stop
            page = cur
        logger.info(f"Pagination finished at page {page}")

    def fetch_all(self) -> Generator[dict, None, None]:
        seen: set[str] = set()
        # Group language rows per symbol as we stream; English preferred at the
        # download stage, so one download per unique symbol is enough.
        yielded = 0
        for row in self._iter_rows():
            symbol = row["symbol"]
            if symbol in seen:
                continue
            seen.add(symbol)
            text, file_url, fname = self._download_text(symbol)
            if len(text) < MIN_TEXT_CHARS:
                logger.info(f"  {symbol}: insufficient text ({len(text)}) — skip")
                time.sleep(0.5)
                continue
            row["text"] = text
            row["file_url"] = file_url
            if fname:
                row["filename"] = fname
            yield row
            yielded += 1
            if yielded % 25 == 0:
                logger.info(f"  yielded {yielded} decisions so far")
            time.sleep(1.0)
        logger.info(f"Finished: {yielded} unique decisions with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No per-item modified date; re-scan (storage dedup handles idempotency)."""
        yield from self.fetch_all()


def main():
    args = sys.argv[1:]
    cmd = args[0] if args else "bootstrap"

    scraper = OHCHRJurisprudenceScraper()

    if cmd == "test":
        html = scraper._search_first_page()
        rows = scraper._parse_rows(html)
        logger.info(f"Search OK: {len(rows)} rows on page 1; "
                    f"current page={scraper._current_page(html)}")
        if rows:
            r = rows[0]
            logger.info(f"First row: {r['symbol']} | {r['committee']} | "
                        f"{r['state']} | {r['date_raw']}")
            text, url, fname = scraper._download_text(r["symbol"])
            logger.info(f"Downloaded {fname}: {len(text)} chars; "
                        f"preview: {text[:200]!r}")
        return

    if cmd in ("bootstrap", "bootstrap-fast", "update"):
        sample = "--sample" in args
        size = 12
        for i, a in enumerate(args):
            if a == "--sample-size" and i + 1 < len(args):
                size = int(args[i + 1])
        stats = scraper.bootstrap(sample_mode=sample, sample_size=size)
        logger.info(f"Complete: {json.dumps(stats, indent=2, default=str)}")
        return

    print(f"Unknown command: {cmd}")
    sys.exit(1)


if __name__ == "__main__":
    main()
