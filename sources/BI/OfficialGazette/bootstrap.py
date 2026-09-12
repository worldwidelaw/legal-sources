#!/usr/bin/env python3
"""
BI/OfficialGazette -- Burundi Official Gazette (Bulletin Officiel du Burundi)

Fetches Burundi laws, decrees and ordonnances with FULL TEXT from the official
government portal amategeko.gov.bi (Service chargé de la législation).

Strategy:
  - Enumerate individual acts via the WordPress sitemap for the custom post
    type `laws_and_other_acts` (~2000 URLs).
  - Each act detail page is metadata-only (title, BOB number, status, language)
    plus a link to the consolidated Bulletin Officiel du Burundi (BOB) PDF.
  - The BOB PDFs have SELECTABLE text (not scanned). The full text of an
    individual act is the section delimited by its heading
    (e.g. "LOI N°1/04 DU 29/01/2018 ...") and the next act's heading.
  - Texts are published bilingually (French / Kirundi).

Note: the site's /wp-json/ REST API is blocked by a security plugin (HTTP 420),
so the public sitemap + HTML detail pages + PDF extraction is used instead.

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import html as htmlmod
import io
import json
import logging
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BI.OfficialGazette")

BASE_URL = "https://amategeko.gov.bi"
SITEMAP_URL = f"{BASE_URL}/wp-sitemap-posts-laws_and_other_acts-1.xml"

# Heading of an act inside a BOB PDF, e.g.:
#   "LOI N°1/04 DU 29/01/2018 PORTANT ..."
#   "DECRET N°100/197 DU 25 SEPTEMBRE 2023 ..."
#   "ORDONNANCE MINISTERIELLE N°610/1194 DU 18/09/2023 ..."
#   "DECISION N°553/386/26/2022 DU ..."
HEADING_RE = re.compile(
    r"(?m)^[ \t]*"
    r"(LOI|D[ÉE]CRET|ORDONNANCE(?:\s+MINIST[ÉE]RIELLE)?|ARR[ÊE]T[ÉE]|"
    r"D[ÉE]CISION|R[ÈE]GLEMENT)\s+"
    r"N[°ºoO]\s*([0-9][0-9./ -]*?)\s+DU\b"
)

# Act type/number parsed from the detail-page title, e.g. "Loi N°1/04 du 29/01/2018 portant ..."
# Handles both "Décret N°100/003 du ..." and "Décret 100/003 du ..." (no N° prefix).
# Requires " du " after the number to avoid matching dates.
TITLE_NUM_RE = re.compile(
    r"^\s*(Loi|D[ée]cret|Ordonnance(?:[-\s]+minist[ée]rielle)?|Arr[êe]t[ée]|"
    r"D[ée]cision|R[èe]glement|Circulaire)\b"
    r".*?"
    r"(?:N[°ºoO]\s*)?"
    r"(\d+(?:\s*/\s*\d+)*)"
    r"\s+du\b",
    re.IGNORECASE | re.DOTALL,
)
TITLE_DATE_RE = re.compile(r"\bdu\s+(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})\b", re.IGNORECASE)

_FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}
TITLE_DATE_FR_RE = re.compile(
    r"\bdu\s+(\d{1,2})(?:er)?\s+([a-zûéùA-ZÛÉÙ]+)\s+(\d{4})\b", re.IGNORECASE
)

# Trailing colophon printed at the very end of each BOB issue.
COLOPHON_RE = re.compile(
    r"\n\s*Pour tout renseignement relatif au Bulletin Officiel.*$",
    re.IGNORECASE | re.DOTALL,
)

PDF_LINK_RE = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)
H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>", re.IGNORECASE | re.DOTALL)
FIELD_RE = re.compile(
    r'<div class="title">\s*(.*?)\s*</div>\s*<div class="text">\s*(.*?)\s*</div>',
    re.IGNORECASE | re.DOTALL,
)


def _normalize_act_number(num: str) -> str:
    """Normalize an act number for matching: strip spaces and leading zeros per segment.

    "1/04" -> "1/4", "100/003" -> "100/3", "553/386/26/2022" -> "553/386/26/2022".
    """
    if not num:
        return ""
    parts = re.split(r"[/]", num.strip())
    out = []
    for p in parts:
        p = p.strip()
        p = re.sub(r"\s+", "", p)
        # strip leading zeros but keep at least one digit
        p2 = p.lstrip("0") or "0"
        out.append(p2)
    return "/".join(out)


def _normalize_type(t: str) -> str:
    t = t.upper()
    t = (
        t.replace("É", "E")
        .replace("È", "E")
        .replace("Ê", "E")
        .replace("DÉCRET", "DECRET")
    )
    if t.startswith("ORDONNANCE"):
        return "ORDONNANCE"
    if t.startswith("ARR"):
        return "ARRETE"
    if t.startswith("DECISION") or t.startswith("DÉCISION"):
        return "DECISION"
    if t.startswith("REGLEMENT") or t.startswith("RÈGLEMENT"):
        return "REGLEMENT"
    if t.startswith("DECRET"):
        return "DECRET"
    if t.startswith("LOI"):
        return "LOI"
    if t.startswith("CIRCULAIRE"):
        return "CIRCULAIRE"
    return t.strip()


def _clean_pdf_text(text: str) -> str:
    """Strip BOB page headers/footers and standalone gazette page numbers."""
    lines = []
    for ln in text.split("\n"):
        s = ln.strip()
        # page header/footer: "BOB N° 9 BIS/2023"
        if re.match(r"(?i)^BOB\s*N[°ºoO]", s):
            continue
        # standalone gazette page number (continuous numbering, in the thousands)
        if re.fullmatch(r"\d{4}", s) and 1000 <= int(s) <= 9999:
            continue
        lines.append(ln)
    out = "\n".join(lines)
    out = COLOPHON_RE.sub("", out)
    out = re.sub(r"[ \t]{2,}", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


class OfficialGazetteScraper(BaseScraper):
    """Scraper for BI/OfficialGazette -- Burundi Bulletin Officiel."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })
        # LRU cache of parsed BOB PDFs: url -> (full_text, segments, gazette_page_offsets)
        self._pdf_cache: "OrderedDict[str, Tuple[str, list, dict]]" = OrderedDict()
        self._pdf_cache_max = 8

    # ----- HTTP -----
    def _request(self, url: str, timeout: int = 60, binary: bool = False):
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(8)
        return None

    # ----- Enumeration -----
    def _list_act_urls(self) -> List[str]:
        resp = self._request(SITEMAP_URL)
        if resp is None:
            return []
        urls = re.findall(r"<loc>\s*([^<]+?)\s*</loc>", resp.text)
        return [u for u in urls if "/laws_and_other_acts/" in u]

    # ----- Detail page parsing -----
    def _parse_detail(self, html: str, url: str) -> Optional[Dict[str, Any]]:
        # Title: the content <h2> (skip nav). Pick the longest h2 (the act title).
        h2s = [htmlmod.unescape(re.sub(r"<[^>]+>", " ", m.group(1))).strip()
               for m in H2_RE.finditer(html)]
        h2s = [re.sub(r"\s+", " ", t) for t in h2s if t]
        title = max(h2s, key=len) if h2s else ""
        if not title:
            return None

        fields: Dict[str, str] = {}
        for m in FIELD_RE.finditer(html):
            label = htmlmod.unescape(re.sub(r"<[^>]+>", " ", m.group(1))).strip()
            value = htmlmod.unescape(re.sub(r"<[^>]+>", " ", m.group(2))).strip()
            value = re.sub(r"\s+", " ", value)
            if label:
                fields[label.lower()] = value

        pdf_m = PDF_LINK_RE.search(html)
        pdf_url = pdf_m.group(1) if pdf_m else ""
        if pdf_url.startswith("//"):
            pdf_url = "https:" + pdf_url
        gazette_page = None
        if "#page=" in pdf_url:
            try:
                gazette_page = int(pdf_url.split("#page=")[1].split("&")[0])
            except (ValueError, IndexError):
                gazette_page = None
        pdf_clean = pdf_url.split("#")[0]

        # act type + number from title
        act_type = act_number = None
        tm = TITLE_NUM_RE.search(title)
        if tm:
            act_type = tm.group(1)
            act_number = tm.group(2).strip()

        # date from title (dd/mm/yyyy or "dd <month> yyyy") -> ISO
        date_iso = ""
        dm = TITLE_DATE_RE.search(title)
        if dm:
            d, mth, y = dm.group(1), dm.group(2), dm.group(3)
            date_iso = f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"
        else:
            fm = TITLE_DATE_FR_RE.search(title)
            if fm and fm.group(2).lower() in _FR_MONTHS:
                d, mth, y = fm.group(1), _FR_MONTHS[fm.group(2).lower()], fm.group(3)
                date_iso = f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"

        return {
            "url": url,
            "title": title,
            "act_type": act_type,
            "act_number": act_number,
            "date": date_iso,
            "bob_number": fields.get("numéro du bob") or fields.get("numero du bob", ""),
            "status": fields.get("statut de l'acte", ""),
            "language": fields.get("langue", ""),
            "description": fields.get("brève description") or fields.get("breve description", ""),
            "pdf_url": pdf_clean,
            "gazette_page": gazette_page,
        }

    # ----- PDF parsing & segmentation -----
    def _get_pdf(self, pdf_url: str) -> Optional[Tuple[str, list, dict]]:
        if pdf_url in self._pdf_cache:
            self._pdf_cache.move_to_end(pdf_url)
            return self._pdf_cache[pdf_url]
        if fitz is None:
            logger.error("PyMuPDF (fitz) not available")
            return None
        resp = self._request(pdf_url, timeout=120)
        if resp is None:
            return None
        try:
            doc = fitz.open(stream=io.BytesIO(resp.content), filetype="pdf")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Cannot open PDF {pdf_url}: {e}")
            return None

        page_texts = []
        gazette_offsets: Dict[int, int] = {}
        cursor = 0
        for i in range(doc.page_count):
            t = doc[i].get_text()
            page_texts.append(t)
            # gazette page marker: a standalone 4-digit number (thousands) on the page
            for gm in re.findall(r"(?m)^\s*(\d{4})\s*$", t):
                gp = int(gm)
                if 1000 <= gp <= 9999 and gp not in gazette_offsets:
                    gazette_offsets[gp] = cursor
            cursor += len(t) + 1  # +1 for the join newline
        doc.close()

        full_text = "\n".join(page_texts)

        segments = []  # list of (start, end, type_norm, number_norm, heading)
        matches = list(HEADING_RE.finditer(full_text))
        for idx, m in enumerate(matches):
            start = m.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(full_text)
            segments.append((
                start,
                end,
                _normalize_type(m.group(1)),
                _normalize_act_number(m.group(2)),
                re.sub(r"\s+", " ", m.group(0)).strip(),
            ))

        result = (full_text, segments, gazette_offsets)
        self._pdf_cache[pdf_url] = result
        self._pdf_cache.move_to_end(pdf_url)
        while len(self._pdf_cache) > self._pdf_cache_max:
            self._pdf_cache.popitem(last=False)
        return result

    def _extract_act_text(self, meta: Dict[str, Any]) -> str:
        pdf_url = meta.get("pdf_url")
        if not pdf_url:
            return ""
        parsed = self._get_pdf(pdf_url)
        if parsed is None:
            return ""
        full_text, segments, gazette_offsets = parsed
        if not segments:
            return ""

        # Primary: match by normalized act number (+ type as tiebreaker).
        target_num = _normalize_act_number(meta.get("act_number") or "")
        target_type = _normalize_type(meta.get("act_type") or "") if meta.get("act_type") else ""
        if target_num:
            num_matches = [s for s in segments if s[3] == target_num]
            if len(num_matches) > 1 and target_type:
                tm = [s for s in num_matches if s[2] == target_type]
                if tm:
                    num_matches = tm
            if num_matches:
                s = num_matches[0]
                return _clean_pdf_text(full_text[s[0]:s[1]])

        # Fallback: gazette page -> closest heading segment.
        gp = meta.get("gazette_page")
        if gp and gazette_offsets:
            # nearest known gazette page offset (handle off-by-one anchors)
            candidates = [p for p in (gp, gp + 1, gp - 1, gp + 2) if p in gazette_offsets]
            if candidates:
                offset = gazette_offsets[candidates[0]]
                # segment whose heading start is closest to that offset
                best = min(segments, key=lambda s: abs(s[0] - offset))
                return _clean_pdf_text(full_text[best[0]:best[1]])

        # Last resort: if the BOB has a single act, return it.
        if len(segments) == 1:
            s = segments[0]
            return _clean_pdf_text(full_text[s[0]:s[1]])
        return ""

    # ----- Normalize -----
    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("_id", ""),
            "_source": "BI/OfficialGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "act_type": raw.get("act_type", ""),
            "act_number": raw.get("act_number", ""),
            "bob_number": raw.get("bob_number", ""),
            "status": raw.get("status", ""),
            "language": raw.get("language", "fr"),
            "description": raw.get("description", ""),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def _slug_id(self, url: str) -> str:
        slug = url.rstrip("/").split("/")[-1]
        return f"BI-{slug}"[:200]

    # ----- Generators -----
    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        urls = self._list_act_urls()
        if not urls:
            logger.error("No act URLs found in sitemap")
            return
        logger.info(f"Sitemap: {len(urls)} acts listed")

        count = 0
        for url in urls:
            if max_records and count >= max_records:
                return
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch detail: {url}")
                continue
            meta = self._parse_detail(resp.text, url)
            if not meta:
                logger.warning(f"Could not parse detail page: {url}")
                continue
            text = self._extract_act_text(meta)
            if not text or len(text) < 200:
                logger.warning(
                    f"Insufficient text ({len(text)} chars) for {meta.get('title','?')[:70]}"
                )
                continue
            raw = dict(meta)
            raw["_id"] = self._slug_id(url)
            raw["text"] = text
            record = self.normalize(raw)
            count += 1
            yield record

        logger.info(f"Completed: {count} acts fetched with full text")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # The sitemap is ordered most-recent-first; sample the front of it.
        yield from self.fetch_all(max_records=30)

    # ----- Test -----
    def test(self) -> bool:
        urls = self._list_act_urls()
        if not urls:
            logger.error("Cannot fetch act sitemap")
            return False
        logger.info(f"Sitemap OK: {len(urls)} acts")
        resp = self._request(urls[0])
        if not resp:
            logger.error("Cannot fetch first detail page")
            return False
        meta = self._parse_detail(resp.text, urls[0])
        logger.info(f"Detail OK: {meta.get('title','?')[:70]} pdf={bool(meta.get('pdf_url'))}")
        text = self._extract_act_text(meta) if meta else ""
        logger.info(f"Full text: {len(text)} chars")
        return bool(text)


def main():
    parser = argparse.ArgumentParser(description="BI/OfficialGazette data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OfficialGazetteScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if args.sample else None
        for record in scraper.fetch_all(max_records=max_records):
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:75]} "
                f"({len(record.get('text', '')):,} chars)"
            )
            count += 1
        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
