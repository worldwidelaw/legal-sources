#!/usr/bin/env python3
"""
IT/Bolzano -- South Tyrol Provincial Legislation (LexBrowser Bolzano/Bozen)

Consolidated provincial legislation of the Autonomous Province of
Bolzano/Südtirol (Provincia autonoma di Bolzano - Alto Adige) from the official
LexBrowser system (https://lexbrowser.provinz.bz.it). Covers provincial laws
(leggi provinciali), decrees of the President (decreti del Presidente della
Provincia) and regulations (regolamenti) from 1946 to present.

Strategy:
  - Enumerate via the chronological index /chrono/it/{YYYY}/ (1946-present).
    Each year page lists every act as an anchor
        /doc/it/{docId}/{slug}.aspx
    where the slug encodes the act type, date and number, e.g.
        legge_provinciale_3_gennaio_2020_n_1
        decreto_del_presidente_della_provincia_8_gennaio_2020_n_2
        regolamento_...
    We keep the clean-legislation types (legge_provinciale,
    decreto_del_presidente, regolamento) and skip delibere / contratti / court
    decisions.
  - Full text: GET /doc/it/{docId}/{slug}.aspx and extract the inner
        <div id="documento" class="documentoesteso"> ... </div>
    which holds the consolidated article text.

THROTTLE HANDLING: the doc endpoint has an aggressive per-IP request queue.
A queued/blocked request 302-redirects to /TooManyRequests.aspx (whose body is a
meta-refresh shell). We warm a session against the chrono index (which sets the
throttle cookie), then poll the doc URL with polite backoff, honoring the
~3-second meta-refresh, until the real body arrives.

fetch_all() yields RAW metadata dicts (one lightweight network call per year for
enumeration); normalize() performs the per-document full-text download so
BaseScraper.bootstrap_fast can parallelize it.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # full pull, concurrent downloads
  python bootstrap.py update 2024          # acts of a given year onward
  python bootstrap.py test                 # connectivity/parse test
"""

import re
import sys
import json
import time
import logging
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Bolzano")

BASE = "https://lexbrowser.provinz.bz.it"
CHRONO = BASE + "/chrono/it/{year}/"
FIRST_YEAR = 1946

# Slug prefixes that identify a clean-legislation act (vs. delibere, contratti,
# court decisions, etc.).
LEGISLATION_PREFIXES = (
    "legge_provinciale",
    "legge_regionale",
    "decreto_del_presidente",
    "decreto_legislativo",
    "regolamento",
    "testo_unico",
    "statuto",
)

TYPE_LABELS = {
    "legge_provinciale": "Legge provinciale",
    "legge_regionale": "Legge regionale",
    "decreto_del_presidente": "Decreto del Presidente della Provincia",
    "decreto_legislativo": "Decreto legislativo",
    "regolamento": "Regolamento",
    "testo_unico": "Testo unico",
    "statuto": "Statuto",
}

MONTHS_IT = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
}

DOC_RE = re.compile(r'href="(/doc/it/([^/"]+)/([^"?]+?)\.aspx)(\?[^"]*)?"')


class BolzanoScraper(BaseScraper):
    SOURCE_ID = "IT/Bolzano"

    def __init__(self, source_dir=None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "it,de;q=0.8",
        })
        self._warmed = False

    # ── HTTP helpers ──────────────────────────────────────────────────
    def _warm(self):
        """Prime the throttle cookie by hitting a chrono index page once."""
        if self._warmed:
            return
        try:
            self.session.get(CHRONO.format(year=2020), timeout=60)
            self._warmed = True
        except requests.RequestException as e:
            logger.warning("Session warm-up failed: %s", e)

    def _get_index(self, url: str) -> Optional[str]:
        """Fetch a chrono/index page (these are not throttle-gated)."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 200:
                    resp.encoding = resp.apparent_encoding or "utf-8"
                    return resp.text
                logger.warning("Index %s -> HTTP %s", url, resp.status_code)
            except requests.RequestException as e:
                logger.warning("Index GET attempt %d failed for %s: %s", attempt + 1, url, e)
            time.sleep(2 ** attempt)
        return None

    def _get_doc(self, url: str, max_polls: int = 12) -> Optional[str]:
        """Fetch a document page, polling through the throttle queue."""
        self._warm()
        for attempt in range(max_polls):
            try:
                resp = self.session.get(url, timeout=60, allow_redirects=False)
            except requests.RequestException as e:
                logger.warning("Doc GET failed for %s: %s", url, e)
                time.sleep(4)
                continue

            # Throttled -> 302 to /TooManyRequests.aspx
            if resp.status_code in (301, 302, 303):
                loc = resp.headers.get("Location", "")
                if "TooManyRequests" in loc:
                    time.sleep(4)
                    continue
                # A real redirect (rare) -> follow once
                nxt = loc if loc.startswith("http") else BASE + loc
                url = nxt
                time.sleep(1)
                continue

            if resp.status_code != 200:
                logger.warning("Doc %s -> HTTP %s", url, resp.status_code)
                time.sleep(3)
                continue

            resp.encoding = resp.apparent_encoding or "utf-8"
            text = resp.text
            # Throttle shell served with 200: small body + meta-refresh
            if "Troppe richieste" in text or ("TooManyRequests" in text and len(text) < 40000):
                time.sleep(4)
                continue
            return text
        logger.error("Throttle never cleared for %s", url)
        return None

    # ── Body extraction ───────────────────────────────────────────────
    @staticmethod
    def _extract_body(html: str) -> str:
        marker = html.find('id="documento"')
        if marker < 0:
            marker = html.find('class="documentoesteso"')
        if marker < 0:
            return ""
        open_tag = html.rfind("<div", 0, marker + 1)
        if open_tag < 0:
            return ""
        depth = 0
        end = None
        for m in re.finditer(r"<div\b|</div>", html[open_tag:]):
            if m.group() == "<div":
                depth += 1
            else:
                depth -= 1
                if depth == 0:
                    end = open_tag + m.end()
                    break
        div = html[open_tag:end] if end else html[open_tag:]
        div = re.sub(r"<script.*?</script>", " ", div, flags=re.S)
        div = re.sub(r"<style.*?</style>", " ", div, flags=re.S)
        div = re.sub(r"<[^>]+>", " ", div)
        text = _html.unescape(div)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        # Drop a trailing "Top" back-to-top link if present
        text = re.sub(r"\s*Top\s*$", "", text).strip()
        return text

    @staticmethod
    def _extract_title(html: str) -> str:
        m = re.search(r'class="titolodocumento"[^>]*>(.*?)</', html, re.S)
        if m:
            t = re.sub(r"<[^>]+>", " ", m.group(1))
            t = _html.unescape(t)
            t = re.sub(r"\s+", " ", t).strip()
            # strip stray leading javascript artefacts (e.g. w'') / '') that the
            # tit_doc span emits before the real title
            t = re.sub(
                r"^.*?(?=Legge|Decreto|Regolamento|Testo unico|Statuto|Deliberazione)",
                "", t, count=1,
            )
            # drop a trailing footnote marker like " 1)"
            t = re.sub(r"\s+\d+\)\s*$", "", t).strip()
            return t
        return ""

    # ── Slug parsing ──────────────────────────────────────────────────
    @staticmethod
    def _parse_slug(slug: str):
        """Return (type_key, label, date_iso, number) from an act slug."""
        type_key = None
        for pref in LEGISLATION_PREFIXES:
            if slug.startswith(pref):
                type_key = pref
                break
        label = TYPE_LABELS.get(type_key, "Atto")
        date_iso = None
        number = None
        dm = re.search(r"_(\d{1,2})_([a-z]+)_(\d{4})", slug)
        if dm and dm.group(2) in MONTHS_IT:
            day, mon, year = dm.group(1), MONTHS_IT[dm.group(2)], dm.group(3)
            date_iso = f"{year}-{mon}-{int(day):02d}"
        nm = re.search(r"_n_(\d+)", slug)
        if nm:
            number = nm.group(1)
        return type_key, label, date_iso, number

    # ── Enumeration ───────────────────────────────────────────────────
    def _year_docs(self, year: int) -> List[Dict[str, Any]]:
        html = self._get_index(CHRONO.format(year=year))
        if not html:
            return []
        seen = set()
        out = []
        for m in DOC_RE.finditer(html):
            path, doc_id, slug = m.group(1), m.group(2), m.group(3)
            if doc_id in seen:
                continue
            type_key, label, date_iso, number = self._parse_slug(slug)
            if type_key is None:
                continue  # skip delibere/contratti/court decisions
            # Keep only acts whose parsed date falls in the enumerated year.
            # This attributes each act to the correct year and drops the
            # cross-referenced older acts linked inside act summaries (those
            # are enumerated under their own year).
            if not date_iso or not date_iso.startswith(str(year)):
                continue
            seen.add(doc_id)
            out.append({
                "doc_id": doc_id,
                "slug": slug,
                # ?view=1 expands the full consolidated article text; without it
                # the page renders only collapsed article headers.
                "url": BASE + path + "?view=1",
                "type_key": type_key,
                "label": label,
                "date": date_iso,
                "number": number,
                "year": year,
            })
        return out

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now(timezone.utc).year
        for year in range(current_year, FIRST_YEAR - 1, -1):
            docs = self._year_docs(year)
            logger.info("Year %d: %d legislation acts", year, len(docs))
            for d in docs:
                yield d
            time.sleep(0.5)

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        try:
            since_year = int(str(since)[:4])
        except (TypeError, ValueError):
            since_year = datetime.now(timezone.utc).year - 1
        current_year = datetime.now(timezone.utc).year
        for year in range(current_year, since_year - 1, -1):
            for d in self._year_docs(year):
                yield d
            time.sleep(0.5)

    # ── Normalization (downloads full text) ───────────────────────────
    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = raw.get("url", "")
        if not url:
            return None
        html = self._get_doc(url)
        if not html:
            return None
        text = self._extract_body(html)
        if len(text) < 60:
            logger.warning("Short/empty text for %s (%d chars), skipping", url, len(text))
            return None

        title = self._extract_title(html) or raw.get("slug", "").replace("_", " ")
        label = raw.get("label", "Atto")
        number = raw.get("number")
        year = raw.get("year")
        date = raw.get("date")

        if number:
            law_number = f"{label} {number}/{year}"
        else:
            law_number = f"{label} ({year})"

        return {
            "_id": f"IT/Bolzano/{raw['doc_id']}",
            "_source": "IT/Bolzano",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "law_number": law_number,
            "act_type": label,
            "jurisdiction": "IT-BZ",
            "language": "it",
        }

    def test(self) -> bool:
        try:
            docs = self._year_docs(2020)
            if not docs:
                logger.error("No docs enumerated for 2020")
                return False
            rec = self.normalize(docs[0])
            ok = bool(rec and len(rec["text"]) > 60)
            if ok:
                logger.info("Test OK: %s -> %d chars", rec["law_number"], len(rec["text"]))
            return ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    scraper = BolzanoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                if not record:
                    continue
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info("[%d] %s — %d chars", count,
                            record.get("law_number", record["_id"]), len(record["text"]))
                if count >= 15:
                    break
            logger.info("Done: %d sample records saved", count)
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            logger.info("bootstrap-fast done: %s", stats)
        else:
            stats = scraper.bootstrap()
            logger.info("bootstrap done: %s", stats)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else str(datetime.now().year - 1)
        count = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                count += 1
                logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
