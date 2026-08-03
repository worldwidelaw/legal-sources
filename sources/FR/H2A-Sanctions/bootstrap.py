#!/usr/bin/env python3
"""
FR/H2A-Sanctions -- Haute autorité de l'audit (H2A), Commission des sanctions
-- Décisions.

The Haute autorité de l'audit (H2A) is the French public authority that
regulates the statutory-audit profession (commissaires aux comptes / statutory
auditors and, since the CSRD, sustainability-assurance providers). It succeeded
the Haut conseil du commissariat aux comptes (H3C) on 1 January 2024. Its
Commission des sanctions (before 2021, the "formation restreinte" of the H3C) is
an independent adjudicatory body that, after adversarial disciplinary
proceedings, imposes sanctions (avertissement, blâme, interdiction temporaire,
radiation, sanction pécuniaire) on auditors and audit firms for professional
misconduct. Each decision is a reasoned adjudication of a specific case =
case_law.

Decisions are published on the H2A site as born-digital PDFs under
/wp-content/uploads/. The public WordPress REST media API enumerates every
uploaded file; decision PDFs are identified by their file-name pattern:
  - "CS-YYYY-NN" : Commission des sanctions decisions (2021-present)
  - "FR-YYYY-NN-S" : formation restreinte decisions (former H3C, pre-2021)

Strategy:
  - Page the WP REST media endpoint (/wp-json/wp/v2/media) and keep the PDFs
    whose file name matches the CS-/FR- decision pattern.
  - Download each born-digital decision PDF and extract full text with PyMuPDF
    (pdfplumber/pypdf fallback). No OCR needed.
  - Parse the decision number (CS/FR YYYY-NN) from the file name and the
    decision date from the PDF body ("Décision du/rendue le DD mois YYYY"),
    falling back to the media upload date.
  - One record per decision.

Data:
  - ~120 Commission des sanctions / formation restreinte decisions, 2010-present
  - Language: French
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import unquote
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.H2A-Sanctions")

BASE_URL = "https://h2a-france.org"
MEDIA_API = "/wp-json/wp/v2/media?per_page=100&page={page}"
MAX_PAGES = 20

# Decision file-name patterns: CS-2024-15..., FR-2020-05-S..., FR2023-40-S...
DECISION_RE = re.compile(r"\b(CS|FR)[-_ ]?(20\d\d)[-_](\d{1,3})\b", re.I)
# Explicitly exclude non-decision publications that might still carry a CS/FR
# token (annexes, settlement agreements, opinions, reports, handbooks, lists).
EXCLUDE_RE = re.compile(
    r"(vademecum|rapport|annexe|accord|avis|liste|tableau|formulaire|"
    r"communique|guide|charte|reglement|deliberation)", re.I)

DATE_TXT_RE = re.compile(
    r"(?:D[ée]cision\s+(?:du|rendue\s+le)|s[ée]ance\s+du|rendue\s+le|du)\s+"
    r"(\d{1,2})(?:er)?\s+"
    r"(janvier|février|fevrier|mars|avril|mai|juin|juillet|ao[uû]t|"
    r"septembre|octobre|novembre|décembre|decembre)\s+(\d{4})", re.I)

_FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11,
    "décembre": 12, "decembre": 12,
}


def _parse_fr_date(text: str) -> Optional[str]:
    if not text:
        return None
    m = DATE_TXT_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = _FR_MONTHS.get(m.group(2).lower())
    year = int(m.group(3))
    if not month:
        return None
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, TypeError):
        return None


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital decision PDF via PyMuPDF, with a shared
    pdfplumber/pypdf fallback."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 120:
                return text
        except Exception as e:
            logger.debug(f"fitz extract failed: {e}")
    try:
        from common import pdf_extract as _pe
        for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
            f = getattr(_pe, fn, None)
            if f:
                try:
                    t = f(pdf_bytes)
                    if t and len(t) >= 120:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in (text or "").replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


class H2ASanctionsScraper(BaseScraper):
    """Scraper for the H2A (ex-H3C) Commission des sanctions decisions
    (WP REST media enumeration + born-digital decision PDFs)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json,text/html,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
            timeout=90,
        )

    # -- HTTP helpers ----------------------------------------------------
    def _get(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"GET {url}: HTTP {resp.status_code}")
            return None
        return resp.content

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        data = self._get(url)
        if not data:
            return None
        if not data[:5].startswith(b"%PDF"):
            logger.debug(f"pdf {url}: not a PDF")
            return None
        return data

    # -- listing (WP media API) ------------------------------------------
    def _list_decisions(self) -> List[Dict[str, Any]]:
        seen, out = set(), []
        for page in range(1, MAX_PAGES + 1):
            body = self._get(BASE_URL + MEDIA_API.format(page=page))
            if not body:
                break
            try:
                items = json.loads(body)
            except Exception as e:
                logger.warning(f"media page {page} parse: {e}")
                break
            if not isinstance(items, list) or not items:
                break
            for m in items:
                su = (m.get("source_url") or "")
                if not su.lower().endswith(".pdf"):
                    continue
                fname = unquote(su.split("/")[-1])
                if EXCLUDE_RE.search(fname):
                    continue
                dm = DECISION_RE.search(fname)
                if not dm:
                    continue
                kind = dm.group(1).upper()
                number = f"{kind}-{dm.group(2)}-{int(dm.group(3)):02d}"
                if number in seen:
                    continue
                seen.add(number)
                out.append({
                    "number": number,
                    "kind": kind,
                    "pdf_url": su,
                    "filename": fname,
                    "upload_date": (m.get("date") or "")[:10] or None,
                })
        return out

    def _build_raw(self, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pdf = self._fetch_pdf(meta["pdf_url"])
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract {meta['pdf_url']}: {e}")
            text = ""
        if not text:
            return None
        date_iso = _parse_fr_date(text[:3000]) or meta.get("upload_date")
        raw = dict(meta)
        raw["text"] = text
        raw["date"] = date_iso
        return raw

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        decisions = self._list_decisions()
        if not decisions:
            raise RuntimeError(
                "H2A media API returned 0 decision PDFs — site blocked, "
                "media endpoint changed, or file-name pattern changed"
            )
        produced = 0
        for meta in decisions:
            raw = self._build_raw(meta)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "H2A: found decision files but extracted 0 full-text PDFs "
                "— download blocked or PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """The corpus is small; re-scan and keep decisions on/after `since`
        (by decision date, falling back to upload date)."""
        since_date = since.date()
        for meta in self._list_decisions():
            raw = self._build_raw(meta)
            if not raw:
                continue
            d = raw.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        number = raw.get("number")
        kind = raw.get("kind")
        body = ("Commission des sanctions" if kind == "CS"
                else "formation restreinte (ex-H3C)")
        title = f"Décision de la {body} n° {number}"
        return {
            "_id": f"FR-H2A-{number}",
            "_source": "FR/H2A-Sanctions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("pdf_url"),
            "pdf_url": raw.get("pdf_url"),
            "decision_number": number,
            "court": "Haute autorité de l'audit (H2A) — Commission des sanctions",
            "jurisdiction": "FR",
            "language": "fr",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing H2A WP media enumeration...")
        decisions = self._list_decisions()
        print(f"  decision PDFs found: {len(decisions)}")
        if not decisions:
            return
        kinds = {}
        for d in decisions:
            kinds[d["kind"]] = kinds.get(d["kind"], 0) + 1
        print(f"  by kind: {kinds}")
        print(f"  first: {decisions[0]['number']} | {decisions[0]['filename'][:50]}")
        raw = self._build_raw(decisions[0])
        if raw:
            print(f"  date={raw['date']}  full text: {len(raw['text'])} chars - OK")


def main():
    scraper = H2ASanctionsScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
