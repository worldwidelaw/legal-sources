#!/usr/bin/env python3
"""
IN/AERB -- Atomic Energy Regulatory Board (India)

Fetches the full text of the regulatory corpus published by India's Atomic
Energy Regulatory Board (AERB), the nuclear and radiation safety regulator
constituted under the Atomic Energy Act, 1962:

  * Safety Codes, Safety Guides, Safety Standards and Safety Manuals
    (the AERB "Codes & Guides" series covering nuclear power plants,
    research reactors, fuel-cycle facilities, radiation facilities,
    industrial radiography, radiotherapy, diagnostic radiology, transport
    of radioactive material, radioactive-waste management, siting,
    decommissioning and emergency preparedness) — the binding regulatory
    instruments AERB issues.
  * Acts and Rules administered by AERB (Atomic Energy Act 1962 and its
    amendments, Radiation Protection Rules 2004, Atomic Energy (Safe
    Disposal of Radioactive Wastes) Rules 1987, Atomic Energy (Factories)
    Rules 1996, Civil Liability for Nuclear Damage Act 2010 and Rules
    2011, GSR notifications).
  * Regulatory decisions and consents — press releases and status notes
    announcing licensing actions for named installations (clearance for
    first approach to criticality, operating-licence grants and renewals,
    site approvals, INES event ratings, regulatory-inspection outcomes).
  * Annual Reports, Safety Research Institute highlights, guidelines and
    other official AERB doctrine.

Strategy (Internet Archive / Wayback Machine):

  aerb.gov.in resets the TLS handshake for every connection from this and
  other non-Indian vantages (port 80 answers with a 302 to https, port 443
  sends TCP RST at Client Hello — verified 2026-08-06 with LibreSSL and
  OpenSSL 3.6 clients, browser UA, TLS 1.2 pinning and bare-IP/no-SNI).
  The corpus is therefore read from the Internet Archive, which holds a
  deep crawl of the Board's document tree (~2,700 archived PDF snapshots,
  most recent captures December 2025).

  1. fetch_all() enumerates every archived ``*.pdf`` under the aerb.gov.in
     domain via the Wayback CDX API. Rows whose archived status is 4xx/5xx
     are dropped; revisit rows (statuscode "-") are KEPT — the CDX filter
     ``statuscode:200`` silently hides them.

  2. AERB has reorganised its site several times, so the same document is
     reachable under several path schemes
     (``/AERBPortal/pages/English/t/publications/CODESGUIDES/x.pdf``,
     ``/T/PUBLICATIONS/CODESGUIDES/x.pdf``, ``/images/PDF/x.pdf``,
     ``/storage/uploads/documents/x.pdf``, plus ``index.php/english/``
     prefixed variants). Records are de-duplicated on
     (language, lowercase filename), keeping the newest good capture and
     preferring the modern ``/images/PDF/`` | ``/storage/`` scheme.

  3. normalize() downloads the raw archived PDF
     (``https://web.archive.org/web/{ts}id_/{original}``) and extracts full
     text via the shared ``common.pdf_extract`` helper (PyMuPDF with
     pdfplumber / OCR fallback for scanned scans). Live aerb.gov.in is
     attempted first and the scraper latches to archive-only after three
     consecutive live failures with no live success, so the same code runs
     unchanged from an Indian vantage.

  Document type is inferred from the path and title: Acts / Rules / GSR
  notifications => legislation, licensing and consent decisions for a named
  installation => case_law, everything else (safety codes, guides,
  manuals, annual reports, guidance) => doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.AERB")

SOURCE_ID = "IN/AERB"
CDX_URL = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"

CDX_PARAMS = {
    "url": "aerb.gov.in",
    "matchType": "domain",
    "filter": "urlkey:.*\\.pdf",
    "collapse": "urlkey",
    "fl": "original,timestamp,statuscode",
    "output": "text",
    "limit": "60000",
}

# Purely administrative material — recruitment, tenders, procurement.
SKIP_PATH_RE = re.compile(
    r"(erecruit|recruit|vacanc|tender|quotation|procure|advt|career|"
    r"apply[-_ ]?online|result[-_ ]?sheet)",
    re.I,
)

# Acts / Rules / statutory notifications administered by AERB. Kept
# deliberately narrow: a safety guide that merely cites "the Atomic Energy
# Act, 1962" in its opening line is doctrine, not legislation.
LEGISLATION_PATH_RE = re.compile(r"(actsrules|/acts?/|/rules?/|\bgsr[-_]?\d)", re.I)
LEGISLATION_FILE_RE = re.compile(
    r"^(the[-_ ])?[a-z0-9()&,._ -]*?"
    r"(act|rules|ordinance|amendment)[-_ ]?(no[-_ ])?\d{2,4}",
    re.I,
)
LEGISLATION_TITLE_RE = re.compile(
    r"^(the\s+)?[A-Za-z0-9()&,.\- ]{0,90}?\b(act|rules|ordinance)\b,?\s*"
    r"(no\.?\s*\d+\s*of\s*)?\d{4}\b",
    re.I,
)

# Licensing / consenting decisions addressed to a named installation.
DECISION_RE = re.compile(
    r"(consent[-_ ]?(issued|granted|for|to)|"
    r"clearance[-_ ]?(for|to|of)|aerbclearance|"
    r"first[-_ ]?approach[-_ ]?to[-_ ]?criticality|"
    r"site[-_ ]?approval|"
    r"(operating[-_ ])?licen[cs]e[-_ ]?(grant|renew|issu|validity|extension)|"
    r"(grant|renewal|issue|validity|extension)[-_ ]?of[-_ ]?"
    r"(the[-_ ])?(operating[-_ ])?licen[cs]e|"
    r"authoris(ation)?[-_ ]?(issued|granted)|"
    r"ines[-_ ]?rating)",
    re.I,
)

# Filename date encodings AERB uses.
DATE_DDMONYYYY_RE = re.compile(
    r"(\d{1,2})[-_ ]?"
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
    r"[-_ ]?(\d{4})",
    re.I,
)
DATE_P_DDMMYYYY_RE = re.compile(r"^p?(\d{2})(\d{2})(20\d{2})$", re.I)
DATE_MMYYYY_RE = re.compile(r"[-_](\d{2})(20\d{2})$")
MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

MIN_TEXT = 400  # below this a PDF is a form/cover sheet, not a document

# Masthead lines that appear above the real title on AERB documents.
BOILERPLATE_TITLE_RE = re.compile(
    r"^("
    r"government of india|bhabha atomic|department of atomic energy|"
    r"atomic energy regulatory board|परमाणु|भारत सरकार|"
    r"aerb[/\s-]|no[.:]?\s*aerb|niyamak bhavan|anushaktinagar|mumbai\b|"
    r"press release|for immediate release|website|e-?mail|telephone|fax|"
    r"page \d|revision \d|rev\.?\s*\d|\(?\d{4}\)?$"
    r")",
    re.I,
)
# Banner-only titles ("AERB SAFETY GUIDE", "SAFETY CODE") that need the
# following subject line to be meaningful.
BANNER_TITLE_RE = re.compile(
    r"(aerb\s+)?(safety\s+(guide|code|standard|manual|report)|"
    r"guidelines?|regulatory\s+(body|document))\s*(no\.?[\w/\-. ]*)?",
    re.I,
)


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ").replace("­", "")
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def prettify(filename: str) -> str:
    """Human-readable fallback title from a PDF filename."""
    stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
    stem = re.sub(r"[_\-]+", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem[:300] or filename


def filename_date(stem: str) -> str | None:
    m = DATE_DDMONYYYY_RE.search(stem)
    if m:
        day, mon, year = int(m.group(1)), MONTHS[m.group(2).lower()[:3]], int(m.group(3))
        if 1 <= day <= 31 and 1950 <= year <= 2100:
            return f"{year:04d}-{mon:02d}-{day:02d}"
    m = DATE_P_DDMMYYYY_RE.match(stem)
    if m:
        day, mon, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= day <= 31 and 1 <= mon <= 12:
            return f"{year:04d}-{mon:02d}-{day:02d}"
    m = DATE_MMYYYY_RE.search(stem)
    if m:
        mon, year = int(m.group(1)), int(m.group(2))
        if 1 <= mon <= 12:
            return f"{year:04d}-{mon:02d}-01"
    return None


def wayback_date(ts: str) -> str | None:
    if len(ts) >= 8 and ts[:8].isdigit():
        return f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
    return None


def normalized_dir(path: str) -> str:
    """Path directory with the historical site-scheme prefixes stripped."""
    d = path.rsplit("/", 1)[0].lower()
    d = re.sub(r"^/(index\.php/)?", "/", d)
    d = re.sub(r"^/(english|hindi)/", "/", d)
    d = re.sub(r"^/aerbportal/pages/(english|hindi)/", "/", d)
    return d


def dir_priority(ndir: str) -> int:
    """Crawl the regulatory core (codes, guides, acts, rules) first."""
    d = ndir.lower()
    if "codesguides" in d or "actsrules" in d or "publications" in d:
        return 0
    if "document" in d:
        return 1
    if "prsrel" in d or "news" in d:
        return 2
    if "form" in d:
        return 4
    return 3


def classify(path: str, title: str) -> str:
    filename = path.rsplit("/", 1)[-1]
    if (
        LEGISLATION_PATH_RE.search(path)
        or LEGISLATION_FILE_RE.match(filename)
        or LEGISLATION_TITLE_RE.match(title.strip())
    ):
        return "legislation"
    if DECISION_RE.search(f"{filename} {title}"):
        return "case_law"
    return "doctrine"


class AERBScraper(BaseScraper):

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
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 0.4
        # Live-first latch: stop trying aerb.gov.in after repeated failures.
        self._live_ok = 0
        self._live_fail = 0
        self._live_disabled = False

    # ---- low-level fetch ----------------------------------------------------

    def _get(self, url: str, retries: int = 3, timeout: int = 90) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code in (403, 404, 410):
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_live(self, url: str) -> bytes | None:
        """Try the live AERB host once; latch off after repeated failures."""
        if self._live_disabled:
            return None
        try:
            resp = self.session.get(url, timeout=45)
            if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
                self._live_ok += 1
                self._live_fail = 0
                return resp.content
        except Exception as e:
            logger.debug(f"live fetch failed for {url}: {e}")
        self._live_fail += 1
        if self._live_fail >= 3 and self._live_ok == 0:
            logger.info(
                "aerb.gov.in unreachable from this vantage — "
                "switching to Internet Archive only"
            )
            self._live_disabled = True
        return None

    # ---- CDX enumeration ----------------------------------------------------

    def _cdx_rows(self) -> list[tuple[str, str, str]]:
        for attempt in range(5):
            try:
                resp = self.session.get(CDX_URL, params=CDX_PARAMS, timeout=240)
                if resp.status_code == 200:
                    rows = []
                    for line in resp.text.splitlines():
                        parts = line.split()
                        if len(parts) >= 3:
                            rows.append((parts[0], parts[1], parts[2]))
                    return rows
                logger.warning(f"CDX HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"CDX error (attempt {attempt + 1}): {e}")
            time.sleep(3 * (attempt + 1))
        raise RuntimeError(
            "Wayback CDX enumeration failed for aerb.gov.in — refusing to "
            "report an empty corpus"
        )

    def _enumerate_documents(self) -> list[dict]:
        rows = self._cdx_rows()
        logger.info(f"CDX returned {len(rows)} archived aerb.gov.in PDF snapshots")

        best: dict[tuple[str, str], dict] = {}
        for orig, ts, status in rows:
            if status not in ("200", "-"):
                continue
            path = urllib.parse.unquote(urllib.parse.urlparse(orig).path)
            if not path.lower().endswith(".pdf"):
                continue  # CDX rows with junk suffixes (.pdf.98, .pdfV, …)
            if SKIP_PATH_RE.search(path):
                continue

            filename = path.rsplit("/", 1)[-1]
            ndir = normalized_dir(path)
            lang = "hi" if "/hindi/" in path.lower() else "en"
            key = (lang, filename.lower())

            # Prefer the modern site scheme, then the newest capture.
            scheme_score = 1 if ndir.startswith(("/images/", "/storage/")) else 0
            cand = {
                "orig": orig,
                "ts": ts,
                "path": path,
                "dir": ndir,
                "filename": filename,
                "language": lang,
                "score": scheme_score,
                "first_ts": ts,
            }
            cur = best.get(key)
            if cur is None:
                best[key] = cand
            else:
                cand["first_ts"] = min(cur["first_ts"], ts)
                if (scheme_score, ts) > (cur["score"], cur["ts"]):
                    best[key] = cand
                else:
                    cur["first_ts"] = cand["first_ts"]

        # Round-robin the categories so a truncated run (or the sample) spans
        # codes & guides, acts & rules, licensing decisions and doctrine
        # rather than draining one directory alphabetically.
        by_dir: dict[str, list[dict]] = {}
        for rec in sorted(best.values(), key=lambda r: (r["dir"], r["filename"])):
            by_dir.setdefault(rec["dir"], []).append(rec)
        buckets = sorted(
            by_dir.values(),
            key=lambda b: (dir_priority(b[0]["dir"]), -len(b), b[0]["dir"]),
        )
        records = []
        for i in range(max(len(b) for b in buckets) if buckets else 0):
            for bucket in buckets:
                if i < len(bucket):
                    records.append(bucket[i])
        logger.info(
            f"{len(records)} unique AERB documents after dedup "
            f"across {len(buckets)} site directories"
        )
        return records

    # ---- text extraction ----------------------------------------------------

    def _pdf_text(self, pdf_bytes: bytes, doc_id: str, doc_type: str) -> str:
        try:
            text = extract_pdf_markdown(
                SOURCE_ID,
                doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation" if doc_type != "case_law" else "case_law",
                force=True,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {doc_id}: {e}")
            return ""
        return clean_text(text or "")

    @staticmethod
    def _title_from_text(text: str, fallback: str) -> str:
        """First substantive line, skipping masthead / document-code boilerplate."""
        def is_upper(s: str) -> bool:
            letters = [c for c in s if c.isalpha()]
            return bool(letters) and sum(c.isupper() for c in letters) / len(letters) > 0.7

        lines = [
            re.sub(r"\s+", " ", ln.strip(" .-—_*#|")).strip()
            for ln in text.split("\n")[:60]
        ]
        candidates = []
        for idx, line in enumerate(lines):
            if len(line) < 10 or len(line) > 250:
                continue
            if BOILERPLATE_TITLE_RE.match(line):
                continue
            alpha = sum(c.isalpha() for c in line)
            if alpha < 10 or alpha / len(line) < 0.55:
                continue  # document codes, tables of figures, garbled glyphs
            if len(line.split()) < 3:
                continue
            # AERB titles are set in all caps and wrap over several lines;
            # rejoin the wrapped continuation so the title is not cut mid-phrase.
            if is_upper(line):
                for nxt in lines[idx + 1: idx + 5]:
                    if (
                        not nxt
                        or len(title_join := f"{line} {nxt}") > 180
                        or not is_upper(nxt)
                        or BOILERPLATE_TITLE_RE.match(nxt)
                        or line.endswith((".", ":"))
                    ):
                        break
                    line = title_join
            candidates.append(line)
            if len(candidates) >= 2:
                break
        if not candidates:
            return fallback
        title = candidates[0]
        # A leading "AERB SAFETY GUIDE"-style banner is more useful with the
        # actual subject line appended.
        if BANNER_TITLE_RE.fullmatch(title) and len(candidates) > 1:
            title = f"{title} — {candidates[1]}"
        # AERB reprints the document-code banner under the title; drop the echo.
        head = title[:14]
        echo = title.find(head, 14)
        if len(head) >= 10 and echo > 0:
            title = title[:echo]
        return title.strip(" -—:;,")[:300]

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        path = raw["path"]
        filename = raw["filename"]
        stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
        doc_id = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{raw['language']}_{stem}")[:180]

        live_url = f"https://www.aerb.gov.in{urllib.parse.quote(path)}"
        archive_url = WB_RAW.format(ts=raw["ts"], url=raw["orig"])

        pdf = self._get_live(live_url)
        used_url = live_url
        if not pdf:
            pdf = self._get(archive_url)
            used_url = archive_url
        if not pdf or pdf[:5] != b"%PDF-":
            logger.debug(f"No PDF bytes for {filename}")
            return None

        prelim_type = classify(path, prettify(filename))
        text = self._pdf_text(pdf, doc_id, prelim_type)
        if len(text) < MIN_TEXT:
            logger.debug(f"Short/empty text for {filename} ({len(text)} chars)")
            return None

        title = self._title_from_text(text, prettify(filename))
        doc_type = classify(path, title)
        date = filename_date(stem) or wayback_date(raw.get("first_ts") or raw["ts"])

        return {
            "_id": f"{SOURCE_ID}/{doc_id}",
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": live_url,
            "archive_url": archive_url if used_url == archive_url else None,
            "document_category": raw["dir"].strip("/") or "root",
            "language": raw["language"],
            "authority": "Atomic Energy Regulatory Board (AERB)",
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing AERB document enumeration via Wayback CDX…")
        try:
            records = self._enumerate_documents()
            if len(records) < 200:
                logger.error(f"  Too few documents enumerated: {len(records)}")
                return False
            logger.info(f"  Enumerated {len(records)} documents")
            got = 0
            for raw in records:
                rec = self.normalize(raw)
                if rec:
                    got += 1
                    logger.info(
                        f"  {rec['_type']}: {rec['title'][:70]!r} "
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

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._enumerate_documents()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self._enumerate_documents():
            stem = re.sub(r"\.pdf$", "", raw["filename"], flags=re.I)
            date = filename_date(stem) or wayback_date(raw.get("first_ts") or raw["ts"])
            if not since or not date or date >= since:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/AERB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AERBScraper()

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
