#!/usr/bin/env python3
"""
EE/VAKO -- Estonia, Riigihangete vaidlustuskomisjon (Public Procurement
           Review Committee), VAKO otsused

Full text of the decisions (otsused) of the Estonian Public Procurement
Review Committee published by the Ministry of Finance at

    https://fin.ee/riigihanked-riigiabi-osalused/riigihanked/vaidlustusmenetlus

Strategy (server-rendered index, no auth, no captcha):

  The page carries three tables. Two of them are decision registers —
  small procurements (väikehange) and mini-competitions (minikonkurss) —
  with six columns each: procedure type, decision date, decision number,
  subject of the challenge, outcome, and a link to the decision PDF. Every
  metadata field therefore comes from the register, and only the body text
  has to be read out of the born-digital PDF.

  The third table lists the Committee's annual statistical reports, which
  are not decisions; it is skipped.

SCOPE CAVEAT: this is the curated subset the Ministry publishes (~83
decisions, 2017 -> 2026), not the complete VAKO corpus. The full set lives
in the Riigihangete register SPA whose dispute-search API is auth-gated
(POST https://riigihanked.riik.ee/rhr/api/public/v1/search/disputes -> 401
across three payload shapes with session cookies, re-verified 2026-08-02).

GOTCHAS:
  - Estonian diacritics in the file names are percent-encoded; the exact
    href bytes from the HTML are fetched unchanged, never re-typed.
  - A few rows link the same PDF under two entries; records de-duplicate
    on the decision number, falling back to the file path.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402
from common.pdf_extract import _extract as extract_pdf_text  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.VAKO")

SOURCE_ID = "EE/VAKO"
BASE_URL = "https://fin.ee"
INDEX_URL = f"{BASE_URL}/riigihanked-riigiabi-osalused/riigihanked/vaidlustusmenetlus"

USER_AGENT = (
    "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://legaldatahunter.com)"
)
REQUEST_DELAY = 1.0
MAX_RETRIES = 5

TABLE_RE = re.compile(r"<table.*?</table>", re.S)
TR_RE = re.compile(r"<tr>(.*?)</tr>", re.S)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S)
HREF_RE = re.compile(r'href="([^"]+\.pdf[^"]*)"', re.I)
DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
TAG_RE = re.compile(r"<[^>]+>")

# The decision tables carry this header; the annual-reports table does not.
DECISION_HEADER = "otsuse nr"


def strip_tags(fragment: Optional[str]) -> str:
    if not fragment:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", fragment)
    text = TAG_RE.sub(" ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def clean_pdf_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_ee_date(cell: str) -> Optional[str]:
    """'26.09.2025' -> '2025-09-26'."""
    m = DATE_RE.search(cell or "")
    if not m:
        return None
    day, month, year = m.groups()
    try:
        return datetime(int(year), int(month), int(day)).date().isoformat()
    except ValueError:
        return None


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


class EEVakoScraper(BaseScraper):
    """Scraper for the VAKO decision register on fin.ee."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "et,en;q=0.8",
            }
        )

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str) -> requests.Response:
        delay = 2.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=180)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if (retry_after or "").isdigit() else delay
                    logger.warning(
                        "HTTP %s for %s — retry %s/%s in %.0fs",
                        resp.status_code, url, attempt, MAX_RETRIES, wait,
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "Request error for %s — retry %s/%s in %.0fs (%s)",
                    url, attempt, MAX_RETRIES, delay, exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 120)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Exhausted retries for {url}")

    # --------------------------------------------------------------- listing

    def _decision_rows(self) -> list[dict]:
        resp = self._get(INDEX_URL)
        html = resp.text
        tables = TABLE_RE.findall(html)
        if not tables:
            raise RuntimeError(
                f"{INDEX_URL} returned no tables — the page layout changed or "
                "this vantage is being served an error shell"
            )

        out: list[dict] = []
        seen: set[str] = set()
        for table in tables:
            rows = TR_RE.findall(table)
            if not rows:
                continue
            header = " | ".join(strip_tags(c) for c in CELL_RE.findall(rows[0])).lower()
            if DECISION_HEADER not in header:
                # The annual statistical-report table — not decisions.
                continue
            for row in rows[1:]:
                cells = CELL_RE.findall(row)
                if len(cells) < 6:
                    continue
                m = HREF_RE.search(cells[5])
                if not m:
                    continue
                # Keep the exact href bytes — the Estonian diacritics are
                # percent-encoded and must not be re-typed.
                pdf_url = urljoin(BASE_URL, m.group(1))
                decision_no = strip_tags(cells[2])
                key = decision_no or pdf_url
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "procedure": strip_tags(cells[0]),
                        "date": parse_ee_date(strip_tags(cells[1])),
                        "decision_no": decision_no,
                        "challenge_object": strip_tags(cells[3]),
                        "outcome": strip_tags(cells[4]),
                        "parties": strip_tags(cells[5]).split("|")[0].replace(".pdf", "").strip(),
                        "pdf_url": pdf_url,
                    }
                )

        if not out:
            raise RuntimeError(
                "VAKO index carried no decision rows — the register layout "
                "changed or fin.ee is blocking this vantage"
            )
        return out

    def fetch_all(self) -> Generator[dict, None, None]:
        rows = self._decision_rows()
        logger.info("VAKO register: %s decisions with a PDF", len(rows))
        for row in rows:
            yield row

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        if isinstance(since, datetime):
            since_iso = since.date().isoformat()
        else:
            since_iso = str(since)[:10]
        for row in self._decision_rows():
            if row["date"] and row["date"] >= since_iso:
                yield row

    # ------------------------------------------------------------- normalize

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None
        try:
            resp = self._get(pdf_url)
        except Exception as exc:  # noqa: BLE001 — logged, record skipped
            logger.warning("PDF download failed for %s: %s", raw.get("decision_no"), exc)
            return None

        content = resp.content
        if not content.startswith(b"%PDF-"):
            logger.warning(
                "Not a PDF for %s (%s bytes, starts %r)",
                raw.get("decision_no"), len(content), content[:16],
            )
            return None

        text = clean_pdf_text(extract_pdf_text(content))
        if len(text) < 200:
            logger.warning(
                "Empty/short extraction for %s (%s chars)",
                raw.get("decision_no"), len(text),
            )
            return None

        decision_no = raw.get("decision_no") or ""
        parties = raw.get("parties") or ""
        ident = slug(decision_no) or slug(parties) or slug(pdf_url.rsplit("/", 1)[-1])

        head = "VAKO otsus"
        if decision_no:
            head += f" {decision_no}"
        title = " — ".join([head] + [b for b in (parties, raw.get("challenge_object") or "") if b])

        return {
            "_id": f"ee-vako-{ident}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "text": text,
            "date": raw.get("date"),
            "url": pdf_url,
            "country": "EE",
            "language": "et",
            "court": "Riigihangete vaidlustuskomisjon (Public Procurement Review Committee)",
            "decision_number": decision_no or None,
            "parties": parties or None,
            "procedure_type": raw.get("procedure") or None,
            "challenge_object": raw.get("challenge_object") or None,
            "outcome": raw.get("outcome") or None,
            "document_type": "otsus",
        }

    # ------------------------------------------------------------------ test

    def test_api(self) -> bool:
        logger.info("Testing fin.ee VAKO register ...")
        try:
            rows = self._decision_rows()
            logger.info("  index OK — %s decision rows", len(rows))
            rec = self.normalize(rows[0])
            if not rec or not rec.get("text"):
                logger.error("  full-text extraction failed for %s", rows[0]["decision_no"])
                return False
            logger.info(
                "  %s OK — %s chars, date=%s, no=%s",
                rec["_id"], len(rec["text"]), rec["date"], rec["decision_number"],
            )
            logger.info("API test PASSED")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("API test FAILED: %s", exc)
            return False


def main():
    parser = argparse.ArgumentParser(description="EE/VAKO bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "updates"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = EEVakoScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    if args.command == "updates":
        since = args.since or datetime.now(timezone.utc).date().isoformat()
        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
