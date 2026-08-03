#!/usr/bin/env python3
"""
US/CO-OAC -- Colorado Office of Administrative Courts
(Workers' Compensation ALJ Decisions & Orders)

Fetches the full text of Administrative Law Judge decisions and orders of
the Colorado Office of Administrative Courts (OAC), the state's central
independent administrative tribunal. OAC's Workers' Compensation unit hears
contested workers'-compensation claims between injured workers and
employers/insurers; each ALJ decision resolves a specific contested case =
case_law. The decisions are official Colorado state-government works in the
public domain (government edicts, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The OAC publishes its redacted decisions as monthly/annual *compilation*
  PDFs on its own reachable host:
      https://oac.colorado.gov/case-types/workers-compensation/decisions-and-orders-wc
  That listing page server-renders ~34 links to born-digital compilation
  PDFs under /sites/oac/files/documents/*.pdf (e.g. "Feb 2026 Decisions.pdf",
  "2025 Jan Combined Orders.pdf", "2015-WC-Decisions-oac.pdf"), spanning
  2015-present. Each compilation concatenates many individual ALJ decisions;
  every decision begins with a fixed header block:
      Office of Administrative Courts
      State of Colorado
      Workers' Compensation No. WC 5-262-272-001
  and closes with a signing date ("Signed: February 3, 2026"). We split each
  compilation on that header into individual decision records so one record =
  one contested case.

Strategy:
  1. GET the listing page; harvest compilation PDF URLs (+ a period label).
  2. Download each PDF (curl, browser UA, ~1 req/s); extract text via
     common.pdf_extract (born-digital text layer, no OCR).
  3. Split the text on the decision-start header; emit one raw record per
     individual decision (WC number, signed date, body).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CO-OAC")

BASE_URL = "https://oac.colorado.gov"
LISTING_URL = BASE_URL + "/case-types/workers-compensation/decisions-and-orders-wc"

# Compilation PDF hrefs on the listing page (absolute or site-relative).
PDF_HREF_RE = re.compile(
    r'href="((?:https://oac\.colorado\.gov)?/sites/oac/files/documents/[^"]+\.pdf)"',
    re.I,
)

# Start-of-decision header inside a compilation. The apostrophe in
# "Workers'" varies (straight ' vs curly ’), so match any 0-3 non-space
# glyphs between "Workers" and "Compensation".
DECISION_START_RE = re.compile(
    r"Office of Administrative Courts\s+State of Colorado\s+"
    r"Workers\S{0,3}\s*Compensation\s+No\.?\s*(WC[\s0-9-]+)",
    re.I,
)

# Signing / dated lines that close a decision.
SIGNED_RE = re.compile(
    r"(?:Signed|Dated|Entered|Issued)\s*:?\s*"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
ANY_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_TOKEN_RE = re.compile(
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t)?(?:ember)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)",
    re.I,
)
MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


class COOACScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_text(self, url: str) -> str | None:
        b = self._curl_bytes(url)
        return b.decode("utf-8", "replace") if b else None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _period_from_name(name: str) -> tuple[str | None, int | None]:
        """(iso-ish period label, best-guess year) from the file name."""
        from urllib.parse import unquote
        n = unquote(name)
        y = None
        ym = re.search(r"(20\d{2}|19\d{2})", n)
        if ym:
            y = int(ym.group(1))
        mo = None
        mt = MONTH_TOKEN_RE.search(n)
        if mt:
            mo = MONTH_ABBR.get(mt.group(1).lower()[:3])
        if y and mo:
            return f"{y:04d}-{mo:02d}", y
        if y:
            return f"{y:04d}", y
        return None, None

    @staticmethod
    def _norm_wc(raw_wc: str) -> str:
        wc = re.sub(r"\s+", " ", raw_wc.strip())
        # keep "WC" + the dash-number run only
        m = re.match(r"(WC[\s0-9-]*)", wc, re.I)
        wc = (m.group(1) if m else wc).strip().rstrip("-").strip()
        return wc

    @classmethod
    def _decision_date(cls, text: str, comp_year: int | None,
                       comp_period: str | None) -> str | None:
        tail = text[-4000:]
        m = SIGNED_RE.search(tail) or SIGNED_RE.search(text)
        if not m:
            # last plain date in the decision (decisions close with a date)
            all_d = list(ANY_DATE_RE.finditer(text))
            m = all_d[-1] if all_d else None
        if m:
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        if comp_period and re.fullmatch(r"\d{4}-\d{2}", comp_period):
            return f"{comp_period}-01"
        if comp_year:
            return f"{comp_year:04d}-01-01"
        return None

    # --------------------------------------------------------- discovery
    def discover_documents(self) -> list[dict]:
        html = self._curl_text(LISTING_URL)
        if not html:
            logger.error("Listing page returned no content")
            return []
        seen: set[str] = set()
        out: list[dict] = []
        for href in PDF_HREF_RE.findall(html):
            url = href if href.startswith("http") else BASE_URL + href
            if url in seen:
                continue
            seen.add(url)
            name = url.rsplit("/", 1)[-1]
            period, year = self._period_from_name(name)
            out.append({
                "doc_url": url,
                "file_name": name,
                "period": period,
                "comp_year": year,
                "safe_slug": re.sub(r"[^A-Za-z0-9._-]+", "-",
                                    self._period_from_name(name)[0] or name)[:60],
            })
        # newest first (period label sorts reasonably)
        out.sort(key=lambda r: (r["period"] or ""), reverse=True)
        logger.info(f"Discovered {len(out)} OAC compilation PDFs")
        return out

    # ------------------------------------------------- split a compilation
    def _split_decisions(self, comp: dict, full_text: str) -> list[dict]:
        matches = list(DECISION_START_RE.finditer(full_text))
        records: list[dict] = []
        if not matches:
            # No per-decision header found — keep the whole compilation as
            # one record so no full text is lost.
            body = full_text.strip()
            if len(body) >= 400:
                records.append(self._make_raw(comp, None, body, 0))
            return records
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
            body = full_text[start:end].strip()
            if len(body) < 400:
                continue
            wc = self._norm_wc(m.group(1))
            records.append(self._make_raw(comp, wc, body, i))
        return records

    def _make_raw(self, comp: dict, wc: str | None, body: str, seq: int) -> dict:
        date = self._decision_date(body, comp.get("comp_year"), comp.get("period"))
        h = hashlib.sha1(body.encode("utf-8", "replace")).hexdigest()[:10]
        wc_slug = re.sub(r"[^A-Za-z0-9-]+", "", (wc or "").replace("WC", "WC-")).strip("-")
        idbase = wc_slug or f"{comp['safe_slug']}-{seq}"
        return {
            "wc_number": wc,
            "text": body,
            "date": date,
            "doc_url": comp["doc_url"],
            "period": comp.get("period"),
            "compilation": comp["file_name"],
            "id_slug": f"{idbase}-{h}",
        }

    def _process_pdf(self, comp: dict) -> list[dict]:
        blob = self._curl_bytes(comp["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {comp['doc_url']}")
            return []
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {comp['doc_url']}")
            return []
        text = pdf_extract.extract_pdf_markdown(
            "US/CO-OAC", comp["safe_slug"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {comp['doc_url']}")
            return []
        recs = self._split_decisions(comp, text)
        logger.info(f"  {comp['file_name']}: {len(recs)} decisions")
        return recs

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CO OAC discovery + PDF split...")
        try:
            comps = self.discover_documents()
            if not comps:
                logger.error("  No compilation PDFs discovered")
                return False
            logger.info(f"  Discovered {len(comps)} compilations")
            recs = self._process_pdf(comps[0])
            if recs and recs[0]["text"] and len(recs[0]["text"]) > 400:
                logger.info(f"  Split OK — first decision {recs[0].get('wc_number')} "
                            f"({len(recs[0]['text'])} chars, date {recs[0].get('date')})")
            else:
                logger.error("  Split produced no usable decision")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        wc = raw.get("wc_number")
        if wc:
            title = f"Colorado OAC Workers' Compensation Decision — {wc}"
        else:
            title = f"Colorado OAC Workers' Compensation Decisions — {raw.get('period') or raw.get('compilation')}"
        title = title[:300]
        return {
            "_id": f"US/CO-OAC/{raw['id_slug']}",
            "_source": "US/CO-OAC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["id_slug"],
            "case_number": wc,
            "issuer": "Colorado Office of Administrative Courts",
            "docket_type": "Workers' Compensation",
            "compilation": raw.get("compilation"),
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CO",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_ids: set[str] = set()
        comps = self.discover_documents()
        if sample:
            comps = comps[:2]
        for comp in comps:
            for raw in self._process_pdf(comp):
                if raw["id_slug"] in seen_ids:
                    continue
                seen_ids.add(raw["id_slug"])
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CO-OAC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = COOACScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
