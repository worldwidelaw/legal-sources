#!/usr/bin/env python3
"""
US/TX-JudicialEthics -- Texas Committee on Judicial Ethics
                        — Judicial Ethics Opinions (1975 to Present)

Fetches the full text of the judicial ethics opinions issued by the Committee
on Judicial Ethics of the State Bar of Texas Judicial Section. The Committee
renders written opinions interpreting the Texas Code of Judicial Conduct in
response to written inquiries from judges; the General Counsel of the Office of
Court Administration (OCA) publishes them = doctrine (official written
interpretation of the judicial-conduct rules).

Access (no JavaScript, no CAPTCHA, no auth):
  The OCA publishes the ENTIRE corpus of opinions in a single born-digital PDF
  compilation ("TEXAS JUDICIAL ETHICS OPINIONS 1975 to Present"):

      https://www.txcourts.gov/media/678096/JudicialEthicsOpinions.pdf

  The PDF has a real text layer (a Subject Index followed by each opinion, in
  "Opinion No. N (YYYY)" / QUESTION / ANSWER / DISCUSSION form). It is split
  per-opinion here — one normalized record per opinion.

  txcourts.gov sits behind an Azure Front Door WAF that returns HTTP 403 to
  datacenter / non-residential IPs (and to this build vantage). The scraper
  therefore tries the live host first (desktop-Chrome UA) and, on a WAF block,
  falls back to the latest Internet-Archive Wayback capture of the same PDF
  (which is refreshed several times a year), so the corpus stays reachable and
  current from any vantage.

Strategy:
  1. Obtain the consolidated PDF bytes (live host -> Wayback fallback).
  2. Extract the full text layer, strip the running page footer.
  3. Split on the per-opinion "Opinion No. N (YYYY)" headers (each header is
     alone on its own line; inline citations to other opinions are excluded).
  4. Take the ALL-CAPS topic caption printed just above each header as the
     opinion title, and the year in the header parenthesis as the date.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-JudicialEthics")

PDF_URL = "https://www.txcourts.gov/media/678096/JudicialEthicsOpinions.pdf"
CDX_URL = (
    "http://web.archive.org/cdx/search/cdx?url=txcourts.gov/media/678096/"
    "JudicialEthicsOpinions.pdf&output=json"
    "&filter=statuscode:200&filter=mimetype:application/pdf&limit=-8"
)

# A per-opinion header is alone on its own line: "Opinion No. 12 (1981)" with an
# optional trailing footnote asterisk. This excludes inline citations to other
# opinions, which occur mid-sentence.
HEADER_RE = re.compile(
    r"(?m)^[ \t]*Opinion\s+No\.\s*(\d+[A-Z]?)\s*\((\d{4})\)[ \t]*\*?[ \t]*(?=\n)")
FOOTER_RE = re.compile(
    r"\n?[ \t]*Texas Judicial Ethics Opinions[ \t]*\n[ \t]*Page \d+ of \d+[ \t]*")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")


class TXJudicialEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._pdf_source_url = PDF_URL  # updated to the Wayback URL on fallback

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, timeout: int = 90) -> bytes | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--compressed", "--max-time", "60",
                     "-A", UA, url],
                    capture_output=True, timeout=timeout,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _wayback_latest(self) -> str | None:
        """Return the id_ raw URL of the most recent good Wayback capture."""
        raw = self._curl(CDX_URL, timeout=60)
        if not raw:
            return None
        try:
            rows = json.loads(raw.decode("utf-8", "replace"))
        except Exception:
            return None
        if len(rows) < 2:
            return None
        # rows[0] is the header; pick the newest by timestamp
        best = max(rows[1:], key=lambda r: r[1])
        ts, original = best[1], best[2]
        return f"https://web.archive.org/web/{ts}id_/{original}"

    def _get_pdf_bytes(self) -> bytes | None:
        """Live host first (WAF may 403 datacenter IPs) -> Wayback fallback."""
        raw = self._curl(PDF_URL)
        if raw and raw[:4] == b"%PDF":
            self._pdf_source_url = PDF_URL
            logger.info("fetched consolidated PDF from the live host")
            return raw
        logger.info("live host unavailable/WAF-blocked; trying Wayback")
        wb = self._wayback_latest()
        if not wb:
            logger.error("no Wayback capture available")
            return None
        raw = self._curl(wb, timeout=120)
        if raw and raw[:4] == b"%PDF":
            self._pdf_source_url = PDF_URL  # canonical citation URL
            logger.info(f"fetched consolidated PDF from Wayback ({wb})")
            return raw
        logger.error("Wayback capture did not return a PDF")
        return None

    # ---------------------------------------------------------- discovery
    @staticmethod
    def _extract_text(pdf_bytes: bytes) -> str:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        parts = [doc[i].get_text() for i in range(doc.page_count)]
        doc.close()
        text = "\n".join(parts)
        return FOOTER_RE.sub("\n", text)

    def _parse_opinions(self, pdf_bytes: bytes) -> list[dict]:
        """Split the consolidated PDF text into per-opinion records."""
        full = self._extract_text(pdf_bytes)
        ms = list(HEADER_RE.finditer(full))
        out: list[dict] = []
        seen: set[str] = set()
        for i, m in enumerate(ms):
            number, year = m.group(1), m.group(2)
            if number in seen:
                continue
            seen.add(number)
            start = m.start()
            end = ms[i + 1].start() if i + 1 < len(ms) else len(full)
            body = full[start:end].strip()
            # The topic caption is the last non-empty line printed above the
            # header (i.e. the tail of the previous segment / preamble).
            pre = full[ms[i - 1].start():start] if i > 0 else full[:start]
            pre_lines = [ln.strip() for ln in pre.splitlines() if ln.strip()]
            caption = pre_lines[-1] if pre_lines else ""
            # A caption is a short topic label, not a sentence.
            if len(caption) > 90 or caption.endswith((".", ",", ";", ":")):
                caption = ""
            text = re.sub(r"[ \t]+\n", "\n", body)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            if len(text) < 120:
                continue
            out.append({
                "number": number,
                "year": year,
                "caption": caption,
                "text": text,
            })
        return out

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Texas Committee on Judicial Ethics opinions...")
        pdf = self._get_pdf_bytes()
        if not pdf:
            logger.error("API test FAILED: could not obtain the consolidated PDF")
            return False
        ops = self._parse_opinions(pdf)
        if not ops:
            logger.error("API test FAILED: no opinions parsed")
            return False
        logger.info(f"  parsed {len(ops)} opinions")
        for op in ops[:3] + ops[-2:]:
            logger.info(f"  Opinion {op['number']} ({op['year']}) "
                        f"{len(op['text'])} chars — {op['caption'][:50]}")
        return True

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        caption = (raw.get("caption") or "").title()
        title = f"Texas Judicial Ethics Opinion No. {number} ({raw['year']})"
        if caption:
            title += f": {caption}"
        return {
            "_id": f"US/TX-JudicialEthics/{number}",
            "_source": "US/TX-JudicialEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Committee on Judicial Ethics, State Bar of Texas "
                      "Judicial Section",
            "title": title,
            "text": raw["text"],
            "url": self._pdf_source_url,
            "date": f"{raw['year']}-01-01",
            "jurisdiction": "US-TX",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        pdf = self._get_pdf_bytes()
        if not pdf:
            return
        ops = self._parse_opinions(pdf)
        emitted = 0
        for op in ops:
            yield op
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
            if not since or (f"{raw['year']}-01-01") >= since:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TX-JudicialEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TXJudicialEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
