#!/usr/bin/env python3
"""
US/HI-EthicsOpinions -- Hawaii State Ethics Commission -- Advisory Opinions
(and informal advisory opinions + resolutions of charge/investigation).

Fetches the full text of the opinions and resolutions of the Hawaii State
Ethics Commission (HSEC) issued under the State Ethics Code, Hawai'i Revised
Statutes (HRS) chapter 84 -- the conflict-of-interest, gifts, financial-
disclosure and lobbying provisions. Formal advisory opinions and informal
advisory opinions interpret the ethics code = doctrine; resolutions of a
charge / investigation resolve a specific enforcement matter = case_law. Every
published record is a public record of the State of Hawaii (pd-us).

Access (no CAPTCHA, no auth, no JavaScript engine needed):
  The opinions live in a Salesforce Experience Cloud portal at
  hawaiiethics.my.site.com as records of the custom object Ethics_Advice__c.

  1. ENUMERATION (zero-fetch) -- the public sitemap lists every record URL:
         https://hawaiiethics.my.site.com/public/s/sitemap-ethics_advice__c-1.xml
     Each <loc> is /public/s/ethics-advice/{18charRecordId}/{slug}.

  2. METADATA + FULL-TEXT LINK -- the record page is a Salesforce Aura/LWC
     shell that returns only a "Loading..." skeleton to plain HTTP, BUT the
     record data is obtainable by replaying the Aura getRecord action against
         https://hawaiiethics.my.site.com/public/s/sfsites/aura
     as a guest. The returned Ethics_Advice__c record carries:
         Name            e.g. "AO2020-2", "IAO2020-01", "ROC2020-4"
         Advice_Type__c  "Advisory Opinion" / "Informal Advisory Opinion" /
                         "Settlement of Charge" / "Settlement of Investigation"
         Date_Issued__c  ISO issue date
         Advice_URL__c   direct link to the born-digital opinion PDF on
                         https://files.hawaii.gov/ethics/advice/{file}.pdf
         Public_Keywords__c
     The fwuid + app markup id needed for the Aura context are parsed live from
     a record page each run (they rotate when Salesforce redeploys the site).

  3. FULL TEXT -- download the Advice_URL__c PDF (born-digital text layer;
     OCR fallback for any older scan) and extract via common.pdf_extract.

Type is assigned per record: advisory / informal advisory opinions = doctrine;
settlements/resolutions of a charge or investigation = case_law.

Usage:
  python bootstrap.py bootstrap            # Full pull (all ~909 records)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.HI-EthicsOpinions")

PORTAL = "https://hawaiiethics.my.site.com"
SITEMAP = f"{PORTAL}/public/s/sitemap-ethics_advice__c-1.xml"
AURA_URL = f"{PORTAL}/public/s/sfsites/aura?r=1&aura.RecordUi.getRecord=1"
# Any record page works as a bootstrap page to read the live aura context from.
CTX_PAGE = f"{PORTAL}/public/s/ethics-advice/a1J2K000006zhfrUAA/ao20202"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
REC_RE = re.compile(r"/ethics-advice/([A-Za-z0-9]{15,18})/([^/?<\s]+)")
FWUID_RE = re.compile(r'"fwuid":"([^"]+)"')
APPID_RE = re.compile(
    r'"(APPLICATION@markup://siteforce:communityApp)":"([^"]+)"'
)

# Advice_Type__c values that resolve a specific enforcement matter -> case_law.
CASE_LAW_TYPES = ("settlement", "resolution", "charge", "investigation")


def _classify(advice_type: str) -> str:
    t = (advice_type or "").lower()
    if any(k in t for k in CASE_LAW_TYPES):
        return "case_law"
    return "doctrine"


class HIEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self._ctx = None  # cached aura context dict

    # ---------------------------------------------------------------- http
    def _get(self, url: str, **kw):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True, **kw)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # -------------------------------------------------------- aura context
    def _aura_context(self) -> Optional[dict]:
        """Parse the live fwuid + app markup id from a record page."""
        if self._ctx is not None:
            return self._ctx
        r = self._get(CTX_PAGE)
        if r is None or r.status_code != 200:
            logger.error("Could not load aura context page")
            return None
        idx = r.text.find('"fwuid"')
        if idx < 0:
            idx = r.text.find("fwuid")
        seg = unquote(r.text[max(0, idx - 300):idx + 800])
        fw = FWUID_RE.search(seg)
        app = APPID_RE.search(seg)
        if not fw or not app:
            logger.error("Could not parse fwuid/app from context page")
            return None
        self._ctx = {
            "mode": "PROD",
            "fwuid": fw.group(1),
            "app": "siteforce:communityApp",
            "loaded": {app.group(1): app.group(2)},
            "dns": "c",
            "globals": {},
            "uad": False,
        }
        logger.info(f"Aura context OK (fwuid {fw.group(1)[:16]}...)")
        return self._ctx

    def _get_record(self, rid: str) -> Optional[dict]:
        ctx = self._aura_context()
        if ctx is None:
            return None
        actions = [{
            "id": "1;a",
            "descriptor": ("serviceComponent://ui.force.components.controllers."
                           "detail.DetailController/ACTION$getRecord"),
            "callingDescriptor": "UNKNOWN",
            "params": {
                "recordId": rid, "record": None, "inContextOfComponent": "",
                "mode": "VIEW", "layoutType": "FULL",
                "defaultFieldValues": None, "navigationLocation": "LIST_VIEW_ROW",
            },
        }]
        data = {
            "message": json.dumps({"actions": actions}),
            "aura.context": json.dumps(ctx),
            "aura.pageURI": f"/public/s/ethics-advice/{rid}",
            "aura.token": "null",
        }
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                resp = self.session.post(AURA_URL, data=data, timeout=60)
                j = resp.json()
                return j["actions"][0]["returnValue"]["record"]
            except Exception as e:
                logger.warning(f"  aura getRecord {rid} attempt {attempt+1}: {e}")
                # a 401/expired fwuid -> refresh context and retry
                self._ctx = None
                ctx = self._aura_context()
                if ctx:
                    data["aura.context"] = json.dumps(ctx)
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _sitemap_records(self) -> list[dict]:
        r = self._get(SITEMAP)
        if r is None or r.status_code != 200:
            logger.error("Could not fetch sitemap")
            return []
        out: list[dict] = []
        seen: set[str] = set()
        for loc in LOC_RE.findall(r.text):
            m = REC_RE.search(loc)
            if not m:
                continue
            rid = m.group(1)
            if rid in seen:
                continue
            seen.add(rid)
            out.append({"record_id": rid, "slug": m.group(2), "page_url": loc})
        logger.info(f"Sitemap: {len(out)} Ethics_Advice__c records")
        return out

    # ------------------------------------------------------------- fetch1
    def _fetch_one(self, row: dict) -> Optional[dict]:
        rec = self._get_record(row["record_id"])
        if not rec:
            logger.warning(f"  {row['slug']}: no record data — skipped")
            return None
        pdf_url = rec.get("Advice_URL__c")
        name = rec.get("Name") or row["slug"]
        if not pdf_url:
            logger.warning(f"  {name}: no Advice_URL__c — skipped")
            return None
        pr = self._get(pdf_url)
        if pr is None or pr.status_code != 200 or not pr.content:
            logger.warning(f"  {name}: PDF download failed ({pdf_url}) — skipped")
            return None
        if not pr.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {name}: not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(pr.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {name}: thin text ({len(text)} chars) — skipped")
            return None
        advice_type = rec.get("Advice_Type__c") or ""
        return {
            "record_id": row["record_id"],
            "name": name,
            "advice_type": advice_type,
            "type": _classify(advice_type),
            "date": rec.get("Date_Issued__c"),
            "keywords": rec.get("Public_Keywords__c"),
            "pdf_url": pr.url,
            "page_url": row["page_url"],
            "text": text,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._sitemap_records():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['name']} OK ({rec['type']}, "
                            f"{len(rec['text'])} chars, date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Hawaii State Ethics Commission opinions...")
        rows = self._sitemap_records()
        if len(rows) < 100:
            logger.error(f"API test FAILED: sitemap too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:5]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['name']} OK ({rec['type']}, "
                            f"{len(rec['text'])} chars)")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        name = raw.get("name")
        return {
            "_id": f"US/HI-EthicsOpinions/{name}",
            "_source": "US/HI-EthicsOpinions",
            "_type": raw.get("type", "doctrine"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": name,
            "document_type": raw.get("advice_type") or "Advisory Opinion",
            "issuer": "Hawaii State Ethics Commission",
            "title": f"Hawaii State Ethics Commission {raw.get('advice_type') or 'Opinion'} {name}",
            "text": raw["text"],
            "keywords": raw.get("keywords"),
            "url": raw.get("page_url") or raw.get("pdf_url"),
            "pdf_url": raw.get("pdf_url"),
            "date": raw.get("date"),
            "jurisdiction": "US-HI",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/HI-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HIEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
