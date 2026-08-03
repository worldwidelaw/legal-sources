#!/usr/bin/env python3
"""
US/MI-PSC -- Michigan Public Service Commission Orders (E-Dockets)

Fetches the full text of Orders issued by the Michigan Public Service
Commission (MPSC) resolving utility dockets (electric, natural-gas,
water, telecommunications, video/cable). Each Order is an administrative
adjudication of a specific case by the Commission = case_law. Public
domain (US state government edict).

Strategy (official MPSC E-Dockets, a public Salesforce Experience Cloud
community at https://mi-psc.my.site.com):

  1. Discovery — the community publishes a public SEO sitemap index at
     /s/sitemap.xml which links per-object sitemaps for the Filing__c
     object (/s/sitemap-filing__c-{N}.xml). Each shard lists ~20,000
     filing detail-page URLs of the form
     /s/filing/{RecordId}/{FilingNumber}; ~180K filings total.

  2. Metadata — for each filing RecordId we replay the community's own
     guest Aura endpoint (POST /s/sfsites/aura,
     aura://RecordUiController/ACTION$getRecordsWithFields, batched 50 at a
     time, aura.token="null") to read Filing_Type__c, Public__c,
     File_Link_Internal__c (the document's Salesforce ContentVersion
     download URL), File_Date__c and the filing Name (e.g. "U-21000-0003",
     whose stem "U-21000" is the case number). We keep only
     Filing_Type__c == "Order" that are Public__c == true with a document
     link — the binding Commission Orders.

  3. Full text — normalize() downloads the born-digital Order PDF from the
     Salesforce shepherd endpoint (rewritten to the guest-accessible
     community host) and extracts the text with fitz/PyMuPDF (Tesseract
     OCR fallback for the rare image-only scan).

The Aura framework UID (fwuid) and app-load id rotate on redeploy, so they
are parsed live from the community home page on startup.

fetch_all() checkpoints its progress (which sitemap shard + batch offset it
has consumed) to data/mi_psc_checkpoint.json so fleet reruns advance
monotonically over the ~180K-filing scan instead of restarting.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MI-PSC")

BASE = "https://mi-psc.my.site.com"
SITEMAP_INDEX = f"{BASE}/s/sitemap.xml"
AURA_URL = f"{BASE}/s/sfsites/aura"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

FILING_FIELDS = [
    "Filing__c.Name",
    "Filing__c.Filing_Type__c",
    "Filing__c.Public__c",
    "Filing__c.File_Link_Internal__c",
    "Filing__c.File_Date__c",
    "Filing__c.Filing_Description__c",
]
BATCH = 50

SHEPHERD_RE = re.compile(r"/sfc/servlet\.shepherd/version/download/([A-Za-z0-9]+)")
FILING_URL_RE = re.compile(r"/s/filing/([A-Za-z0-9]{15,18})/")
CASENO_RE = re.compile(r"Case\s+No\.?\s*[:\s]*([A-Z]-\d{3,6}(?:-\d+)?)", re.I)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class MIPSCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.delay = 0.4
        self._ctx = None
        self.ckpt_path = Path(source_dir) / "data" / "mi_psc_checkpoint.json"

    # ---- Aura plumbing -------------------------------------------------

    def _aura_context(self) -> dict:
        """Parse the live fwuid + app-load id from the community home page."""
        if self._ctx:
            return self._ctx
        r = self.session.get(f"{BASE}/s/", timeout=60)
        html = r.text
        fw = re.search(r"fwuid%22%3A%22([^%]+)%22", html)
        app = re.search(
            r"APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22([^%]+)%22",
            html,
        )
        if not fw or not app:
            raise RuntimeError("Could not parse Aura fwuid/app id from home page")
        self._ctx = {
            "mode": "PROD",
            "fwuid": fw.group(1),
            "app": "siteforce:communityApp",
            "loaded": {
                "APPLICATION@markup://siteforce:communityApp": app.group(1)
            },
            "dn": [],
            "globals": {},
            "uad": False,
        }
        logger.info(f"Aura context: fwuid={fw.group(1)[:16]}...")
        return self._ctx

    def _aura_call(self, descriptor: str, params: dict, retries: int = 4):
        ctx = self._aura_context()
        msg = {
            "actions": [
                {
                    "id": "1;a",
                    "descriptor": descriptor,
                    "callingDescriptor": "UNKNOWN",
                    "params": params,
                }
            ]
        }
        data = {
            "message": json.dumps(msg),
            "aura.context": json.dumps(ctx),
            "aura.pageURI": "/s/",
            "aura.token": "null",
        }
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.post(
                    f"{AURA_URL}?a=1",
                    data=data,
                    headers={"X-SFDC-Page-Scope-Id": "x"},
                    timeout=60,
                )
                if r.status_code == 200:
                    j = r.json()
                    act = j["actions"][0]
                    if act["state"] == "SUCCESS":
                        return act["returnValue"]
                    logger.warning(f"Aura action state {act['state']}")
                elif r.status_code == 401:
                    # fwuid rotated — re-parse context and retry
                    self._ctx = None
                else:
                    logger.warning(f"Aura HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"Aura call error (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_meta_batch(self, record_ids: list) -> list:
        rv = self._aura_call(
            "aura://RecordUiController/ACTION$getRecordsWithFields",
            {"recordIds": record_ids, "fields": FILING_FIELDS, "optionalFields": []},
        )
        if not rv or "results" not in rv:
            return []
        out = []
        for res in rv["results"]:
            rec = res.get("result")
            if not rec or "fields" not in rec:
                out.append(None)
                continue
            out.append(rec["fields"])
        return out

    # ---- Discovery -----------------------------------------------------

    def _filing_sitemaps(self) -> list:
        r = self.session.get(SITEMAP_INDEX, timeout=60)
        locs = re.findall(r"<loc>([^<]+sitemap-filing__c-\d+\.xml)</loc>", r.text)
        return locs

    def _sitemap_ids(self, url: str) -> list:
        r = self.session.get(url, timeout=90)
        return FILING_URL_RE.findall(r.text)

    # ---- Checkpoint ----------------------------------------------------

    def _load_ckpt(self) -> dict:
        try:
            return json.loads(self.ckpt_path.read_text())
        except Exception:
            return {"done_shards": [], "shard": None, "offset": 0}

    def _save_ckpt(self, ck: dict):
        try:
            self.ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            self.ckpt_path.write_text(json.dumps(ck))
        except Exception as e:
            logger.debug(f"checkpoint save failed: {e}")

    # ---- PDF text ------------------------------------------------------

    def _ocr_pdf(self, doc) -> str:
        try:
            import pytesseract
            from PIL import Image
            import io
        except Exception:
            return ""
        parts = []
        for page in doc:
            try:
                pix = page.get_pixmap(dpi=200)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                parts.append(pytesseract.image_to_string(img))
            except Exception as e:
                logger.debug(f"OCR page failed: {e}")
        return "\n".join(parts)

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open failed: {e}")
            return ""
        try:
            text = clean_text("\n".join(page.get_text() for page in doc))
            if len(text) < 200:
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def _download_pdf(self, version_id: str, retries: int = 4) -> bytes | None:
        url = (
            f"{BASE}/sfc/servlet.shepherd/version/download/"
            f"{version_id}?operationContext=S1"
        )
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90, allow_redirects=True)
                if r.status_code == 200 and r.content[:5] == b"%PDF-":
                    return r.content
                if r.status_code == 404:
                    return None
            except Exception as e:
                logger.warning(f"PDF download error {version_id} (try {attempt+1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- Raw dict builder ----------------------------------------------

    @staticmethod
    def _is_order(fields: dict) -> bool:
        if not fields:
            return False
        ft = (fields.get("Filing_Type__c") or {}).get("value")
        pub = (fields.get("Public__c") or {}).get("value")
        link = (fields.get("File_Link_Internal__c") or {}).get("value")
        return ft == "Order" and bool(pub) and bool(link)

    @staticmethod
    def _raw_from_fields(rid: str, fields: dict) -> dict | None:
        link = (fields.get("File_Link_Internal__c") or {}).get("value") or ""
        m = SHEPHERD_RE.search(link)
        if not m:
            return None
        version_id = m.group(1)
        name = (fields.get("Name") or {}).get("value") or ""
        case_number = re.sub(r"-\d+$", "", name) if name else None
        return {
            "id": rid,
            "name": name,
            "case_number": case_number,
            "version_id": version_id,
            "date": (fields.get("File_Date__c") or {}).get("value"),
            "description": (fields.get("Filing_Description__c") or {}).get("value"),
            "url": f"{BASE}/s/filing/{rid}/{name}",
        }

    # ---- Framework hooks -----------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._download_pdf(raw["version_id"])
        if not pdf_bytes:
            return None
        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for {raw['name']}")
            return None

        # The filing's own docket (from its Name, e.g. "U-21000-0003" ->
        # "U-21000") is the authoritative per-filing case number. Fall back to
        # the body's "Case No." only when the filing carries no docket stem
        # (the body reference can point to the lead case of a consolidated
        # matter rather than this filing's own docket).
        case_number = raw.get("case_number")
        if not case_number:
            cm = CASENO_RE.search(text[:2000])
            if cm:
                case_number = cm.group(1).upper()

        title = f"MPSC Order — Case No. {case_number}" if case_number else "MPSC Order"
        desc = (raw.get("description") or "").strip()
        if desc and 3 <= len(desc) <= 300:
            title = f"{title}: {desc}"

        return {
            "_id": f"US/MI-PSC/{raw['id']}",
            "_source": "US/MI-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "filing_number": raw.get("name"),
            "title": title,
            "text": text,
            "url": raw["url"],
            "pdf_url": (
                f"{BASE}/sfc/servlet.shepherd/version/download/"
                f"{raw['version_id']}?operationContext=S1"
            ),
            "date": raw.get("date"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        shards = self._filing_sitemaps()
        logger.info(f"{len(shards)} filing sitemap shards")
        ck = self._load_ckpt()
        done = set(ck.get("done_shards", []))
        seen = set()
        for shard in shards:
            if shard in done:
                continue
            ids = self._sitemap_ids(shard)
            logger.info(f"{shard.rsplit('/',1)[-1]}: {len(ids)} filings")
            start = ck["offset"] if ck.get("shard") == shard else 0
            for off in range(start, len(ids), BATCH):
                batch = ids[off:off + BATCH]
                metas = self._get_meta_batch(batch)
                for rid, fields in zip(batch, metas):
                    if not self._is_order(fields) or rid in seen:
                        continue
                    seen.add(rid)
                    raw = self._raw_from_fields(rid, fields)
                    if raw:
                        yield raw
                ck = {"done_shards": sorted(done), "shard": shard, "offset": off + BATCH}
                self._save_ckpt(ck)
            done.add(shard)
            ck = {"done_shards": sorted(done), "shard": None, "offset": 0}
            self._save_ckpt(ck)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing MPSC E-Dockets...")
        try:
            shards = self._filing_sitemaps()
            if not shards:
                logger.error("  No filing sitemaps found")
                return False
            logger.info(f"  {len(shards)} filing sitemap shards")
            ids = self._sitemap_ids(shards[0])
            logger.info(f"  shard 1: {len(ids)} filings")
            rec = None
            for off in range(0, min(len(ids), 600), BATCH):
                batch = ids[off:off + BATCH]
                metas = self._get_meta_batch(batch)
                for rid, fields in zip(batch, metas):
                    if self._is_order(fields):
                        raw = self._raw_from_fields(rid, fields)
                        if raw:
                            rec = self.normalize(raw)
                            if rec:
                                break
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"{rec['filing_number']}, date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Could not extract a full-text Order")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MI-PSC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"]
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = MIPSCScraper()

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
