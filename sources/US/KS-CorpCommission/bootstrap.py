#!/usr/bin/env python3
"""
US/KS-CorpCommission -- Kansas Corporation Commission (KCC) Orders

Fetches the full text of Orders issued by the Kansas Corporation Commission
(KCC) adjudicating utility, energy (oil & gas conservation) and transportation
dockets across the electric, natural-gas, telecom, water, oil-&-gas and motor-
carrier industries (rate cases, certificate applications, complaints, show-cause
and conservation orders, procedural + final orders). Each Order is an
administrative adjudication of a specific docket by the Commission = case_law.
Public domain (US state government edict).

Strategy (public "KCC Connect" Salesforce Experience Cloud community,
kcc-connect.kcc.ks.gov/s/):
  1. DISCOVERY: the community publishes a public SEO sitemap
     /s/sitemap.xml → per-object shards /s/sitemap-order__c-{N}.xml
     (~20K Order__c record URLs each, ~60-80K Orders total). Each URL is
     /s/order/{Order__c-id}/{slug}.
  2. METADATA: replay the community's own GUEST Aura endpoint
     (POST /s/sfsites/aura, aura://RecordUiController/ACTION$getRecordsWithFields,
     batched 50 ids/call, aura.token="null") to read Name (order number),
     Docket__c (an <a> to the case, anchor text = docket number), OrderDate__c,
     Title__c, Type__c, Publicly_Available__c/DocumentAccess__c and
     PreviewUrl__c — a PUBLIC SpringCM "DownloadPdf" URL for the born-digital
     Order PDF.
  3. FULL TEXT: normalize() downloads the born-digital Order PDF directly from
     PreviewUrl__c (shareus11.springcm.com/Public/DownloadPdf/..., guest-
     downloadable 200 application/pdf) and extracts the full text via
     fitz/PyMuPDF (Tesseract OCR fallback for the rare image-only scan).

The Aura framework UID (fwuid) and app-load id rotate on redeploy, so they are
parsed live from the community home page and re-parsed on a 401.

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
logger = logging.getLogger("legal-data-hunter.US.KS-CorpCommission")

BASE = "https://kcc-connect.kcc.ks.gov"
SITEMAP_INDEX = f"{BASE}/s/sitemap.xml"
AURA_URL = f"{BASE}/s/sfsites/aura"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

ORDER_FIELDS = [
    "Order__c.Name",
    "Order__c.Docket__c",
    "Order__c.RelatedDockets__c",
    "Order__c.CaseSubject__c",
    "Order__c.TrackingNumber__c",
    "Order__c.OrderDate__c",
    "Order__c.MeetingDate__c",
    "Order__c.Title__c",
    "Order__c.Synopsis__c",
    "Order__c.Type__c",
    "Order__c.Subtype__c",
    "Order__c.Status__c",
    "Order__c.Order_Outcome__c",
    "Order__c.Service_Type__c",
    "Order__c.DocumentAccess__c",
    "Order__c.Publicly_Available__c",
    "Order__c.PreviewUrl__c",
]
BATCH = 50

ORDER_URL_RE = re.compile(r"/s/order/([A-Za-z0-9]{15,18})/")
ANCHOR_TEXT_RE = re.compile(r">([^<]+)</a>")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_html(val: str) -> str:
    if not val:
        return ""
    # Docket__c is an <a href=...>DOCKET</a>; prefer the anchor text.
    m = ANCHOR_TEXT_RE.search(val)
    if m:
        return m.group(1).strip()
    return re.sub(r"<[^>]+>", "", val).strip()


class KSCCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.delay = 0.4
        self._ctx = None
        self.ckpt_path = Path(source_dir) / "data" / "ks_cc_checkpoint.json"

    # ---- Aura plumbing -------------------------------------------------

    def _aura_context(self) -> dict:
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
                elif r.status_code in (401, 403):
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
            {"recordIds": record_ids, "fields": ORDER_FIELDS, "optionalFields": []},
        )
        if not rv or "results" not in rv:
            return [None] * len(record_ids)
        out = []
        for res in rv["results"]:
            rec = res.get("result")
            if not rec or "fields" not in rec:
                out.append(None)
                continue
            out.append(rec["fields"])
        return out

    # ---- Discovery -----------------------------------------------------

    def _order_sitemaps(self) -> list:
        r = self.session.get(SITEMAP_INDEX, timeout=60)
        locs = re.findall(r"<loc>([^<]+sitemap-order__c-\d+\.xml)</loc>", r.text)
        return locs

    def _sitemap_ids(self, url: str) -> list:
        r = self.session.get(url, timeout=90)
        seen = []
        for rid in ORDER_URL_RE.findall(r.text):
            if rid not in seen:
                seen.append(rid)
        return seen

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

    def _download_pdf(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90, allow_redirects=True)
                if r.status_code == 200 and r.content[:5] == b"%PDF-":
                    return r.content
                if r.status_code == 404:
                    return None
            except Exception as e:
                logger.warning(f"PDF download error (try {attempt+1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- Raw dict builder ----------------------------------------------

    @staticmethod
    def _fv(fields: dict, key: str):
        return (fields.get(key) or {}).get("value")

    @classmethod
    def _is_public_order(cls, fields: dict) -> bool:
        if not fields:
            return False
        pub = cls._fv(fields, "Publicly_Available__c")
        access = cls._fv(fields, "DocumentAccess__c")
        preview = cls._fv(fields, "PreviewUrl__c")
        public = bool(pub) or (access == "Public")
        return public and bool(preview)

    @classmethod
    def _raw_from_fields(cls, rid: str, fields: dict) -> dict | None:
        preview = cls._fv(fields, "PreviewUrl__c")
        if not preview:
            return None
        name = cls._fv(fields, "Name") or ""
        docket = strip_html(cls._fv(fields, "Docket__c") or "")
        if not docket:
            docket = (cls._fv(fields, "CaseSubject__c")
                      or cls._fv(fields, "TrackingNumber__c") or "")
        date = cls._fv(fields, "OrderDate__c") or cls._fv(fields, "MeetingDate__c")
        if isinstance(date, str) and len(date) > 10:
            date = date[:10]
        return {
            "id": rid,
            "name": name,
            "docket": docket.strip() or None,
            "title": (cls._fv(fields, "Title__c") or "").strip(),
            "otype": cls._fv(fields, "Type__c"),
            "subtype": cls._fv(fields, "Subtype__c"),
            "status": cls._fv(fields, "Status__c"),
            "outcome": cls._fv(fields, "Order_Outcome__c"),
            "service_type": cls._fv(fields, "Service_Type__c"),
            "preview_url": preview,
            "date": date,
            "url": f"{BASE}/s/order/{rid}/{name}",
        }

    # ---- Framework hooks -----------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._download_pdf(raw["preview_url"])
        if not pdf_bytes:
            return None
        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for {raw['name']}")
            return None

        docket = raw.get("docket")
        title = raw.get("title") or ""
        if docket and title:
            full_title = f"KCC Order — Docket {docket}: {title}"
        elif docket:
            full_title = f"KCC Order — Docket {docket}"
        elif title:
            full_title = f"KCC Order — {title}"
        else:
            full_title = f"KCC Order — {raw.get('name') or raw['id']}"

        return {
            "_id": f"US/KS-CorpCommission/{raw['id']}",
            "_source": "US/KS-CorpCommission",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "order_number": raw.get("name"),
            "docket": docket,
            "order_type": raw.get("otype"),
            "outcome": raw.get("outcome"),
            "service_type": raw.get("service_type"),
            "title": full_title,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["preview_url"],
            "date": raw.get("date"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        shards = self._order_sitemaps()
        logger.info(f"{len(shards)} order sitemap shards")
        ck = self._load_ckpt()
        done = set(ck.get("done_shards", []))
        seen = set()
        for shard in shards:
            if shard in done:
                continue
            ids = self._sitemap_ids(shard)
            logger.info(f"{shard.rsplit('/',1)[-1]}: {len(ids)} orders")
            start = ck["offset"] if ck.get("shard") == shard else 0
            for off in range(start, len(ids), BATCH):
                batch = ids[off:off + BATCH]
                metas = self._get_meta_batch(batch)
                for rid, fields in zip(batch, metas):
                    if rid in seen or not self._is_public_order(fields):
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
        logger.info("Testing KCC Connect order community...")
        try:
            shards = self._order_sitemaps()
            if not shards:
                logger.error("  No order sitemaps found")
                return False
            logger.info(f"  {len(shards)} order sitemap shards")
            ids = self._sitemap_ids(shards[0])
            logger.info(f"  shard 1: {len(ids)} orders")
            rec = None
            for off in range(0, min(len(ids), 300), BATCH):
                batch = ids[off:off + BATCH]
                metas = self._get_meta_batch(batch)
                for rid, fields in zip(batch, metas):
                    if self._is_public_order(fields):
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
                    f"order {rec['order_number']}, docket {rec.get('docket')}, "
                    f"date={rec.get('date')})"
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

    parser = argparse.ArgumentParser(description="US/KS-CorpCommission bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"]
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = KSCCScraper()

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
