#!/usr/bin/env python3
"""
KR/NTS-TaxLaw -- South Korean Tax Law Information System (국세법령정보시스템)

Fetches tax doctrine from the Korean National Tax Service via their JSON API.
Covers ~291,000 documents: interpretations (pre-rulings, inquiry-responses,
consultations) and precedents (objections, examinations, tribunal, court cases).

API endpoint: POST https://taxlaw.nts.go.kr/action.do
  - actionId=ASIPDI002PR01: search/list documents (paginated)
  - actionId=ASIQTB002PR01: fetch full document detail with HTML content

Full text is returned in dcmHwpEditorDVOList[].dcmFleByte (HTML format).
Server requires TLS 1.2.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import ssl
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.util.ssl_ import create_urllib3_context

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.NTS-TaxLaw")

API_URL = "https://taxlaw.nts.go.kr/action.do"
DELAY = 1.5

# Collections
COLLECTIONS = {
    "question,question_gr": "Interpretations",
    "precedent,precedent_gr": "Precedents",
}

# Document class codes
DOC_CLASS_NAMES = {
    "01": "Pre-ruling (사전답변)",
    "02": "Inquiry-response (질의회신)",
    "03": "Tax standard consultation (과세기준자문)",
    "04": "Official written inquiry (고시서면질의)",
    "05": "Tax appropriateness (과세적부)",
    "06": "Objection (이의신청)",
    "07": "Examination request (심사청구)",
    "08": "Tribunal request (심판청구)",
    "09": "Court case (판례)",
    "10": "Constitutional court (헌재)",
    "31": "Maintenance/revision (정비)",
}

# Detail action IDs per collection type
DETAIL_ACTIONS = {
    "question,question_gr": "ASIQTB002PR01",
    "precedent,precedent_gr": "ASIPDV002PR01",
}


class TLS12Adapter(HTTPAdapter):
    """Force TLS 1.2+ — required by taxlaw.nts.go.kr."""
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)


def strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def format_date(raw_date: str) -> Optional[str]:
    """Convert YYYYMMDD to ISO 8601 date string."""
    if not raw_date or len(raw_date) < 8:
        return None
    try:
        return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    except Exception:
        return None


class NTSTaxLaw(BaseScraper):
    SOURCE_ID = "KR/NTS-TaxLaw"

    def __init__(self):
        self.session = requests.Session()
        retry = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", TLS12Adapter(max_retries=retry))
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        })

    def _post_action(self, action_id: str, param_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Post to action.do and return the inner data for the action."""
        body = urlencode({
            "actionId": action_id,
            "paramData": json.dumps(param_data, ensure_ascii=False),
        })
        for attempt in range(6):
            try:
                resp = self.session.post(API_URL, data=body, timeout=30)
                time.sleep(DELAY)
                if resp.status_code != 200:
                    return None
                result = resp.json()
                if result.get("status") != "SUCCESS":
                    return None
                return result.get("data", {}).get(action_id, {})
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 2 * (attempt + 1)
                logger.warning("Attempt %d failed for %s: %s. Retrying in %ds...", attempt + 1, action_id, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.warning("Request failed for %s: %s", action_id, e)
                time.sleep(2)
                return None
        logger.error("All retries exhausted for %s", action_id)
        return None

    def search_documents(
        self,
        collection: str,
        start_count: int = 1,
        view_count: int = 50,
        session_uuid: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Search for documents in a collection. Returns inner data dict."""
        params: Dict[str, Any] = {
            "sortField": "DCM_RGT_DTM/DESC",
            "startCount": start_count,
            "viewCount": view_count,
            "collectionName": collection,
        }
        if session_uuid:
            params["wnSessionUuid"] = session_uuid
        if start_date:
            params["bltnStrtDt"] = start_date
            params["schDtBase"] = "DCM_RGT_DTM"
        if end_date:
            params["bltnEndDt"] = end_date
        return self._post_action("ASIPDI002PR01", params)

    def fetch_document(self, doc_id: str, collection: str) -> Optional[Dict[str, Any]]:
        """Fetch full document detail by ID."""
        action_id = DETAIL_ACTIONS.get(collection, "ASIQTB002PR01")
        params = {"dcmDVO": {"ntstDcmId": doc_id}}
        return self._post_action(action_id, params)

    def extract_text(self, detail: Dict[str, Any]) -> str:
        """Extract full text from document detail response."""
        texts = []

        # Primary: dcmHwpEditorDVOList contains full HTML content
        editor_list = detail.get("dcmHwpEditorDVOList") or []
        for entry in editor_list:
            if entry.get("dcmFleTy") == "html" and entry.get("dcmFleByte"):
                raw_html = entry["dcmFleByte"]
                cleaned = strip_html(raw_html)
                if cleaned:
                    texts.append(cleaned)

        if texts:
            return "\n\n".join(texts)

        # Fallback: dcmDVO content fields
        dvo = detail.get("dcmDVO") or {}
        for field in ["ntstDcmCntn", "ntstDcmGistCntn", "ntstDcmRplyCntn", "ntstDcmDscmCntn"]:
            val = dvo.get(field, "")
            if val:
                cleaned = strip_html(val) if "<" in val else val.strip()
                if cleaned:
                    texts.append(cleaned)

        return "\n\n".join(texts)

    def normalize(self, search_dcm: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a document into the standard schema."""
        dvo = detail.get("dcmDVO") or {}
        doc_id = dvo.get("ntstDcmId") or search_dcm.get("DOC_ID", "")
        doc_class = dvo.get("ntstDcmClCd") or search_dcm.get("NTST_DCM_CL_CD", "")
        raw_date = dvo.get("ntstDcmRgtDt") or search_dcm.get("DCM_RGT_DTM", "")[:8]
        title = dvo.get("ntstDcmTtl") or search_dcm.get("TTL", "")
        text = self.extract_text(detail)
        ref_number = dvo.get("dsbdHpnnNo", "")

        return {
            "_id": str(doc_id),
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": format_date(str(raw_date)),
            "url": f"https://taxlaw.nts.go.kr/qt/USEQTA002P.do?ntstDcmId={doc_id}",
            "language": "ko",
            "document_class": DOC_CLASS_NAMES.get(str(doc_class), str(doc_class)),
            "reference_number": ref_number,
            "tax_type": dvo.get("ntstTlawClCd", ""),
            "summary": dvo.get("ntstDcmGistCntn", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax law doctrine documents."""
        total_yielded = 0
        sample_limit = 15 if sample else None

        for collection, coll_name in COLLECTIONS.items():
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info("Fetching %s...", coll_name)

            # First page
            data = self.search_documents(collection, start_count=1, view_count=50)
            if not data:
                logger.warning("Failed to search %s", coll_name)
                continue

            items = data.get("body", [])

            # Get total from category counts
            top = data.get("top", [])
            total_count = 0
            if top and top[0].get("categoryMap"):
                cats = top[0]["categoryMap"].get("SUB_ID_CATEGORY", [])
                for cat in cats:
                    if isinstance(cat, dict):
                        total_count += int(cat.get("count", 0))

            # Get session UUID for pagination
            session_uuid = None
            uuid_list = data.get("wnSessionUuid", [])
            if uuid_list and isinstance(uuid_list, list) and uuid_list:
                session_uuid = uuid_list[0].get("wnSessionUuid")

            logger.info("  %s: ~%d documents", coll_name, total_count)

            page = 1
            while True:
                if page > 1:
                    data = self.search_documents(
                        collection, start_count=page, view_count=50,
                        session_uuid=session_uuid,
                    )
                    if not data:
                        break
                    items = data.get("body", [])

                if not items:
                    break

                for item in items:
                    if sample_limit and total_yielded >= sample_limit:
                        break

                    dcm = item.get("dcm", {})
                    doc_id = dcm.get("DOC_ID", "")
                    if not doc_id:
                        continue

                    detail = self.fetch_document(doc_id, collection)
                    if not detail:
                        logger.warning("Failed to fetch document %s", doc_id)
                        continue

                    record = self.normalize(dcm, detail)
                    if not record["text"]:
                        logger.warning("Empty text for %s: %s", doc_id, record["title"][:60])
                        continue

                    yield record
                    total_yielded += 1

                    if total_yielded % 50 == 0:
                        logger.info("  Progress: %d documents fetched", total_yielded)

                if sample_limit and total_yielded >= sample_limit:
                    break

                page += 1

            logger.info("  Done with %s. Total so far: %d", coll_name, total_yielded)

        logger.info("Fetch complete. Total documents: %d", total_yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents modified since a given date (YYYY-MM-DD)."""
        since_compact = since.replace("-", "")
        for collection, coll_name in COLLECTIONS.items():
            logger.info("Checking updates for %s since %s...", coll_name, since)

            data = self.search_documents(
                collection, start_count=1, view_count=50, start_date=since_compact
            )
            if not data:
                continue

            items = data.get("body", [])
            session_uuid = None
            uuid_list = data.get("wnSessionUuid", [])
            if uuid_list and isinstance(uuid_list, list) and uuid_list:
                session_uuid = uuid_list[0].get("wnSessionUuid")

            page = 1
            while True:
                if page > 1:
                    data = self.search_documents(
                        collection, start_count=page, view_count=50,
                        session_uuid=session_uuid, start_date=since_compact,
                    )
                    if not data:
                        break
                    items = data.get("body", [])

                if not items:
                    break

                for item in items:
                    dcm = item.get("dcm", {})
                    doc_id = dcm.get("DOC_ID", "")
                    if not doc_id:
                        continue
                    detail = self.fetch_document(doc_id, collection)
                    if not detail:
                        continue
                    record = self.normalize(dcm, detail)
                    if record["text"]:
                        yield record

                page += 1

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            data = self.search_documents("question,question_gr", start_count=1, view_count=1)
            if not data:
                logger.error("Test failed: no data returned")
                return False
            items = data.get("body", [])
            top = data.get("top", [])
            total = 0
            if top and top[0].get("categoryMap"):
                cats = top[0]["categoryMap"].get("SUB_ID_CATEGORY", [])
                for cat in cats:
                    if isinstance(cat, dict):
                        total += int(cat.get("count", 0))
            logger.info("Test passed: %d interpretation docs available, %d items returned", total, len(items))
            return len(items) > 0
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse
    import warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    parser = argparse.ArgumentParser(description="KR/NTS-TaxLaw bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = NTSTaxLaw()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', str(record['_id']))
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                "  [%d] %s | %s | text=%d chars",
                count, record["date"], record["title"][:60], text_len
            )

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
