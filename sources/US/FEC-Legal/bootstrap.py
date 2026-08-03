#!/usr/bin/env python3
"""
US/FEC-Legal -- U.S. Federal Election Commission: Advisory Opinions + Enforcement Matters (MURs)

Fetches the FEC's legal corpus via the official, documented open REST API
(api.open.fec.gov) plus full-text PDF documents from fec.gov.

Two document classes, both public-domain U.S. government works:
  - Advisory Opinions (AOs)  -> _type "doctrine"  (~970 formal interpretive opinions)
  - Enforcement matters (MURs, "Matters Under Review") -> _type "case_law" (~4,800 closed
    enforcement cases: General Counsel reports, Factual & Legal Analyses, Commission
    certifications, conciliation & settlement agreements)

Data access:
  - Index/search: https://api.open.fec.gov/v1/legal/search/?type={advisory_opinions|murs}
  - Documents:    https://www.fec.gov{document.url}   (born-digital PDFs)

API key: the FEC/api.data.gov API requires a key. DEMO_KEY works out of the box but is
rate-limited (~30 req/hr) — fine for sampling. For a full run, set FEC_API_KEY to a free
key from https://api.data.gov/signup/ (instant, no cost).

Usage:
  python bootstrap.py bootstrap          # Full pull (needs FEC_API_KEY for volume)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py bootstrap-fast     # Alias for full bootstrap (VPS wrapper)
  python bootstrap.py update [SINCE]     # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FEC-Legal")

API_BASE = "https://api.open.fec.gov/v1/legal/search/"
DOC_BASE = "https://www.fec.gov"
API_KEY = os.environ.get("FEC_API_KEY", "DEMO_KEY")
HITS_PER_PAGE = 20  # FEC legal search caps hits_returned at 20
DELAY = 1.5


def _get_with_retry(session: requests.Session, url: str, *,
                    params: Dict[str, Any] = None, timeout: int = 60,
                    max_retries: int = 6) -> requests.Response:
    """GET with backoff on 429/503 so a transient rate-limit does not kill the
    whole run (see #1114). Honors Retry-After; exponential backoff capped at 120s.
    DEMO_KEY is capped at ~30 req/hr so it will still exhaust — set FEC_API_KEY to
    a free api.data.gov key for a full run — but a real key's brief 429 bursts now
    survive instead of raising exit 1 mid-corpus."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            last_exc = e
            time.sleep(min(2 ** attempt, 120))
            continue
        if resp.status_code in (429, 500, 502, 503, 504):
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else min(2 ** attempt * 5, 120)
            except ValueError:
                wait = min(2 ** attempt * 5, 120)
            hint = ""
            if resp.status_code == 429:
                hint = (" (DEMO_KEY 30/hr cap — set FEC_API_KEY for a full run)"
                        if API_KEY == "DEMO_KEY" else " (rate limit)")
            elif resp.status_code >= 500:
                hint = " (FEC API server error — transient)"
            logger.warning(
                f"  HTTP {resp.status_code} from FEC API — backing off {wait:.0f}s "
                f"(attempt {attempt + 1}/{max_retries}){hint}"
            )
            time.sleep(wait)
            last_exc = requests.HTTPError(f"{resp.status_code} after retries", response=resp)
            continue
        return resp
    if last_exc:
        raise last_exc
    raise requests.HTTPError("exhausted retries")


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36 "
            "LegalDataHunter/1.0 (research; zacharie@goodlegal.fr)"
        ),
        "Accept": "application/json,application/pdf,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """FEC dates arrive as ISO like '2025-04-30T00:00:00' — keep the date part."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        return m.group(1) if m else None


def extract_pdf_text(pdf_bytes: bytes, source_id: str) -> str:
    return extract_pdf_markdown(
        source="US/FEC-Legal",
        source_id=source_id,
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


def fetch_doc_text(session: requests.Session, doc: Dict[str, Any], source_id: str) -> str:
    """Download one legal document PDF and extract its text."""
    rel = doc.get("url") or ""
    if not rel:
        return ""
    url = rel if rel.startswith("http") else DOC_BASE + rel
    try:
        resp = _get_with_retry(session, url, timeout=90)
        if resp.status_code == 404:
            return ""
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type", "")
        if "pdf" not in ctype and not resp.content[:4] == b"%PDF":
            return ""
        return extract_pdf_text(resp.content, source_id)
    except requests.RequestException as e:
        logger.warning(f"  doc fetch failed {url}: {e}")
        return ""


def search_page(session: requests.Session, doc_type: str, from_hit: int) -> Dict[str, Any]:
    params = {
        "api_key": API_KEY,
        "type": doc_type,
        "from_hit": from_hit,
        "hits_returned": HITS_PER_PAGE,
        # blank q returns everything, sorted by relevance/date
        "q": "",
    }
    resp = _get_with_retry(session, API_BASE, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ---- Advisory Opinions (doctrine) ----

def normalize_ao(ao: Dict[str, Any], text: str) -> Dict[str, Any]:
    ao_no = ao.get("ao_no") or ao.get("no") or ""
    return {
        "_id": f"US-FEC-AO-{ao_no}",
        "_source": "US/FEC-Legal",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"FEC Advisory Opinion {ao_no}: {ao.get('name', '').strip()}"[:250],
        "text": clean_text(text),
        "date": parse_date(ao.get("issue_date")),
        "url": f"https://www.fec.gov/data/legal/advisory-opinions/{ao_no}/",
        "matter_type": "advisory_opinion",
        "ao_number": ao_no,
        "ao_year": ao.get("ao_year"),
        "status": ao.get("status"),
        "summary": (ao.get("summary") or "").strip(),
        "requestor_names": ao.get("requestor_names") or [],
        "statutory_citations": ao.get("statutory_citations") or [],
        "regulatory_citations": ao.get("regulatory_citations") or [],
    }


def ao_text(session: requests.Session, ao: Dict[str, Any], source_id: str) -> str:
    """Prefer the Final Opinion document; fall back to concatenating all docs."""
    docs = ao.get("documents") or []
    finals = [d for d in docs if (d.get("ao_doc_category_id") == "F"
                                  or "final opinion" in (d.get("category") or "").lower())]
    ordered = finals + [d for d in docs if d not in finals]
    parts: List[str] = []
    for d in ordered:
        t = fetch_doc_text(session, d, source_id)
        time.sleep(DELAY)
        if t:
            parts.append(t)
        if finals and parts:
            # A clean final opinion is enough on its own.
            break
    return "\n\n".join(parts)


# ---- Enforcement Matters / MURs (case_law) ----

def normalize_mur(mur: Dict[str, Any], text: str) -> Dict[str, Any]:
    no = mur.get("no") or ""
    respondents = mur.get("respondents") or []
    if isinstance(respondents, list):
        resp_str = "; ".join(r if isinstance(r, str) else (r.get("name", "") if isinstance(r, dict) else str(r))
                              for r in respondents)
    else:
        resp_str = str(respondents)
    return {
        "_id": f"US-FEC-MUR-{no}",
        "_source": "US/FEC-Legal",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"FEC MUR {no}: {mur.get('name', '').strip()}"[:250],
        "text": clean_text(text),
        "date": parse_date(mur.get("close_date") or mur.get("open_date")),
        "url": mur.get("url") if (mur.get("url") or "").startswith("http")
               else f"https://www.fec.gov/data/legal/matter-under-review/{no}/",
        "matter_type": "enforcement_mur",
        "mur_number": no,
        "mur_subtype": mur.get("mur_type"),
        "open_date": parse_date(mur.get("open_date")),
        "close_date": parse_date(mur.get("close_date")),
        "respondents": resp_str,
        "subjects": mur.get("subjects") or [],
        "election_cycles": mur.get("election_cycles") or [],
    }


def mur_text(session: requests.Session, mur: Dict[str, Any], source_id: str,
             max_docs: int = 25) -> str:
    """Concatenate text across the matter's documents (born-digital ones dominate)."""
    docs = mur.get("documents") or []
    parts: List[str] = []
    for d in docs[:max_docs]:
        t = fetch_doc_text(session, d, source_id)
        time.sleep(DELAY)
        if t and len(t) > 40:
            cat = (d.get("category") or "").strip()
            parts.append(f"### {cat}\n{t}" if cat else t)
    return "\n\n".join(parts)


# ---- Iterators ----

def iter_type(session: requests.Session, doc_type: str, limit: Optional[int]) -> Generator[Dict[str, Any], None, None]:
    from_hit = 0
    total = None
    yielded = 0
    key = "advisory_opinions" if doc_type == "advisory_opinions" else "murs"
    total_key = "total_advisory_opinions" if doc_type == "advisory_opinions" else "total_murs"
    while True:
        data = search_page(session, doc_type, from_hit)
        if total is None:
            total = data.get(total_key, 0)
            logger.info(f"{doc_type}: {total} total matters")
        rows = data.get(key) or []
        if not rows:
            break
        for row in rows:
            if doc_type == "advisory_opinions":
                text = ao_text(session, row, row.get("ao_no", ""))
                if not text or len(text) < 400:
                    logger.debug(f"  AO {row.get('ao_no')} thin ({len(text)} chars) — skip")
                    continue
                yield normalize_ao(row, text)
            else:
                text = mur_text(session, row, row.get("no", ""))
                if not text or len(text) < 400:
                    logger.debug(f"  MUR {row.get('no')} thin ({len(text)} chars) — skip")
                    continue
                yield normalize_mur(row, text)
            yielded += 1
            if limit and yielded >= limit:
                return
        from_hit += HITS_PER_PAGE
        if from_hit >= total:
            break
        time.sleep(DELAY)


def fetch_all(sample: bool = False) -> Generator[Dict[str, Any], None, None]:
    session = get_session()
    if sample:
        # Advisory opinions are consistently born-digital -> reliable full-text samples.
        count = 0
        for rec in iter_type(session, "advisory_opinions", limit=12):
            yield rec
            count += 1
        # add a couple of enforcement matters for type coverage
        for rec in iter_type(session, "murs", limit=3):
            yield rec
            count += 1
        logger.info(f"Sample complete: {count} records")
        return

    for rec in iter_type(session, "advisory_opinions", limit=None):
        yield rec
    for rec in iter_type(session, "murs", limit=None):
        yield rec


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    since_date = datetime.fromisoformat(since).date()
    for record in fetch_all():
        d = record.get("date")
        if d:
            try:
                if datetime.fromisoformat(d).date() < since_date:
                    continue
            except ValueError:
                pass
        yield record


def test_connectivity() -> bool:
    session = get_session()
    try:
        data = search_page(session, "advisory_opinions", 0)
        n = data.get("total_advisory_opinions", 0)
        logger.info(f"FEC legal API OK: {n} advisory opinions")
        aos = data.get("advisory_opinions") or []
        if aos:
            t = ao_text(session, aos[0], aos[0].get("ao_no", ""))
            logger.info(f"AO {aos[0].get('ao_no')} text: {len(t)} chars")
            return len(t) > 0
        return n > 0
    except Exception as e:
        logger.error(f"Failed: {e}")
        return False


def main():
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    data_dir = script_dir / "data"

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        sys.exit(0 if test_connectivity() else 1)

    if command in ("bootstrap", "bootstrap-fast"):
        sample_dir.mkdir(exist_ok=True)
        records_path = None
        jsonl = None
        if not sample:
            data_dir.mkdir(exist_ok=True)
            records_path = data_dir / "records.jsonl"
            jsonl = open(records_path, "w", encoding="utf-8")
        count = 0
        try:
            for record in fetch_all(sample=sample):
                if sample:
                    fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
                    with open(sample_dir / fname, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)
                else:
                    jsonl.write(json.dumps(record, ensure_ascii=False) + "\n")
                    jsonl.flush()
                count += 1
                print(f"  [{count}] {record['_id']} — {record['title'][:60]} ({len(record['text'])} chars)")
        finally:
            if jsonl:
                jsonl.close()
        dest = sample_dir if sample else records_path
        print(f"\nDone: {count} records -> {dest}")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2024-01-01"
        data_dir.mkdir(exist_ok=True)
        records_path = data_dir / "records.jsonl"
        count = 0
        with open(records_path, "w", encoding="utf-8") as f:
            for record in fetch_updates(since):
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
        print(f"Updated: {count} records since {since} -> {records_path}")


if __name__ == "__main__":
    main()
