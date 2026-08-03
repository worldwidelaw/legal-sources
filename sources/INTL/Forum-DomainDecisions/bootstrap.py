#!/usr/bin/env python3
"""
INTL/Forum-DomainDecisions -- Forum (formerly National Arbitration Forum / NAF)
domain-name dispute decisions.

Forum (forumadr.com, legacy adrforum.com) is an ICANN-accredited dispute
resolution provider and the second-largest UDRP provider after WIPO. It also
administers URS, the U.S. usTLD (usDRP/usRS), the Canadian CDRP, and numerous
other registry dispute policies. Its public "Search Decisions" portal exposes
the complete decision corpus -- 43,000+ UDRP decisions alone -- each linking to
a full-text HTML decision document.

Strategy:
  - GET  webapi.adrforum.com/api/SearchDecisions/GetRulesets
        -> list of dispute rulesets (UDRP, URS, USDRP, CDRP, ...).
  - POST webapi.adrforum.com/api/SearchDecisions/DoStandardSearch
        with {"ruleset": "<RS>", ...} -> the full case list for that ruleset.
        Each row carries caseId, caseNumber, domains, caseName, ruleset,
        status, decisionDate and a `url` to the full-text decision document
        (https://www.adrforum.com/DomainDecisions/{n}.htm).
  - GET each decision `url` -> strip HTML -> full decision text.
  - Rate-limit ~1 req / 1.2s.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap            # full pull -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # alias for full pull (pipeline)
  python bootstrap.py test                 # connectivity test
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_BASE = "https://webapi.adrforum.com/api"
RULESETS_URL = f"{API_BASE}/SearchDecisions/GetRulesets"
SEARCH_URL = f"{API_BASE}/SearchDecisions/DoStandardSearch"
RATE_LIMIT = 1.2
USER_AGENT = "LegalDataHunter/1.0 (+https://github.com/ZachLaik/LegalDataHunter)"
ORIGIN = "https://www.adrforum.com"

HERE = Path(__file__).parent


def _request(url: str, data: Optional[bytes] = None, accept: str = "application/json",
             timeout: int = 120) -> Optional[bytes]:
    """HTTP GET/POST. Returns raw body bytes, or None on error."""
    req = Request(url, data=data, method="POST" if data is not None else "GET")
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", accept)
    req.add_header("Origin", ORIGIN)
    req.add_header("Referer", ORIGIN + "/")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except HTTPError as e:
        if e.code == 404:
            return None
        print(f"  HTTP {e.code} for {url}", file=sys.stderr)
        return None
    except (URLError, OSError) as e:
        print(f"  Network error for {url}: {e}", file=sys.stderr)
        return None


def get_rulesets() -> list:
    """Return the list of dispute ruleset codes."""
    body = _request(RULESETS_URL)
    if not body:
        return []
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return []


def _search_payload(ruleset: str) -> bytes:
    """Minimal-but-complete DoStandardSearch payload for one ruleset."""
    payload = {
        "fullTextSearch": "", "ruleset": ruleset, "panelist": "", "domain": "",
        "caseNumber": "", "caseName": "", "complainant": "", "respondent": "",
        "complainantDomicile": "", "respondentDomicile": "",
        "commencementDateStart": None, "commencementDateEnd": None,
        "status": "Any", "submissionDateStart": None, "submissionDateEnd": None,
        "decisionDateStart": None, "decisionDateEnd": None,
        "responseType": 0, "ursFindingOfAbuse": False, "atLeastOneGtld": False,
    }
    return json.dumps(payload).encode("utf-8")


def search_ruleset(ruleset: str) -> list:
    """Return the full case list for a ruleset (list of row dicts)."""
    body = _request(SEARCH_URL, data=_search_payload(ruleset))
    if not body:
        return []
    try:
        rows = json.loads(body)
    except json.JSONDecodeError:
        return []
    return rows if isinstance(rows, list) else []


def _clean_html(html: str) -> str:
    """Strip an HTML decision document down to clean plain text."""
    html = re.sub(r"(?is)<(script|style|head)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</(p|div|tr|h[1-6]|li)>", "\n", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = unescape(text)
    text = text.replace("﻿", "").replace("\r", "")
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_decision_text(url: str) -> Optional[str]:
    """Fetch a decision document URL and return clean full text."""
    if not url:
        return None
    body = _request(url, accept="text/html,*/*;q=0.8", timeout=90)
    if not body:
        return None
    text = _clean_html(body.decode("utf-8", errors="replace"))
    return text or None


def _iso_date(raw: Optional[str]) -> Optional[str]:
    """'2000-03-09T00:00:00' -> '2000-03-09'."""
    if not raw or not isinstance(raw, str):
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    return m.group(1) if m else None


def normalize(row: dict, text: str) -> dict:
    """Build a normalized record from a search row + full text."""
    case_number = str(row.get("caseNumber") or row.get("caseId") or "").strip()
    case_name = (row.get("caseName") or "").strip()
    domains = (row.get("domains") or "").strip()
    ruleset = (row.get("ruleset") or "").strip()
    date = _iso_date(row.get("decisionDate"))

    title = case_name or f"Forum {ruleset} Decision {case_number}"
    if domains and case_name:
        title = f"{case_name} ({domains})"

    return {
        "_id": f"FORUM-{case_number}" if case_number else f"FORUM-{row.get('caseId')}",
        "_source": "INTL/Forum-DomainDecisions",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": row.get("url", ""),
        "case_number": case_number,
        "ruleset": ruleset,
        "domain_names": domains,
        "decision_result": (row.get("status") or "").strip(),
        "submission_date": _iso_date(row.get("submissionDate")),
    }


def generate(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records with full text across all rulesets."""
    target = 15 if sample else None
    rulesets = get_rulesets()
    if not rulesets:
        print("  no rulesets returned", file=sys.stderr)
        return
    # UDRP first (largest, all-HTML) so sample mode fills quickly.
    rulesets = sorted(rulesets, key=lambda r: 0 if r == "UDRP" else 1)
    count = 0
    seen = set()
    for rs in rulesets:
        rows = search_ruleset(rs)
        print(f"  ruleset {rs}: {len(rows)} cases", file=sys.stderr)
        time.sleep(RATE_LIMIT)
        for row in rows:
            cid = row.get("caseId") or row.get("caseNumber")
            if cid in seen:
                continue
            seen.add(cid)
            url = row.get("url", "")
            if not url.lower().endswith(".htm") and not url.lower().endswith(".html"):
                # Non-HTML (rare PDF/external) -- skip, no clean full text.
                continue
            text = fetch_decision_text(url)
            time.sleep(RATE_LIMIT)
            if not text or len(text) < 400:
                print(f"  skip {row.get('caseNumber')} (no/short text)", file=sys.stderr)
                continue
            rec = normalize(row, text)
            print(f"  got {rec['_id']} [{rs}]: {len(text)} chars", file=sys.stderr)
            yield rec
            count += 1
            if target and count >= target:
                print(f"Done: {count} records", file=sys.stderr)
                return
    print(f"Done: {count} records", file=sys.stderr)


def test_connectivity() -> bool:
    print("Testing Forum adrforum.com connectivity...", file=sys.stderr)
    rulesets = get_rulesets()
    print(f"  rulesets: {rulesets[:6]}{'...' if len(rulesets) > 6 else ''}", file=sys.stderr)
    if not rulesets:
        print("  GetRulesets FAILED", file=sys.stderr)
        return False
    rows = search_ruleset("UDRP")
    print(f"  UDRP cases: {len(rows)}", file=sys.stderr)
    if not rows:
        print("  search FAILED", file=sys.stderr)
        return False
    sample_row = next((r for r in rows if str(r.get("url", "")).lower().endswith(".htm")), None)
    if not sample_row:
        print("  no .htm decision url found", file=sys.stderr)
        return False
    text = fetch_decision_text(sample_row["url"])
    if text and len(text) > 500:
        print(f"  decision text OK: {len(text)} chars "
              f"(case {sample_row.get('caseNumber')})", file=sys.stderr)
        print("PASS", file=sys.stderr)
        return True
    print("  decision text FAILED", file=sys.stderr)
    return False


def run(sample: bool):
    sample_dir = HERE / "sample"
    sample_dir.mkdir(exist_ok=True)
    data_dir = HERE / "data"
    data_dir.mkdir(exist_ok=True)
    jsonl = data_dir / "records.jsonl"

    count = 0
    jf = None if sample else jsonl.open("w", encoding="utf-8")
    try:
        for rec in generate(sample=sample):
            if sample:
                safe = re.sub(r"[^A-Za-z0-9_.-]", "_", rec["_id"])
                (sample_dir / f"{safe}.json").write_text(
                    json.dumps(rec, ensure_ascii=False, indent=2))
            else:
                jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                jf.flush()
                if count < 15:  # keep a few samples for validation
                    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", rec["_id"])
                    (sample_dir / f"{safe}.json").write_text(
                        json.dumps(rec, ensure_ascii=False, indent=2))
            count += 1
    finally:
        if jf:
            jf.close()
    dest = "sample/" if sample else str(jsonl)
    print(f"Saved {count} records to {dest}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample]")
        sys.exit(1)
    cmd = sys.argv[1]
    sample = "--sample" in sys.argv
    if cmd == "test":
        sys.exit(0 if test_connectivity() else 1)
    elif cmd in ("bootstrap", "bootstrap-fast"):
        run(sample=sample)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
