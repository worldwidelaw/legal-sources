#!/usr/bin/env python3
"""
INTL/CAC-UDRP -- Czech Arbitration Court, domain-name dispute decisions (UDRP + .eu ADR)

The Arbitration Court attached to the Czech Chamber of Commerce ("CAC", also
operating as adr.eu) is an ICANN-accredited UDRP provider and the sole ADR
provider designated by the European Commission for ".eu" domain disputes.
Its full decisions portal at https://udrp.adr.eu publishes ~6,000 panel
decisions with complete reasoning (full text, HTML).

Strategy:
  - Enumerate the server-rendered decision grid:
      /decisions/list?grid-perPage=100&grid-page=N
    Each <tr data-id="..."> carries the decision id + row metadata
    (case number, process, domains, parties, panelist, published, result).
  - Fetch each decision detail page:
      /decisions/detail?id={id}
    The <section class="content"> block holds the full decision text and the
    "Date of Panel Decision".
  - Rate-limit to ~1.5s between requests.

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

from bs4 import BeautifulSoup

BASE_URL = "https://udrp.adr.eu"
LIST_URL = f"{BASE_URL}/decisions/list"
DETAIL_URL = f"{BASE_URL}/decisions/detail"
PER_PAGE = 100
RATE_LIMIT = 1.5
USER_AGENT = "LegalDataHunter/1.0 (+https://github.com/ZachLaik/LegalDataHunter)"

HERE = Path(__file__).parent


def _fetch_text(url: str, timeout: int = 60) -> Optional[str]:
    """Fetch a URL and return decoded HTML, or None on error."""
    req = Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")
    req.add_header("Accept-Language", "en-US,en;q=0.8")
    try:
        resp = urlopen(req, timeout=timeout)
        body = resp.read()
        ct = resp.headers.get("Content-Type", "")
        enc = "utf-8"
        m = re.search(r"charset=([^\s;]+)", ct)
        if m:
            enc = m.group(1)
        try:
            return body.decode(enc, errors="replace")
        except (LookupError, UnicodeDecodeError):
            return body.decode("utf-8", errors="replace")
    except HTTPError as e:
        if e.code == 404:
            return None
        print(f"  HTTP {e.code} for {url}", file=sys.stderr)
        return None
    except (URLError, OSError) as e:
        print(f"  Network error for {url}: {e}", file=sys.stderr)
        return None


def _clean(s: str) -> str:
    return unescape(re.sub(r"\s+", " ", s or "")).strip()


def parse_published(raw: str) -> Optional[str]:
    """'25.06.2026 10:04' -> '2026-06-25'. Returns ISO date or None."""
    if not raw:
        return None
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def parse_list_page(html: str) -> list:
    """Parse one grid page into a list of row dicts (id + metadata)."""
    rows = []
    for rm in re.finditer(r'<tr[^>]*data-id="([a-f0-9]+)"[^>]*>(.*?)</tr>', html, re.S):
        rid = rm.group(1)
        cells = re.findall(r"<td[^>]*>(.*?)</td>", rm.group(2), re.S)
        vals = [_clean(re.sub(r"<[^>]+>", " ", c)) for c in cells]
        # cols: case_number, process, domains, complainants, respondents,
        #       panelists, published, result, action
        def g(i):
            return vals[i] if i < len(vals) else ""
        rows.append({
            "id": rid,
            "case_number": g(0),
            "process": g(1),
            "domains": g(2),
            "complainants": g(3),
            "respondents": g(4),
            "panelists": g(5),
            "published": g(6),
            "result": g(7),
        })
    return rows


def iter_list_rows(max_rows: Optional[int] = None) -> Generator[dict, None, None]:
    """Yield row dicts across all grid pages."""
    page = 1
    seen = set()
    emitted = 0
    while True:
        url = f"{LIST_URL}?grid-perPage={PER_PAGE}&grid-page={page}"
        html = _fetch_text(url)
        if not html:
            break
        rows = parse_list_page(html)
        if not rows:
            break
        new_on_page = 0
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            new_on_page += 1
            yield r
            emitted += 1
            if max_rows and emitted >= max_rows:
                return
        if new_on_page == 0:
            break  # no new ids -> reached the end / loop guard
        print(f"  list page {page}: +{new_on_page} (total {emitted})", file=sys.stderr)
        page += 1
        time.sleep(RATE_LIMIT)


def fetch_detail(rid: str) -> Optional[dict]:
    """Fetch a decision detail page. Returns {text, decision_date} or None."""
    url = f"{DETAIL_URL}?id={rid}"
    html = _fetch_text(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    sec = soup.find("section", class_="content")
    if not sec:
        return None
    for tag in sec(["script", "style", "nav", "form", "button"]):
        tag.decompose()
    text = sec.get_text("\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text).strip()
    text = re.sub(r"^Back\s*\n+", "", text).strip()  # drop leading nav token
    # Decision date: "Date of Panel Decision\n\n2026-06-24"
    dd = None
    m = re.search(r"Date of Panel Decision\s*\n+\s*(\d{4}-\d{2}-\d{2})", text)
    if m:
        dd = m.group(1)
    return {"text": text, "decision_date": dd}


def normalize(row: dict, detail: dict) -> dict:
    """Build a normalized record from a list row + detail page."""
    case_number = row.get("case_number", "")
    complainant = row.get("complainants", "")
    respondent = row.get("respondents", "")
    domains = row.get("domains", "")
    date = detail.get("decision_date") or parse_published(row.get("published", ""))
    process = (row.get("process") or "").lower()
    proc_label = ".eu ADR" if process in ("adreu", "adr.eu", "eu") else "UDRP"

    if complainant and respondent:
        title = f"{complainant} v. {respondent} ({domains})"
    else:
        title = f"CAC {proc_label} Decision {case_number}"

    return {
        "_id": case_number or row["id"],
        "_source": "INTL/CAC-UDRP",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": detail.get("text", ""),
        "date": date,
        "url": f"{DETAIL_URL}?id={row['id']}",
        "case_number": case_number,
        "process": proc_label,
        "domain_names": domains,
        "complainant": complainant,
        "respondent": respondent,
        "panelist": row.get("panelists", ""),
        "decision_result": row.get("result", ""),
    }


def generate(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records with full text."""
    count = 0
    target = 15 if sample else None
    # In sample mode we scan more rows than needed because some recent rows
    # may be filings without a published decision yet.
    row_cap = 60 if sample else None
    for row in iter_list_rows(max_rows=row_cap):
        detail = fetch_detail(row["id"])
        time.sleep(RATE_LIMIT)
        if not detail or len(detail.get("text", "")) < 300:
            print(f"  skip {row.get('case_number')} (no/short text)", file=sys.stderr)
            continue
        rec = normalize(row, detail)
        print(f"  got {rec['_id']}: {len(rec['text'])} chars", file=sys.stderr)
        yield rec
        count += 1
        if target and count >= target:
            break
    print(f"Done: {count} records", file=sys.stderr)


def test_connectivity() -> bool:
    print("Testing CAC udrp.adr.eu connectivity...", file=sys.stderr)
    html = _fetch_text(f"{LIST_URL}?grid-perPage={PER_PAGE}&grid-page=1")
    if not html:
        print("  list fetch FAILED", file=sys.stderr)
        return False
    rows = parse_list_page(html)
    print(f"  list page rows: {len(rows)}", file=sys.stderr)
    if not rows:
        return False
    detail = fetch_detail(rows[0]["id"])
    if detail and len(detail.get("text", "")) > 500:
        print(f"  detail text OK: {len(detail['text'])} chars "
              f"(case {rows[0]['case_number']})", file=sys.stderr)
        print("PASS", file=sys.stderr)
        return True
    print("  detail text FAILED", file=sys.stderr)
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
