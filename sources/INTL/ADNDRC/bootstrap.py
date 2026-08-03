#!/usr/bin/env python3
"""
INTL/ADNDRC -- Asian Domain Name Dispute Resolution Centre, UDRP panel decisions

The Asian Domain Name Dispute Resolution Centre (ADNDRC) is an ICANN-accredited
UDRP dispute-resolution provider operating through four offices: Beijing (CIETAC,
case prefix "CN"), Hong Kong ("HK"), Seoul ("KR") and Kuala Lumpur ("KL"). It
publishes its panel decisions openly as full-text PDFs.

Strategy:
  - The single index page https://www.adndrc.org/decisions/udrp server-renders a
    table of *every* decision: case number, complainant + domicile, respondent +
    domicile, disputed domain name(s), status/outcome, and a direct link to the
    decision PDF (https://www.adndrc.org/storage/files/udrp/{OFFICE}/{ID}_Decision.pdf).
  - Download each PDF (browser UA + referer are required; the server drops the
    connection otherwise) and extract the full text.
  - The decision date is parsed from the PDF body ("Date of Decision DD-MM-YYYY"),
    falling back to the listing's Decision Date cell or the year encoded in the
    case id.

Usage:
  python bootstrap.py test                 # connectivity test
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap            # full pull -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # alias for full pull (pipeline)
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
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen


def _encode_url(url: str) -> str:
    """Percent-encode the path/query so stray spaces in PDF hrefs don't crash
    urllib (some KL filenames are like 'KLRCA 386 2016 decision.pdf')."""
    parts = urlsplit(url)
    return urlunsplit((
        parts.scheme,
        parts.netloc,
        quote(parts.path, safe="/%"),
        quote(parts.query, safe="=&%"),
        parts.fragment,
    ))

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

BASE_URL = "https://www.adndrc.org"
INDEX_URL = f"{BASE_URL}/decisions/udrp"
RATE_LIMIT = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

HERE = Path(__file__).parent


def _fetch_text(url: str, timeout: int = 90) -> Optional[str]:
    req = Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")
    req.add_header("Accept-Language", "en-US,en;q=0.8")
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        print(f"  HTTP {e.code} for {url}", file=sys.stderr)
        return None
    except (URLError, OSError) as e:
        print(f"  Network error for {url}: {e}", file=sys.stderr)
        return None


def _fetch_bytes(url: str, timeout: int = 90) -> Optional[bytes]:
    """Download a binary file (the decision PDF) with a browser UA + referer."""
    req = Request(_encode_url(url))
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/pdf,*/*")
    req.add_header("Referer", INDEX_URL)
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except HTTPError as e:
        print(f"  HTTP {e.code} for {url}", file=sys.stderr)
        return None
    except (URLError, OSError) as e:
        print(f"  Network error for {url}: {e}", file=sys.stderr)
        return None


def _clean(s: str) -> str:
    return unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or ""))).strip()


def parse_index(html: str) -> list:
    """Parse the decisions table into a list of row dicts."""
    rows = []
    for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        block = rm.group(1)
        # Decisions are linked as PDFs in two historical layouts:
        #   /storage/files/udrp/{OFFICE}/{ID}_Decision.pdf   (older offices)
        #   /storage/uploads/decisions/udrp/udrp_{TS}.pdf    (newer uploads)
        if "/storage/" not in block or ".pdf" not in block:
            continue
        cells = re.findall(r"<td[^>]*>(.*?)</td>", block, re.S)
        if len(cells) < 8:
            continue
        pdf_m = re.search(
            r'href="(https://www\.adndrc\.org/storage/[^"]+?\.pdf)"',
            block,
        )
        if not pdf_m:
            continue
        vals = [_clean(c) for c in cells]

        def g(i):
            return vals[i] if i < len(vals) else ""

        rows.append({
            "case_number": g(0),
            "complainant": g(1),
            "complainant_domicile": g(2),
            "respondent": g(3),
            "respondent_domicile": g(4),
            "domain_names": g(5),
            "result": g(6),
            "listing_date": g(8),
            "pdf_url": pdf_m.group(1),
        })
    return rows


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def parse_decision_date(text: str, fallback_case: str) -> Optional[str]:
    """Extract ISO decision date from PDF text or fall back to the case-id year."""
    # "Date of Decision 01-07-2002" (DD-MM-YYYY, possibly with stray spaces)
    m = re.search(r"Date of Decision\s*([0-3]?\d)\s*-\s*([01]?\d)\s*-\s*(\d{4})", text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    # "Date: 5 July 2016" / "Dated: 5 July 2016" (KL/HK textual style); take last.
    tm = None
    for tm in re.finditer(
        r"Date[d]?\s*:?\s*([0-3]?\d)\s+([A-Za-z]+)\s+(\d{4})", text):
        pass
    if tm and tm.group(2).lower() in _MONTHS:
        return f"{tm.group(3)}-{_MONTHS[tm.group(2).lower()]:02d}-{int(tm.group(1)):02d}"
    # generic DD-MM-YYYY near the top
    m = re.search(r"\b([0-3]?\d)-([01]?\d)-(\d{4})\b", text[:1500])
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    # case id like CN-0200001 / HK-2100001 -> embedded 2-digit year
    cm = re.search(r"-(\d{2})\d{5}\b", fallback_case)
    if cm:
        yy = int(cm.group(1))
        year = 2000 + yy if yy < 80 else 1900 + yy
        return f"{year}-01-01"
    # KL/AIAC case id like KLRCA/ADNDRC-393-2016 or AIAC/ADNDRC-1054-2022 ->
    # trailing 4-digit year (the sequence number can also be 4 digits, so pick
    # the *last* group that falls in a valid year range).
    years = [int(y) for y in re.findall(r"-(\d{4})\b", fallback_case)
             if 1995 <= int(y) <= 2035]
    if years:
        return f"{years[-1]}-01-01"
    return None


def normalize(row: dict, text: str) -> dict:
    case = row["case_number"]
    date = parse_decision_date(text, case)
    if not date and row.get("listing_date"):
        lm = re.search(r"(\d{4})-(\d{2})-(\d{2})", row["listing_date"])
        if lm:
            date = lm.group(0)
    comp = row.get("complainant", "")
    resp = row.get("respondent", "")
    domains = row.get("domain_names", "")
    if comp and resp:
        title = f"{comp} v. {resp} ({domains})"
    else:
        title = f"ADNDRC UDRP Decision {case}"
    return {
        "_id": case,
        "_source": "INTL/ADNDRC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": row["pdf_url"],
        "case_number": case,
        "domain_names": domains,
        "complainant": comp,
        "complainant_domicile": row.get("complainant_domicile", ""),
        "respondent": resp,
        "respondent_domicile": row.get("respondent_domicile", ""),
        "decision_result": row.get("result", ""),
    }


def generate(sample: bool = False) -> Generator[dict, None, None]:
    html = _fetch_text(INDEX_URL)
    if not html:
        print("Failed to fetch index page", file=sys.stderr)
        return
    rows = parse_index(html)
    print(f"Index lists {len(rows)} decisions", file=sys.stderr)
    target = 15 if sample else None
    count = 0
    for row in rows:
        pdf = _fetch_bytes(row["pdf_url"])
        time.sleep(RATE_LIMIT)
        if not pdf:
            continue
        try:
            text = extract_pdf_markdown("INTL/ADNDRC", row["case_number"], pdf_bytes=pdf)
        except Exception as e:  # noqa: BLE001
            print(f"  extract error {row['case_number']}: {e}", file=sys.stderr)
            continue
        if not text or len(text) < 300:
            print(f"  skip {row['case_number']} (no/short text)", file=sys.stderr)
            continue
        rec = normalize(row, text)
        print(f"  got {rec['_id']}: {len(rec['text'])} chars", file=sys.stderr)
        yield rec
        count += 1
        if target and count >= target:
            break
    print(f"Done: {count} records", file=sys.stderr)


def fetch_all() -> Generator[dict, None, None]:
    yield from generate(sample=False)


def fetch_updates(since=None) -> Generator[dict, None, None]:
    # No incremental endpoint; the index is small enough to re-scan in full.
    yield from generate(sample=False)


def test_connectivity() -> bool:
    print("Testing ADNDRC connectivity...", file=sys.stderr)
    html = _fetch_text(INDEX_URL)
    if not html:
        print("  index fetch FAILED", file=sys.stderr)
        return False
    rows = parse_index(html)
    print(f"  index rows: {len(rows)}", file=sys.stderr)
    if not rows:
        return False
    # Some older/KL decisions are scanned image PDFs; try several rows.
    for row in rows[:8]:
        pdf = _fetch_bytes(row["pdf_url"])
        time.sleep(RATE_LIMIT)
        if not pdf:
            continue
        text = extract_pdf_markdown("INTL/ADNDRC", row["case_number"], pdf_bytes=pdf)
        if text and len(text) > 500:
            print(f"  decision text OK: {len(text)} chars (case {row['case_number']})",
                  file=sys.stderr)
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
                if count < 15:
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
