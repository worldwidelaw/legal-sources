#!/usr/bin/env python3
"""
INTL/ADNDRC-UDRP -- Asian Domain Name Dispute Resolution Centre, UDRP decisions

The ADNDRC (Asian Domain Name Dispute Resolution Centre) is an ICANN-accredited
UDRP provider operating four offices: Beijing (CN), Hong Kong (HK), Kuala Lumpur
(KL) and Seoul (KR). It publishes its panel decisions openly as full-text PDFs
at https://www.adndrc.org/decisions/udrp.

Strategy:
  - Fetch the single server-rendered decisions page (one big HTML table per
    office tab). Each <tr> has 9 cells:
      Case Number | Complainant | Domicile of Complainant | Respondent |
      Domicile of Respondent | Disputed Domain Name(s) | Status |
      Decision (PDF link) | Decision Date
  - For every row carrying an adndrc.org decision PDF (~2,750 decisions), download
    the PDF and extract its full text via common.pdf_extract.
  - The decision date comes from the table cell, falling back to the
    "Date of Decision DD-MM-YYYY" line inside the PDF text.

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
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

HERE = Path(__file__).parent
# Allow importing the shared common/ package when run as a standalone script.
sys.path.insert(0, str(HERE.parents[2]))
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

BASE_URL = "https://www.adndrc.org"
LIST_URL = f"{BASE_URL}/decisions/udrp"
RATE_LIMIT = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

PDF_HREF_RE = re.compile(
    r'href="(https://www\.adndrc\.org/storage/[^"]+\.pdf)"'
)
OFFICE_BY_PREFIX = {
    "CN": "Beijing",
    "HK": "Hong Kong",
    "KL": "Kuala Lumpur",
    "KLRCA": "Kuala Lumpur",
    "AIAC": "Kuala Lumpur",
    "KR": "Seoul",
}


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


def _encode_url(url: str) -> str:
    """Percent-encode the path/query so spaces & brackets are URL-safe."""
    parts = urlsplit(url)
    path = quote(parts.path, safe="/%")
    query = quote(parts.query, safe="=&%")
    return urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


def _fetch_bytes(url: str, timeout: int = 90) -> Optional[bytes]:
    url = _encode_url(url)
    req = Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/pdf,*/*")
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


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def parse_date(raw: str) -> Optional[str]:
    """Normalize table/PDF dates to ISO 8601. Accepts YYYY-MM-DD or DD-MM-YYYY."""
    if not raw:
        return None
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def _date_from_text(text: str) -> Optional[str]:
    """Pull a decision date out of the PDF body across the office formats."""
    # CN / HK / KR: "Date of Decision 01-07-2002"
    m = re.search(r"Date of Decision\s*([\d.\- ]{8,12})", text)
    if m:
        d = parse_date(m.group(1).replace(" ", ""))
        if d:
            return d
    # KL / AIAC: "Dated: 16 August 2023" (take the last such signing date)
    matches = re.findall(
        r"(?:Dated|Date)\s*:?\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
    for day, mon, year in reversed(matches):
        mo = _MONTHS.get(mon.lower())
        if mo:
            return f"{int(year):04d}-{mo:02d}-{int(day):02d}"
    return None


def parse_rows(html: str) -> list:
    """Parse every decision row (9 cells + a PDF link) into a dict."""
    rows = []
    for rm in re.finditer(r"<tr>(.*?)</tr>", html, re.S):
        block = rm.group(1)
        href = PDF_HREF_RE.search(block)
        if not href:
            continue
        cells = re.findall(r"<td[^>]*>(.*?)</td>", block, re.S)
        if len(cells) < 9:
            continue
        vals = [_clean(c) for c in cells]
        case_number = vals[0]
        if not case_number:
            continue
        rows.append({
            "case_number": case_number,
            "complainant": vals[1],
            "complainant_domicile": vals[2],
            "respondent": vals[3],
            "respondent_domicile": vals[4],
            "domain_names": vals[5],
            "status": vals[6],
            "date": parse_date(vals[8]),
            "pdf_url": href.group(1),
        })
    return rows


def _office(case_number: str) -> str:
    prefix = re.split(r"[-/ ]", case_number, 1)[0].upper()
    return OFFICE_BY_PREFIX.get(prefix, "ADNDRC")


def normalize(row: dict, text: str) -> dict:
    case_number = row["case_number"]
    date = row.get("date")
    if not date:
        date = _date_from_text(text)
    complainant = row.get("complainant", "")
    respondent = row.get("respondent", "")
    domains = row.get("domain_names", "")
    if complainant and respondent:
        title = f"{complainant} v. {respondent} ({domains})"
    else:
        title = f"ADNDRC UDRP Decision {case_number}"
    return {
        "_id": case_number,
        "_source": "INTL/ADNDRC-UDRP",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": row["pdf_url"],
        "case_number": case_number,
        "office": _office(case_number),
        "process": "UDRP",
        "domain_names": domains,
        "complainant": complainant,
        "complainant_domicile": row.get("complainant_domicile", ""),
        "respondent": respondent,
        "respondent_domicile": row.get("respondent_domicile", ""),
        "decision_result": row.get("status", ""),
    }


def generate(sample: bool = False) -> Generator[dict, None, None]:
    html = _fetch_text(LIST_URL)
    if not html:
        print("Failed to fetch decisions listing", file=sys.stderr)
        return
    rows = parse_rows(html)
    print(f"Found {len(rows)} decision rows", file=sys.stderr)
    if sample:
        # KL (Kuala Lumpur) decisions are scanned image PDFs with no text
        # layer; prefer the text-bearing offices for the validation sample.
        rows.sort(key=lambda r: _office(r["case_number"]) == "Kuala Lumpur")
    target = 15 if sample else None
    count = 0
    for row in rows:
        pdf_bytes = _fetch_bytes(row["pdf_url"])
        time.sleep(RATE_LIMIT)
        if not pdf_bytes:
            continue
        try:
            text = extract_pdf_markdown(
                "INTL/ADNDRC-UDRP", row["case_number"],
                pdf_bytes=pdf_bytes, force=True,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  extract error {row['case_number']}: {e}", file=sys.stderr)
            text = None
        if not text or len(text) < 300:
            print(f"  skip {row['case_number']} (no/short text)", file=sys.stderr)
            continue
        rec = normalize(row, text)
        print(f"  got {rec['_id']}: {len(text)} chars", file=sys.stderr)
        yield rec
        count += 1
        if target and count >= target:
            break
    print(f"Done: {count} records", file=sys.stderr)


def test_connectivity() -> bool:
    print("Testing ADNDRC connectivity...", file=sys.stderr)
    html = _fetch_text(LIST_URL)
    if not html:
        print("  list fetch FAILED", file=sys.stderr)
        return False
    rows = parse_rows(html)
    print(f"  decision rows: {len(rows)}", file=sys.stderr)
    if not rows:
        return False
    pdf = _fetch_bytes(rows[0]["pdf_url"])
    if not pdf:
        print("  PDF fetch FAILED", file=sys.stderr)
        return False
    text = extract_pdf_markdown(
        "INTL/ADNDRC-UDRP", rows[0]["case_number"],
        pdf_bytes=pdf, force=True,
    )
    if text and len(text) > 500:
        print(f"  PDF text OK: {len(text)} chars (case {rows[0]['case_number']})",
              file=sys.stderr)
        print("PASS", file=sys.stderr)
        return True
    print("  PDF text FAILED", file=sys.stderr)
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
