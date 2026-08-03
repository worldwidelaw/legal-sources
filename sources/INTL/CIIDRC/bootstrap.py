#!/usr/bin/env python3
"""
INTL/CIIDRC -- Canadian International Internet Dispute Resolution Centre

The Canadian International Internet Dispute Resolution Centre (CIIDRC), operated
by the British Columbia International Commercial Arbitration Centre (BCICAC /
Vancouver International Arbitration Centre), is an ICANN-accredited UDRP provider
and the CIRA-approved provider for .CA Canadian Domain Name Dispute Resolution
Policy (CDRP) disputes. It publishes its panel decisions openly as full-text PDFs.

Strategy:
  - The single index page https://ciidrc.org/domain-name-disputes/ciidrc-decisions/
    server-renders a DataTable of *every* decision (~660): listing date, case
    number (a link to /my-portal/decisions/?casenumber={INTERNAL_ID}&action=view),
    disputed domain(s) and status.
  - Each case view page exposes structured metadata (case number like
    "27084-CDRP", decision status, complainant, respondent, decision date,
    panelists) and a documents table linking the full-text decision PDF under
    https://ciidrc.org/wp-content/uploads/YYYY/MM/...-Decision.pdf
  - Download each PDF and extract the full text.

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

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.pdf_extract import extract_pdf_markdown  # noqa: E402

HERE = Path(__file__).parent
BASE_URL = "https://ciidrc.org"
INDEX_URL = f"{BASE_URL}/domain-name-disputes/ciidrc-decisions/"
VIEW_URL = f"{BASE_URL}/my-portal/decisions/?casenumber={{}}&action=view"
RATE_LIMIT = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def _encode_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((
        parts.scheme, parts.netloc,
        quote(parts.path, safe="/%"),
        quote(parts.query, safe="=&%"),
        parts.fragment,
    ))


def _fetch_text(url: str, timeout: int = 90) -> Optional[str]:
    req = Request(_encode_url(url))
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
    """Parse the decisions DataTable into row dicts (one per case)."""
    rows = []
    seen = set()
    for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        block = rm.group(1)
        cm = re.search(r"casenumber=(\d+)&action=view", block)
        if not cm:
            continue
        internal_id = cm.group(1)
        if internal_id in seen:
            continue
        seen.add(internal_id)
        date_m = re.search(r'class="case-created"[^>]*>(.*?)</td>', block, re.S)
        dom_m = re.search(r'class="case-domains"[^>]*>(.*?)</td>', block, re.S)
        # Display case number, e.g. "27055-CDRP", from anchor text.
        disp_m = re.search(r"casenumber=\d+&action=view[^>]*>(.*?)</a>", block, re.S)
        rows.append({
            "internal_id": internal_id,
            "listing_date": _clean(date_m.group(1)) if date_m else "",
            "domain_names": _clean(dom_m.group(1)) if dom_m else "",
            "display_number": _clean(disp_m.group(1)) if disp_m else "",
        })
    return rows


def _dl_value(html: str, label_class: str) -> str:
    """Pull a <dd class="{label_class} definition_value">...</dd> value."""
    m = re.search(
        r'class="' + re.escape(label_class) + r' definition_value"[^>]*>(.*?)</dd>',
        html, re.S)
    return _clean(m.group(1)) if m else ""


def parse_view(html: str) -> dict:
    """Extract case metadata and the decision PDF url from a case view page."""
    pdf_m = re.search(
        r'href="(https://ciidrc\.org/wp-content/uploads/[^"]+?\.pdf)"', html)
    # The decision-rendered date and the panelist names share the same
    # "rendered definition_value" class; drop anything that parses as a date.
    panelists = [
        p for p in (_clean(x) for x in re.findall(
            r'class="rendered definition_value"[^>]*>(.*?)</dd>', html, re.S))
        if p and not _parse_rendered_date(p)
    ]
    return {
        "case_number": _dl_value(html, "casenumber"),
        "status": _dl_value(html, "status"),
        "domains": _dl_value(html, "domains"),
        "complainant": _dl_value(html, "complainant"),
        "respondent": _dl_value(html, "respondent"),
        "date_rendered": _dl_value(html, "rendered"),
        "panelists": panelists[:6],
        "pdf_url": pdf_m.group(1) if pdf_m else None,
    }


def _parse_rendered_date(text: str) -> Optional[str]:
    # "June 16, 2026" possibly with trailing time/zone.
    m = re.search(r"([A-Za-z]+)\s+([0-3]?\d),\s*(\d{4})", text or "")
    if m and m.group(1).lower() in _MONTHS:
        return f"{m.group(3)}-{_MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"
    return None


def normalize(row: dict, view: dict, text: str) -> dict:
    case = view.get("case_number") or row.get("display_number") or row["internal_id"]
    # Decision date: rendered date, then PDF filename date prefix, then listing.
    date = _parse_rendered_date(view.get("date_rendered", ""))
    if not date and view.get("pdf_url"):
        fm = re.search(r"/(\d{4})-(\d{2})-(\d{2})-", view["pdf_url"])
        if fm:
            date = f"{fm.group(1)}-{fm.group(2)}-{fm.group(3)}"
    if not date and row.get("listing_date"):
        lm = re.search(r"(\d{4})-(\d{2})-(\d{2})", row["listing_date"])
        if lm:
            date = lm.group(0)
    comp = view.get("complainant", "")
    resp = view.get("respondent", "")
    domains = view.get("domains") or row.get("domain_names", "")
    if comp and resp:
        title = f"{comp} v. {resp} ({domains}) [{case}]"
    else:
        title = f"CIIDRC Domain Decision {case} ({domains})"
    return {
        "_id": case,
        "_source": "INTL/CIIDRC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": VIEW_URL.format(row["internal_id"]),
        "pdf_url": view.get("pdf_url"),
        "case_number": case,
        "domain_names": domains,
        "complainant": comp,
        "respondent": resp,
        "decision_result": view.get("status", ""),
        "panelists": view.get("panelists", []),
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
        view_html = _fetch_text(VIEW_URL.format(row["internal_id"]))
        time.sleep(RATE_LIMIT)
        if not view_html:
            continue
        view = parse_view(view_html)
        if not view.get("pdf_url"):
            print(f"  skip {row['internal_id']} (no PDF)", file=sys.stderr)
            continue
        pdf = _fetch_bytes(view["pdf_url"])
        time.sleep(RATE_LIMIT)
        if not pdf:
            continue
        ident = view.get("case_number") or row["internal_id"]
        try:
            text = extract_pdf_markdown("INTL/CIIDRC", ident, pdf_bytes=pdf)
        except Exception as e:  # noqa: BLE001
            print(f"  extract error {ident}: {e}", file=sys.stderr)
            continue
        if not text or len(text) < 300:
            print(f"  skip {ident} (no/short text)", file=sys.stderr)
            continue
        rec = normalize(row, view, text)
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
    print("Testing CIIDRC connectivity...", file=sys.stderr)
    html = _fetch_text(INDEX_URL)
    if not html:
        print("  index fetch FAILED", file=sys.stderr)
        return False
    rows = parse_index(html)
    print(f"  index rows: {len(rows)}", file=sys.stderr)
    if not rows:
        return False
    for row in rows[:6]:
        view_html = _fetch_text(VIEW_URL.format(row["internal_id"]))
        time.sleep(RATE_LIMIT)
        if not view_html:
            continue
        view = parse_view(view_html)
        if not view.get("pdf_url"):
            continue
        pdf = _fetch_bytes(view["pdf_url"])
        time.sleep(RATE_LIMIT)
        if not pdf:
            continue
        text = extract_pdf_markdown(
            "INTL/CIIDRC", view.get("case_number") or row["internal_id"], pdf_bytes=pdf)
        if text and len(text) > 500:
            print(f"  decision text OK: {len(text)} chars (case {view.get('case_number')})",
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
