#!/usr/bin/env python3
"""
Montenegro Constitutional Court (Ustavni sud Crne Gore) - Case Law Scraper

Fetches Constitutional Court decisions from the official database at ustavnisud.me.

Access path
-----------
1. ``upit.php`` — DataTables server-side JSON API. Provides the decision INDEX
   (case number, date, document type, challenged act, keywords, articles).
2. ``obrada_fajlovi.php?iddok=<id>`` — per-decision attachment endpoint. Returns
   the download link(s) for the actual decision document (.docx / .doc / .pdf).
   THIS is where the real full text lives.

Why we do not use ``sadrzaj_fajlova`` for the text (issue #1254)
---------------------------------------------------------------
``upit.php`` exposes a ``sadrzaj_fajlova`` column that looks like full text but is
the publisher's *search index* copy: every diacritic, every punctuation mark and
every newline has been deleted server-side. Verified by querying the API's own
``sadrzaj`` search filter — ``sadrzaj=Draskovic`` with diacritics ("Drašković")
returns 0 hits while the stripped form "Drakovi" returns 1042, i.e. the stripping
is in the publisher's database column, not in our transport. It is therefore NOT
recoverable from that field, and case numbers embedded in it are corrupted
("U-I br. 116/26" -> "UI br 11626").

This scraper reads the attached decision documents instead, which carry correct
UTF-8 Montenegrin text with punctuation and line breaks intact. Records with no
attachment are SKIPPED rather than emitted with the corrupted index text.
"""

import argparse
import html
import io
import json
import os
import re
import struct
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
import yaml


BASE_URL = "http://www.ustavnisud.me/ustavnisud"
LIST_ENDPOINT = f"{BASE_URL}/upit.php"
FILES_ENDPOINT = f"{BASE_URL}/obrada_fajlovi.php"
ARCHIVE_PAGE = f"{BASE_URL}/arhiva.php"

PAGE_SIZE = 100
RATE_LIMIT_DELAY = 1.0
MAX_FILE_BYTES = 60_000_000

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# mod_security on ustavnisud.me answers 406 Not Acceptable unless the request
# looks like the site's own jQuery XHR (Accept + X-Requested-With + Referer).
HEADERS = {
    "User-Agent": USER_AGENT,
    "Referer": ARCHIVE_PAGE,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

# The column order upit.php expects; positions are fixed server-side.
COLUMNS = [
    "iddok",
    "datum",
    "djelovodni_broj",
    "vrsta_dokumenta",
    "komitent",
    "kljucne_rijeci_tagovi",
    "clan_ustava_cg_atr19",
    "clan_konvencije_atr20",
    "sadrzaj_fajlova",
    "osporeni_akt",
    "datum_sjednice",
]

CHECKPOINT_PATH = Path(__file__).parent / "data" / "checkpoint.json"
RECORDS_PATH = Path(__file__).parent / "data" / "records.jsonl"


def load_config() -> dict:
    """Load source configuration from config.yaml."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# document text extraction
# ---------------------------------------------------------------------------

def _tidy(text: str) -> str:
    """Normalise whitespace WITHOUT touching diacritics or punctuation.

    Deliberately conservative: collapses runs of spaces/tabs and blank lines but
    preserves every non-ASCII character, every punctuation mark and the line
    structure. See issue #1254 for what happens when this is over-aggressive.
    """
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0b", "\n")
    text = text.replace("\x07", " ").replace("\x00", "")
    # non-breaking / exotic spaces -> plain space
    text = re.sub(r"[   \t]+", " ", text)
    text = re.sub(r"[ ]{2,}", " ", text)
    text = re.sub(r"[ ]*\n[ ]*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_docx(data: bytes) -> str:
    """Extract text from an OOXML .docx using only the stdlib."""
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = [n for n in ("word/document.xml",) if n in zf.namelist()]
        if not names:
            return ""
        xml = zf.read("word/document.xml").decode("utf-8", errors="replace")
    xml = re.sub(r"<w:tab\b[^>]*/>", "\t", xml)
    xml = re.sub(r"<w:br\b[^>]*/>", "\n", xml)
    xml = re.sub(r"</w:p\s*>", "\n", xml)
    xml = re.sub(r"</w:tr\s*>", "\n", xml)
    xml = re.sub(r"<[^>]+>", "", xml)
    return _tidy(html.unescape(xml))


def extract_legacy_doc(data: bytes) -> str:
    """Extract text from a legacy binary Word (.doc, OLE) file.

    Walks the FIB -> Clx -> piece table so that both compressed (cp1250) and
    UTF-16 pieces decode correctly; that is what preserves the Montenegrin
    diacritics (č ć ž š đ) a naive byte scrape would mangle.
    """
    import olefile  # noqa: PLC0415 - optional dependency, only needed for .doc

    with olefile.OleFileIO(io.BytesIO(data)) as ole:
        if not ole.exists("WordDocument"):
            return ""
        wd = ole.openstream("WordDocument").read()
        flags = struct.unpack_from("<H", wd, 0x000A)[0]
        table_name = "1Table" if (flags >> 9) & 1 else "0Table"
        if not ole.exists(table_name):
            table_name = "0Table" if table_name == "1Table" else "1Table"
        if not ole.exists(table_name):
            return ""
        table = ole.openstream(table_name).read()

    fc_clx, lcb_clx = struct.unpack_from("<II", wd, 0x01A2)
    clx = table[fc_clx:fc_clx + lcb_clx]

    pcdt = None
    i = 0
    while i < len(clx):
        kind = clx[i]
        if kind == 1:  # Prc — formatting run, skip
            cb = struct.unpack_from("<H", clx, i + 1)[0]
            i += 3 + cb
        elif kind == 2:  # Pcdt — the piece table we want
            lcb = struct.unpack_from("<I", clx, i + 1)[0]
            pcdt = clx[i + 5:i + 5 + lcb]
            break
        else:
            break
    if not pcdt or len(pcdt) < 16:
        return ""

    n_pieces = (len(pcdt) - 4) // 12
    cps = list(struct.unpack_from("<%dI" % (n_pieces + 1), pcdt, 0))
    parts: List[str] = []
    pcd_base = 4 * (n_pieces + 1)
    for k in range(n_pieces):
        fc = struct.unpack_from("<I", pcdt, pcd_base + k * 8 + 2)[0]
        n_chars = cps[k + 1] - cps[k]
        if n_chars <= 0:
            continue
        if fc & 0x40000000:  # 8-bit compressed piece
            start = (fc & ~0x40000000) // 2
            parts.append(wd[start:start + n_chars].decode("cp1250", errors="replace"))
        else:
            parts.append(wd[fc:fc + n_chars * 2].decode("utf-16-le", errors="replace"))
    return _tidy("".join(parts))


def extract_pdf(data: bytes) -> str:
    """Extract text from a PDF, trying the fastest available library first."""
    try:
        import fitz  # PyMuPDF

        with fitz.open(stream=data, filetype="pdf") as doc:
            out = [page.get_text("text") for page in doc]
        text = _tidy("\n".join(out))
        if len(text) > 50:
            return text
    except Exception:
        pass

    try:
        import pdfplumber

        chunks = []
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for page in pdf.pages:
                chunks.append(page.extract_text() or "")
                # Release per-page layout caches; a few hundred-page decision
                # otherwise peaks at multiple GB (see OOM note in INBOX).
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
        text = _tidy("\n".join(chunks))
        if len(text) > 50:
            return text
    except Exception:
        pass

    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(data))
        return _tidy("\n".join((p.extract_text() or "") for p in reader.pages))
    except Exception:
        return ""


def extract_any(data: bytes, filename: str) -> str:
    """Dispatch on magic bytes first, filename extension second."""
    if not data:
        return ""
    if data[:4] == b"PK\x03\x04":
        return extract_docx(data)
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return extract_legacy_doc(data)
    if data[:5] == b"%PDF-":
        return extract_pdf(data)
    if data[:5] == b"{\\rtf":
        try:
            from striprtf.striprtf import rtf_to_text

            return _tidy(rtf_to_text(data.decode("cp1250", errors="replace")))
        except Exception:
            return ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext in ("html", "htm"):
        return _tidy(html.unescape(re.sub(r"<[^>]+>", " ", data.decode("utf-8", "replace"))))
    if ext == "txt":
        return _tidy(data.decode("utf-8", "replace"))
    return ""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    # Prime cookies / satisfy the Referer check.
    try:
        session.get(ARCHIVE_PAGE, timeout=60)
    except requests.RequestException:
        pass
    return session


def _post(session: requests.Session, url: str, data: dict, attempts: int = 5) -> requests.Response:
    delay = 2.0
    last: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            resp = session.post(url, data=data, timeout=90)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code} from {url}")
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last = exc
            if attempt == attempts - 1:
                break
            time.sleep(min(delay, 60))
            delay *= 2
    raise RuntimeError(f"{url} unreachable after {attempts} attempts: {last}")


def fetch_page(
    start: int = 0,
    length: int = PAGE_SIZE,
    session: Optional[requests.Session] = None,
    direction: str = "asc",
) -> dict:
    """Fetch a page of the decision index.

    Ordered ASCENDING by date by default so newly published decisions append at
    the tail; that keeps offset-based checkpoints valid across restarts.
    """
    if session is None:
        session = make_session()

    data: Dict[str, Any] = {
        "start": start,
        "length": length,
        "draw": 1,
        "order[0][column]": "1",  # datum
        "order[0][dir]": direction,
    }
    for i, col in enumerate(COLUMNS):
        data[f"columns[{i}][data]"] = col

    resp = _post(session, LIST_ENDPOINT, data)
    try:
        return resp.json()
    except ValueError as exc:
        raise RuntimeError(
            f"upit.php returned non-JSON (HTTP {resp.status_code}); first bytes: "
            f"{resp.content[:120]!r} — likely a mod_security block from this vantage"
        ) from exc


def fetch_attachments(iddok: str, session: requests.Session) -> List[Dict[str, str]]:
    """Return the attachment descriptors for one decision."""
    resp = _post(session, FILES_ENDPOINT, {"iddok": iddok})
    try:
        payload = resp.json()
    except ValueError:
        return []

    out: List[Dict[str, str]] = []
    for entry in payload.get("linkovi") or []:
        match = re.search(r"href='([^']+)'", entry.get("link", ""))
        if not match:
            continue
        rel = html.unescape(match.group(1)).lstrip(".").lstrip("/")
        out.append({
            "url": f"{BASE_URL}/{rel}",
            "name": entry.get("naziv_fajla") or rel.rsplit("/", 1)[-1],
            "label": entry.get("korisnicki_naziv") or "",
            "type": entry.get("tip") or "",
            "date": entry.get("datum") or "",
        })
    return out


def fetch_document_text(
    iddok: str, session: requests.Session
) -> Tuple[str, List[Dict[str, str]]]:
    """Download every attachment for a decision and return its combined text."""
    attachments = fetch_attachments(iddok, session)
    if not attachments:
        return "", []

    pieces: List[str] = []
    used: List[Dict[str, str]] = []
    for att in attachments:
        try:
            resp = session.get(att["url"], timeout=180, stream=True)
            resp.raise_for_status()
            data = resp.raw.read(MAX_FILE_BYTES + 1, decode_content=True)
        except requests.RequestException as exc:
            print(f"  ! download failed {att['url']}: {exc}", file=sys.stderr)
            continue
        if len(data) > MAX_FILE_BYTES:
            print(f"  ! oversized attachment skipped {att['url']}", file=sys.stderr)
            continue

        text = extract_any(data, att["name"])
        if not text:
            continue
        label = att["label"] or att["type"]
        pieces.append(f"[{label}]\n{text}" if label and len(attachments) > 1 else text)
        used.append(att)

    return "\n\n".join(pieces).strip(), used


# ---------------------------------------------------------------------------
# normalisation
# ---------------------------------------------------------------------------

def normalize_date(date_str: str) -> Optional[str]:
    """Normalize date to ISO 8601 format."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y.%m.%d", "%d.%m.%Y", "%d.%m.%Y."):
        try:
            dt = datetime.strptime(date_str, fmt)
        except ValueError:
            continue
        if dt.year < 1900:
            return None
        return dt.strftime("%Y-%m-%d")
    return None


def extract_case_number(djel_broj: str) -> Dict[str, Any]:
    """Extract structured case number components."""
    result: Dict[str, Any] = {"raw": djel_broj}
    if not djel_broj:
        return result

    # e.g. "U-III br.383/25", "U-I br. 7/17", "Už-III br. 563/14"
    match = re.match(r"(U[žz]?-[IVX]+)\s*br\.?\s*(\d+)\s*/\s*(\d+)", djel_broj)
    if match:
        result["type"] = match.group(1)
        result["number"] = int(match.group(2))
        year = int(match.group(3))
        result["year"] = 2000 + year if year < 50 else 1900 + year
    return result


def normalize(raw: dict, text: str = "", attachments: Optional[List[dict]] = None) -> dict:
    """Transform a raw index row plus its extracted document text into the schema."""
    attachments = attachments or []

    iddok = str(raw.get("iddok") or raw.get("0") or "").strip()
    case_number = (raw.get("djelovodni_broj") or raw.get("3") or "").strip()
    doc_type = (raw.get("vrsta_dokumenta") or raw.get("4") or "").strip()
    case_info = extract_case_number(case_number)

    decision_date = normalize_date(raw.get("datum") or raw.get("2") or "")
    session_date = normalize_date(raw.get("datum_sjednice") or raw.get("10") or "")

    title = f"{case_number} - {doc_type}" if doc_type else case_number

    return {
        "_id": f"ME/ConstitutionalCourt/{iddok}",
        "_source": "ME/ConstitutionalCourt",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        # Real document text: UTF-8, punctuation and line breaks intact.
        "text": text,
        "date": decision_date or session_date,
        # Per-decision endpoint (accepts GET) that resolves to the current
        # download link for this decision's document(s). The site exposes no
        # other per-document permalink — arhiva.php ignores query parameters.
        "url": f"{FILES_ENDPOINT}?iddok={iddok}",
        "case_number": case_number,
        "case_type": case_info.get("type"),
        "case_year": case_info.get("year"),
        "document_type": doc_type,
        "session_date": session_date,
        "challenged_act": (raw.get("osporeni_akt") or raw.get("9") or "").strip(),
        "keywords": (raw.get("kljucne_rijeci_tagovi") or raw.get("5") or "").strip(),
        "constitutional_articles": (raw.get("clan_ustava_cg_atr19") or raw.get("6") or "").strip(),
        "convention_articles": (raw.get("clan_konvencije_atr20") or raw.get("7") or "").strip(),
        "applicant": (raw.get("komitent") or raw.get("1") or "").strip(),
        "internal_id": iddok,
        "language": "sr",
        "document_title": (attachments[0]["label"] if attachments else "") or None,
        "document_files": [
            {"name": a["name"], "type": a["type"], "url": a["url"]} for a in attachments
        ],
    }


# ---------------------------------------------------------------------------
# checkpointing
# ---------------------------------------------------------------------------

def _load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (ValueError, OSError):
            pass
    return {"offset": 0, "emitted": 0, "skipped": 0}


def _save_checkpoint(state: dict) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    tmp.replace(CHECKPOINT_PATH)


# ---------------------------------------------------------------------------
# crawl
# ---------------------------------------------------------------------------

def fetch_all(sample: bool = False, resume: bool = True) -> Iterator[dict]:
    """Yield decisions with real full text, skipping those with no document."""
    session = make_session()

    probe = fetch_page(start=0, length=1, session=session)
    total = int(probe.get("recordsFiltered") or probe.get("recordsTotal") or 0)
    print(f"Index reports {total} decisions", file=sys.stderr)
    if total == 0:
        raise RuntimeError("upit.php reported 0 decisions — refusing to report success")

    sample_target = 15
    if sample:
        state = {"offset": 0, "emitted": 0, "skipped": 0}
        # Newest-first for samples so the committed fixtures stay current.
        direction = "desc"
    else:
        state = _load_checkpoint() if resume else {"offset": 0, "emitted": 0, "skipped": 0}
        direction = "asc"
        if state["offset"]:
            print(f"Resuming from offset {state['offset']}", file=sys.stderr)

    offset = int(state["offset"])
    emitted = int(state["emitted"])
    skipped = int(state["skipped"])

    while offset < total:
        page = fetch_page(start=offset, length=PAGE_SIZE, session=session, direction=direction)
        rows = page.get("data") or []
        if not rows:
            break
        print(
            f"Index rows {offset + 1}-{offset + len(rows)} of {total} "
            f"(emitted {emitted}, no-document {skipped})",
            file=sys.stderr,
        )

        for row in rows:
            iddok = str(row.get("iddok") or row.get("0") or "").strip()
            if not iddok:
                continue
            text, attachments = fetch_document_text(iddok, session)
            if len(text) < 200:
                # No attachment (or unreadable one). We deliberately do NOT fall
                # back to sadrzaj_fajlova: see issue #1254.
                skipped += 1
            else:
                yield normalize(row, text, attachments)
                emitted += 1
                if sample and emitted >= sample_target:
                    print(
                        f"Sample complete: {emitted} decisions "
                        f"({skipped} skipped, no readable document)",
                        file=sys.stderr,
                    )
                    return
            time.sleep(RATE_LIMIT_DELAY)

        offset += len(rows)
        if not sample:
            _save_checkpoint({"offset": offset, "emitted": emitted, "skipped": skipped})

    print(
        f"Done: {emitted} decisions with full text, {skipped} skipped (no readable document)",
        file=sys.stderr,
    )


def fetch_updates(since: str) -> Iterator[dict]:
    """Fetch decisions dated on or after ``since``."""
    since_str = datetime.fromisoformat(since.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    print(f"Fetching updates since {since_str}...", file=sys.stderr)

    session = make_session()
    offset = 0
    emitted = 0

    while True:
        page = fetch_page(start=offset, length=PAGE_SIZE, session=session, direction="desc")
        rows = page.get("data") or []
        if not rows:
            break

        for row in rows:
            record_date = normalize_date(row.get("datum") or row.get("2") or "")
            if record_date and record_date < since_str:
                print(f"Fetched {emitted} updated decisions", file=sys.stderr)
                return
            iddok = str(row.get("iddok") or row.get("0") or "").strip()
            text, attachments = fetch_document_text(iddok, session)
            if len(text) >= 200:
                yield normalize(row, text, attachments)
                emitted += 1
            time.sleep(RATE_LIMIT_DELAY)

        offset += len(rows)

    print(f"Fetched {emitted} updated decisions", file=sys.stderr)


def save_samples(records: list, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        filepath = output_dir / f"sample_{i + 1:03d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"Saved {filepath}", file=sys.stderr)


def run_full() -> int:
    """Stream the whole corpus to data/records.jsonl (what the fleet ingests)."""
    RECORDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as out:
        for record in fetch_all(sample=False):
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1
            if written % 100 == 0:
                out.flush()
    print(f"Wrote {written} records to {RECORDS_PATH}", file=sys.stderr)
    return written


def run_test() -> int:
    """Connectivity + extraction smoke test."""
    session = make_session()
    page = fetch_page(start=0, length=5, session=session, direction="desc")
    total = page.get("recordsFiltered") or page.get("recordsTotal")
    print(f"upit.php OK — {total} decisions indexed")
    for row in page.get("data") or []:
        iddok = str(row.get("iddok"))
        text, atts = fetch_document_text(iddok, session)
        kinds = ", ".join(a["name"].rsplit(".", 1)[-1] for a in atts) or "none"
        has_dia = bool(re.search(r"[čćžšđČĆŽŠĐ]", text))
        print(
            f"  {iddok} {row.get('datum')} {row.get('djelovodni_broj')}: "
            f"files={kinds} chars={len(text)} diacritics={has_dia}"
        )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Montenegro Constitutional Court case law scraper"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    for name in ("bootstrap", "bootstrap-fast"):
        sub = subparsers.add_parser(name, help="Run data collection")
        sub.add_argument("--sample", action="store_true", help="Fetch a small sample only")
        sub.add_argument("--full", action="store_true", help="Fetch the whole corpus")
        sub.add_argument("--output", type=str, default="sample", help="Sample output dir")
        sub.add_argument(
            "--no-resume", action="store_true", help="Ignore the saved checkpoint"
        )

    updates_parser = subparsers.add_parser("updates", help="Fetch recent decisions")
    updates_parser.add_argument("--since", type=str, required=True, help="ISO date")

    subparsers.add_parser("test", help="Connectivity and extraction smoke test")

    args = parser.parse_args()

    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            records = list(fetch_all(sample=True))
            save_samples(records, Path(__file__).parent / args.output)

            print("\n=== Validation Summary ===", file=sys.stderr)
            lengths = [len(r.get("text", "")) for r in records]
            with_text = sum(1 for n in lengths if n > 200)
            with_dia = sum(1 for r in records if re.search(r"[čćžšđČĆŽŠĐ]", r["text"]))
            with_punct = sum(1 for r in records if re.search(r"[.,;:()]", r["text"]))
            with_nl = sum(1 for r in records if "\n" in r["text"])
            print(f"Records fetched: {len(records)}", file=sys.stderr)
            print(f"With substantial text: {with_text}/{len(records)}", file=sys.stderr)
            print(f"With diacritics: {with_dia}/{len(records)}", file=sys.stderr)
            print(f"With punctuation: {with_punct}/{len(records)}", file=sys.stderr)
            print(f"With newlines: {with_nl}/{len(records)}", file=sys.stderr)
            print(f"Distinct urls: {len({r['url'] for r in records})}", file=sys.stderr)
            if lengths:
                print(
                    f"Text chars min/avg/max: {min(lengths)}/"
                    f"{sum(lengths) // len(lengths)}/{max(lengths)}",
                    file=sys.stderr,
                )
        else:
            run_full()

    elif args.command == "updates":
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "test":
        sys.exit(run_test())

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
