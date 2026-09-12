#!/usr/bin/env python3
"""
SV/SC-Decisions -- El Salvador Superintendencia de Competencia, Casos en Línea

Cases handled by El Salvador's competition authority (Superintendencia de
Competencia, SC). The agency operates "Casos en Línea", a public portal
(app.sc.gob.sv) that lists every case alongside its full administrative and
judicial timeline.

Data source: https://app.sc.gob.sv/
License: Open Government Data (Ley de Acceso a la Información Pública)

Strategy
--------
  - The portal index page lists every case as ``caso.php?id=<N>``. We extract
    those IDs (there are ~70+).
  - For each case, GET ``caso.php?id=<N>`` and parse:
        * Title  -> ``<title>Casos en Línea | Caso XYZ</title>``
        * Subject summary  -> ``<meta property="og:description">``
        * Metadata block (Agente Económico, Práctica, Mercado, Sanción,
          Fecha de Apertura)
        * The full chronological timeline: each event has a date, an actor
          (SC, CD, CDSC, CAMCO, SCA, SCN, SIC, ...), a free-text description,
          and an optional PDF link to the underlying resolution / ruling.
  - The case page itself carries the case's narrative. We compose the
    document ``text`` as: title + subject + opening date + per-event lines
    (date / actor / description / PDF URL).
  - The PDF resolutions are linked but not downloaded — the case page already
    yields several KB of substantive Spanish-language case narrative, which
    is the agreed legal-document text for this source.

Usage
-----
  python bootstrap.py bootstrap --sample   # ~12 cases for validation
  python bootstrap.py bootstrap            # all cases
  python bootstrap.py test-api             # quick connectivity check
"""

import argparse
import html as html_lib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Iterable, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

SOURCE_ID = "SV/SC-Decisions"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SV.SC-Decisions")

BASE_URL = "https://app.sc.gob.sv"
INDEX_URL = f"{BASE_URL}/"
CASE_URL = f"{BASE_URL}/caso.php?id={{case_id}}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-SV,es;q=0.9,en;q=0.8",
}

RATE_LIMIT = 1.0
SAMPLE_TARGET = 12
MIN_TEXT_CHARS = 400

# Map Spanish month abbreviations (some pages use them) and English month names
# that appear in the timeline spans (the portal renders dates with English
# abbreviated months -- "27 Jan 2022", "16 Dec 2021", etc.).
_MONTHS_EN = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Sept": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}
_MONTHS_ES = {
    "Ene": 1, "Feb": 2, "Mar": 3, "Abr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Ago": 8, "Sep": 9, "Sept": 9, "Oct": 10, "Nov": 11, "Dic": 12,
}


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    # The portal embeds a few stray U+FFFD replacement chars (the source HTML
    # mixes utf-8 with what looks like windows-1252-decoded prose). Drop them.
    text = text.replace("�", "")
    return re.sub(r"\s+", " ", html_lib.unescape(text)).strip()


def _parse_event_date(raw: str) -> Optional[str]:
    """Parse '27 Jan 2022' / '27 Ene 2022' into ISO 8601 YYYY-MM-DD."""
    m = re.match(r"(\d{1,2})\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+(\d{4})", raw or "")
    if not m:
        return None
    day, month_word, year = m.group(1), m.group(2)[:4], m.group(3)
    mo = _MONTHS_EN.get(month_word.capitalize()) or _MONTHS_ES.get(month_word.capitalize())
    if not mo:
        return None
    try:
        return datetime(int(year), mo, int(day)).date().isoformat()
    except ValueError:
        return None


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _fetch(session: requests.Session, url: str, *, tries: int = 3) -> Optional[str]:
    last_exc = None
    for attempt in range(1, tries + 1):
        try:
            resp = session.get(url, timeout=(15, 60))
            resp.raise_for_status()
            # The portal claims utf-8 but mixes encodings; requests' default
            # apparent_encoding is reasonable. Force utf-8 and let unparsable
            # bytes become U+FFFD, which _clean() then drops.
            resp.encoding = "utf-8"
            return resp.text
        except Exception as e:
            last_exc = e
            logger.warning("GET %s failed (attempt %d/%d): %s", url, attempt, tries, e)
            time.sleep(RATE_LIMIT * attempt)
    logger.error("Giving up on %s: %s", url, last_exc)
    return None


def discover_case_ids(session: requests.Session) -> list[int]:
    """Scrape the index page for every ``caso.php?id=<N>`` link."""
    html = _fetch(session, INDEX_URL)
    if not html:
        return []
    ids = sorted({int(m) for m in re.findall(r"caso\.php\?id=(\d+)", html)})
    logger.info("Discovered %d case IDs from index", len(ids))
    return ids


def _meta_field(soup: BeautifulSoup, label_text: str) -> str:
    """Read the value that follows a bold label paragraph in the metadata box.

    The portal renders each meta line as:
        <p style="font-weight:bold;">Agente Económico</p>
        <p>QUIMAGRO, S.A. DE C.V.</p>
    """
    norm_label = label_text.lower().rstrip(":").strip()
    for p in soup.find_all("p"):
        style = (p.get("style") or "").lower()
        if "font-weight" not in style:
            continue
        ptext = _clean(p.get_text()).lower().rstrip(":").strip()
        if ptext == norm_label:
            nxt = p.find_next_sibling("p")
            if nxt is not None:
                return _clean(nxt.get_text())
    return ""


def _extract_timeline(soup: BeautifulSoup) -> list[dict]:
    """Pull every dated event from the case timeline.

    Each event is rendered as a ``<div id="cr-cntlft">`` (left column) or
    ``<div id="cr-cntrght">`` (right column) block that contains:
        <span>27 Jan 2022 / CAMCO</span>
        <a href="https://uploads.sc.gob.sv/.../foo.pdf">...</a>  (optional)
        <p>Free-text description of the event.</p>
        <div id="cntlf-date">925 dias</div>
    """
    events: list[dict] = []
    for blk in soup.find_all("div", id=re.compile(r"^cr-cnt(lft|rght)$")):
        span = blk.find("span")
        if not span:
            continue
        m = re.match(
            r"(\d{1,2}\s+[A-Za-zÁÉÍÓÚáéíóú]+\s+\d{4})\s*/\s*([A-Z][A-Z0-9]*)",
            _clean(span.get_text()),
        )
        if not m:
            continue
        raw_date, actor = m.group(1), m.group(2)
        iso_date = _parse_event_date(raw_date)

        desc_p = blk.find("p")
        description = _clean(desc_p.get_text()) if desc_p else ""

        pdf_url = None
        link = blk.find("a")
        if link and link.get("href", "").lower().endswith(".pdf"):
            pdf_url = link["href"].strip()

        if not description and not pdf_url:
            continue
        events.append(
            {
                "date": iso_date,
                "raw_date": raw_date,
                "actor": actor,
                "description": description,
                "pdf_url": pdf_url,
            }
        )
    # Sort oldest -> newest so the narrative reads chronologically.
    events.sort(key=lambda e: e["date"] or "0000-00-00")
    return events


def _parse_case(case_id: int, html: str) -> Optional[dict]:
    """Parse a case page into a structured record. Returns None if unusable."""
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    raw_title = _clean(title_tag.get_text()) if title_tag else ""
    # "Casos en Línea | Caso QUIMAGRO" -> "Caso QUIMAGRO"
    title = re.sub(r"^Casos en L[ií]nea\s*\|\s*", "", raw_title).strip()
    if not title:
        return None

    og_desc = ""
    og = soup.find("meta", attrs={"property": "og:description"})
    if og and og.get("content"):
        og_desc = _clean(og["content"])

    economic_agent = _meta_field(soup, "Agente Económico")
    practice = _meta_field(soup, "Práctica")
    market = _meta_field(soup, "Mercado")
    sanction = _meta_field(soup, "Sanción")
    opened_raw = _meta_field(soup, "Fecha de Apertura")
    opened_iso = _parse_event_date(opened_raw) if opened_raw else None

    events = _extract_timeline(soup)
    if not events and not og_desc:
        return None

    return {
        "case_id": case_id,
        "title": title,
        "summary": og_desc,
        "economic_agent": economic_agent,
        "practice": practice,
        "market": market,
        "sanction": sanction,
        "opened_date": opened_iso,
        "opened_raw": opened_raw,
        "events": events,
    }


def _compose_text(parsed: dict) -> str:
    """Assemble the document body from the parsed case structure."""
    parts: list[str] = []
    parts.append(f"# {parsed['title']}")
    if parsed["summary"]:
        parts.append("")
        parts.append(parsed["summary"])

    meta_lines = []
    if parsed["economic_agent"]:
        meta_lines.append(f"- Agente económico: {parsed['economic_agent']}")
    if parsed["practice"]:
        meta_lines.append(f"- Práctica: {parsed['practice']}")
    if parsed["market"]:
        meta_lines.append(f"- Mercado: {parsed['market']}")
    if parsed["sanction"]:
        meta_lines.append(f"- Sanción: {parsed['sanction']}")
    if parsed["opened_raw"]:
        meta_lines.append(f"- Fecha de apertura: {parsed['opened_raw']}")
    if meta_lines:
        parts.append("")
        parts.append("## Datos del caso")
        parts.extend(meta_lines)

    if parsed["events"]:
        parts.append("")
        parts.append("## Cronología de actuaciones")
        for ev in parsed["events"]:
            date_str = ev["date"] or ev["raw_date"]
            line = f"- {date_str} ({ev['actor']}): {ev['description']}"
            if ev["pdf_url"]:
                line += f" [Resolución PDF: {ev['pdf_url']}]"
            parts.append(line)
    return "\n".join(parts).strip()


def normalize(parsed: dict) -> dict:
    """Transform a parsed case into the standard LDH schema."""
    case_id = parsed["case_id"]
    text = _compose_text(parsed)

    # Document date: latest event date, falling back to opened_date.
    event_dates = [e["date"] for e in parsed["events"] if e["date"]]
    doc_date = max(event_dates) if event_dates else parsed["opened_date"]

    return {
        "_id": f"sv-sc-{case_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": parsed["title"],
        "text": text,
        "date": doc_date,
        "url": CASE_URL.format(case_id=case_id),
        "case_id": case_id,
        "economic_agent": parsed["economic_agent"] or None,
        "practice": parsed["practice"] or None,
        "market": parsed["market"] or None,
        "sanction": parsed["sanction"] or None,
        "opened_date": parsed["opened_date"],
        "event_count": len(parsed["events"]),
        "language": "es",
        "country": "SV",
    }


def _iter_case_ids(session: requests.Session) -> Iterable[int]:
    ids = discover_case_ids(session)
    if not ids:
        logger.error("No case IDs found on index page; aborting.")
    return ids


def fetch_all(session: Optional[requests.Session] = None) -> Generator[dict, None, None]:
    session = session or _session()
    for case_id in _iter_case_ids(session):
        url = CASE_URL.format(case_id=case_id)
        html = _fetch(session, url)
        if not html:
            continue
        parsed = _parse_case(case_id, html)
        if not parsed:
            logger.warning("Case %d: empty / unparseable, skipped", case_id)
            continue
        record = normalize(parsed)
        if len(record["text"]) < MIN_TEXT_CHARS:
            logger.warning(
                "Case %d (%s): text only %d chars, skipped",
                case_id, parsed["title"], len(record["text"]),
            )
            continue
        yield record
        time.sleep(RATE_LIMIT)


def fetch_updates(since: Optional[str] = None) -> Generator[dict, None, None]:
    """The portal exposes no per-record last-modified, so updates require a
    full sweep. Downstream consumers can dedupe by ``_id`` and re-merge."""
    yield from fetch_all()


def _bootstrap(sample: bool) -> int:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    saved = 0
    target = SAMPLE_TARGET if sample else None
    for rec in fetch_all():
        out = SAMPLE_DIR / f"{rec['_id']}.json"
        with out.open("w", encoding="utf-8") as fh:
            json.dump(rec, fh, ensure_ascii=False, indent=2)
        saved += 1
        logger.info(
            "Saved %s (%d events, %d chars) -> %s",
            rec["_id"], rec["event_count"], len(rec["text"]), out.name,
        )
        if target is not None and saved >= target:
            break
    logger.info("Bootstrap complete: %d records", saved)
    return saved


def _test_api() -> int:
    session = _session()
    html = _fetch(session, INDEX_URL)
    if not html:
        print("FAIL: could not fetch index")
        return 1
    ids = sorted({int(m) for m in re.findall(r"caso\.php\?id=(\d+)", html)})
    print(f"Index OK: {len(ids)} case IDs discovered")
    if ids:
        probe = ids[0]
        case_html = _fetch(session, CASE_URL.format(case_id=probe))
        if case_html:
            parsed = _parse_case(probe, case_html)
            if parsed:
                rec = normalize(parsed)
                print(f"Case {probe}: '{rec['title']}', {rec['event_count']} events, {len(rec['text'])} chars")
                return 0
    print("FAIL: could not parse probe case")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="SV/SC-Decisions data fetcher")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_boot = sub.add_parser("bootstrap", help="Fetch records and save to sample/")
    p_boot.add_argument("--sample", action="store_true", help="Stop after the sample target")
    sub.add_parser("test-api", help="Connectivity / parse probe")
    args = parser.parse_args()

    if args.cmd == "bootstrap":
        n = _bootstrap(sample=args.sample)
        return 0 if n > 0 else 1
    if args.cmd == "test-api":
        return _test_api()
    return 1


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    sys.exit(main())
