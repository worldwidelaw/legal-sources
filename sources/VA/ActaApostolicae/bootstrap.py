#!/usr/bin/env python3
"""
VA/ActaApostolicae - Vatican Apostolic Documents Fetcher

Fetches papal legislative documents (encyclicals, motu proprio, apostolic
constitutions, exhortations, letters, and bulls) from vatican.va using the
Adobe Experience Manager / Apache Sling JSON API.

Data source: https://www.vatican.va/
Method: AEM/Sling JSON API (append .N.json to any URL path)
License: Holy See / Vatican
Rate limit: ~1 second between requests

Popes covered: Leo XIV, Francis, Benedict XVI, John Paul II, Paul VI,
               John XXIII, Pius XII (~2,100 legislative documents)

Language handling: many acts (notably the ~640 John Paul II and ~356 Paul VI
apostolic constitutions erecting dioceses) have NO English body — the English
node carries `isemptybody: true`. The body exists in Latin (the original) and
the other official translations, so each document is tried across a language
chain and the first version carrying real text wins.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap --full     # Full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Alias for the full corpus run
  python bootstrap.py test                 # Test connectivity
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

SOURCE_ID = "VA/ActaApostolicae"
MODULE_DIR = Path(__file__).parent
SAMPLE_DIR = MODULE_DIR / "sample"
DATA_DIR = MODULE_DIR / "data"
RECORDS_FILE = DATA_DIR / "records.jsonl"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
BASE_URL = "https://www.vatican.va"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
}

DELAY = 1.0        # seconds between requests
TIMEOUT = 30       # per-request timeout (connect + read)
MAX_ATTEMPTS = 4
MIN_TEXT_CHARS = 50
# A body this long is unambiguously the real act, so stop walking languages.
SUFFICIENT_TEXT_CHARS = 1000

# Popes with content on vatican.va (slug -> display name)
POPES = {
    "leo-xiv": "Pope Leo XIV",
    "francesco": "Pope Francis",
    "benedict-xvi": "Pope Benedict XVI",
    "john-paul-ii": "Pope John Paul II",
    "paul-vi": "Pope Paul VI",
    "john-xxiii": "Pope John XXIII",
    "pius-xii": "Pope Pius XII",
}

# Legislative document types to fetch
LEG_TYPES = {
    "encyclicals": "Encyclical",
    "motu_proprio": "Motu Proprio",
    "apost_constitutions": "Apostolic Constitution",
    "apost_exhortations": "Apostolic Exhortation",
    "apost_letters": "Apostolic Letter",
    "bulls": "Papal Bull",
}

# Language site-trees on vatican.va. Used both to enumerate the union of
# document slugs in a section and, per document, to find a version that
# actually carries a body. English first (preferred corpus language), then
# Latin (the promulgation language for most acts), then the translations.
LANGS = ["en", "la", "it", "es", "fr", "pt", "de"]

SKIP_PREFIXES = ("jcr:", "sling:", "cq:", "rep:")


def strip_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r"<script.*?</script>", " ", html_text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"</?div[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_json(url: str, session: requests.Session) -> Optional[dict]:
    """Fetch JSON with a hard per-request timeout and bounded retries."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = session.get(url, headers=HEADERS, timeout=(10, TIMEOUT))
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = min(DELAY * (2 ** attempt), 60)
                print(f"  HTTP {resp.status_code}, retrying in {wait:.0f}s: {url}",
                      flush=True)
                time.sleep(wait)
                continue
            return None
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"  Request error (attempt {attempt + 1}/{MAX_ATTEMPTS}): {e}",
                  flush=True)
            time.sleep(min(DELAY * (2 ** attempt), 60))
    return None


def extract_text_from_jcr(jcr_content: dict) -> str:
    """
    Extract the document body out of the JCR container.

    The AEM tree is not uniform: some nodes put the body at
    container.vaticanrichtext.text, others directly at container.text, and a
    few nest it one level deeper. Walk the whole container and keep the
    longest string stored under a "text" key.
    """
    container = jcr_content.get("container")
    if not isinstance(container, dict):
        return ""

    best = ""

    def walk(node, depth=0):
        nonlocal best
        if depth > 4 or not isinstance(node, dict):
            return
        for key, val in node.items():
            if key == "text" and isinstance(val, str) and len(val) > len(best):
                best = val
            elif isinstance(val, dict):
                walk(val, depth + 1)

    walk(container)
    return strip_html(best)


def parse_date(date_str: str) -> Optional[str]:
    """Parse Vatican date strings to ISO format."""
    if not date_str:
        return None
    # Format: "Fri Jul 16 2021 12:00:00 GMT+0200"
    match = re.match(r"\w+ (\w+) (\d+) (\d{4})", date_str)
    if match:
        months = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
        }
        mon = months.get(match.group(1), "01")
        return f"{match.group(3)}-{mon}-{match.group(2).zfill(2)}"
    # Try ISO format
    match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if match:
        return match.group(1)
    return None


def list_documents(pope: str, section: str, session: requests.Session) -> list:
    """
    List every document slug in a section, unioned across language trees.

    A section's slug set is not identical between languages (e.g. Paul VI's
    apostolic letters: 229 under /en/ but 325 under /la/), so enumerating a
    single tree silently truncates the corpus.
    """
    slugs = []
    seen = set()
    for lang in LANGS:
        url = f"{BASE_URL}/content/{pope}/{lang}/{section}/documents.1.json"
        data = fetch_json(url, session)
        time.sleep(DELAY)
        if not data:
            continue
        for k in data.keys():
            if k.startswith(SKIP_PREFIXES) or k in seen:
                continue
            seen.add(k)
            slugs.append(k)
    return slugs


def fetch_document(pope: str, section: str, slug: str,
                   session: requests.Session) -> Optional[dict]:
    """
    Fetch a document, walking the language chain until a version has a body.

    English wins whenever it carries the act; otherwise the longest available
    body does. Returns None if no language version has a body (a genuine
    title-only stub).
    """
    candidates = {}  # lang -> (body_text, jcr)

    for lang in LANGS:
        url = f"{BASE_URL}/content/{pope}/{lang}/{section}/documents/{slug}.3.json"
        data = fetch_json(url, session)
        time.sleep(DELAY)
        if not data:
            continue

        jcr = data.get("jcr:content", {})

        # AEM's own marker that this translation has no body. It is serialised
        # as the *string* "true", not a JSON boolean.
        if str(jcr.get("isemptybody", "")).lower() == "true":
            continue

        text = extract_text_from_jcr(jcr)
        if len(text) < MIN_TEXT_CHARS:
            continue

        candidates[lang] = (text, jcr)
        if len(text) >= SUFFICIENT_TEXT_CHARS:
            break

    if not candidates:
        return None

    best = max(candidates, key=lambda l: len(candidates[l][0]))
    # Prefer English unless it is a stub next to a fuller translation.
    if "en" in candidates and \
            len(candidates["en"][0]) >= 0.6 * len(candidates[best][0]):
        best = "en"

    text, jcr = candidates[best]

    abstract_obj = jcr.get("abstract", {})
    abstract_html = abstract_obj.get("text", "") if isinstance(abstract_obj, dict) else ""
    abstract_text = strip_html(abstract_html)
    if abstract_text:
        text = abstract_text + "\n\n" + text

    return _build_record(pope, section, slug, jcr, best, text)


def _build_record(pope: str, section: str, slug: str, jcr: dict,
                  lang: str, text: str) -> dict:
    title = strip_html(jcr.get("jcr:title", slug))
    date_iso = parse_date(jcr.get("eventDate", ""))
    tags = jcr.get("cq:tags", [])

    doc_url = f"{BASE_URL}/content/{pope}/{lang}/{section}/documents/{slug}.html"

    doc_id = f"VA-{pope}-{slug}"
    if len(doc_id) > 100:
        h = hashlib.md5(doc_id.encode()).hexdigest()[:8]
        doc_id = doc_id[:90] + "_" + h

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": doc_url,
        "pope": POPES.get(pope, pope),
        "pope_slug": pope,
        "document_type": LEG_TYPES.get(section, section),
        "section": section,
        "language": lang,
        "tags": tags if isinstance(tags, list) else [tags] if tags else [],
    }


# ── Checkpoint ────────────────────────────────────────────────────────────

def load_checkpoint() -> set:
    """Return the set of already-processed 'pope/section/slug' keys."""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return set(json.load(f).get("done", []))
    except (json.JSONDecodeError, OSError):
        return set()


def save_checkpoint(done: set) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"done": sorted(done)}, f)
    tmp.replace(CHECKPOINT_FILE)


# ── Crawl ─────────────────────────────────────────────────────────────────

def fetch_all(session: requests.Session = None,
              sample=False,
              resume: bool = False) -> Generator[dict, None, None]:
    """
    Yield every legislative document that carries full text.

    `sample` is coerced: the VPS generic persister calls fetch_all(session,
    {...}) with a config dict as the second positional argument, which must
    NOT be read as "sample mode".
    """
    if session is None:
        session = requests.Session()
    sample = sample is True

    limit = 15 if sample else None
    done = load_checkpoint() if resume else set()
    fetched = 0
    skipped = 0
    seen = 0
    last_beat = time.time()

    for pope in POPES:
        for section in LEG_TYPES:
            slugs = list_documents(pope, section, session)
            if not slugs:
                continue

            print(f"  {pope}/{section}: {len(slugs)} documents", flush=True)

            for slug in slugs:
                key = f"{pope}/{section}/{slug}"
                if key in done:
                    continue

                record = fetch_document(pope, section, slug, session)
                seen += 1

                if record:
                    yield record
                    fetched += 1
                else:
                    skipped += 1

                if not sample:
                    done.add(key)
                    if seen % 50 == 0:
                        save_checkpoint(done)

                # Heartbeat regardless of whether anything was yielded — a
                # long run of body-less stubs used to look like a hang.
                if seen % 25 == 0 or time.time() - last_beat > 120:
                    last_beat = time.time()
                    print(f"    ... {seen} processed ({fetched} with text, "
                          f"{skipped} stubs) — at {pope}/{section}", flush=True)

                if limit and fetched >= limit:
                    return

    if not sample:
        save_checkpoint(done)
    print(f"  Total: {fetched} fetched, {skipped} stubs skipped", flush=True)


def save_record(record: dict, sample_dir: Path) -> None:
    """Save a record to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
    if len(safe_id) > 80:
        h = hashlib.md5(record["_id"].encode()).hexdigest()[:8]
        safe_id = safe_id[:70] + "_" + h
    with open(sample_dir / (safe_id + ".json"), "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    """Test that the Vatican JSON API is reachable."""
    session = requests.Session()
    print("Testing Vatican JSON API connectivity...")

    data = fetch_json(f"{BASE_URL}/content/francesco/en.1.json", session)
    if not data:
        print("FAIL: Cannot reach Vatican JSON API")
        return False
    sections = [k for k in data.keys() if k in LEG_TYPES]
    print(f"  Sections API: OK ({len(sections)} legislative sections for Pope Francis)")

    data = fetch_json(f"{BASE_URL}/content/francesco/en/encyclicals/documents.1.json",
                      session)
    if not data:
        print("FAIL: Cannot list documents")
        return False
    docs = [k for k in data.keys() if not k.startswith(SKIP_PREFIXES)]
    print(f"  Document listing: OK ({len(docs)} encyclicals)")

    if docs:
        time.sleep(DELAY)
        rec = fetch_document("francesco", "encyclicals", docs[0], session)
        if rec:
            print(f"  Document fetch: OK ({rec['title'][:50]}, "
                  f"{len(rec['text'])} chars, lang={rec['language']})")
        else:
            print("  Document fetch: FAIL")
            return False

    # The English-empty / Latin-full case that used to silently skip.
    time.sleep(DELAY)
    rec = fetch_document("john-paul-ii", "apost_constitutions",
                         "hf_jp-ii_apc_19860421_spirituali-militum-curae", session)
    if not rec or not rec["text"]:
        print("  Language-fallback fetch: FAIL")
        return False
    print(f"  Language-fallback fetch: OK ({len(rec['text'])} chars, "
          f"lang={rec['language']})")

    print("All tests passed.")
    return True


def bootstrap(sample: bool = False) -> None:
    """Sample mode writes to sample/; full mode streams to data/records.jsonl."""
    session = requests.Session()
    saved = 0

    if sample:
        if SAMPLE_DIR.exists():
            for f in SAMPLE_DIR.glob("*.json"):
                f.unlink()
        print("Sample bootstrap starting...", flush=True)
        for record in fetch_all(session, sample=True):
            save_record(record, SAMPLE_DIR)
            saved += 1
        print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        resume = CHECKPOINT_FILE.exists()
        print(f"Full bootstrap starting"
              f"{' (resuming from checkpoint)' if resume else ''}...", flush=True)
        with open(RECORDS_FILE, "a", encoding="utf-8") as out:
            for record in fetch_all(session, sample=False, resume=True):
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                saved += 1
                if saved % 50 == 0:
                    out.flush()
                    print(f"  Written {saved} records...", flush=True)
        print(f"\nBootstrap complete: {saved} records written to {RECORDS_FILE}")

    if saved == 0:
        print("ERROR: No records saved!")
        sys.exit(1)


def bootstrap_fast() -> dict:
    """Entry point used by the fleet wrapper — full corpus, streamed."""
    bootstrap(sample=False)
    return {"mode": "fast", "output": str(RECORDS_FILE)}


def main():
    parser = argparse.ArgumentParser(description="VA/ActaApostolicae bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only ~15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        sys.exit(0 if test_connectivity() else 1)
    elif args.command == "bootstrap-fast":
        bootstrap(sample=False)
    else:
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
