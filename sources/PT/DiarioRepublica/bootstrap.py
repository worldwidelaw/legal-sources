#!/usr/bin/env python3
"""
PT/DiarioRepublica -- Portuguese Official Journal (Diário da República)

Fetches the full text of Portuguese legislation published in Série I of the
Diário da República.

Data source (2026-07 rewrite)
-----------------------------
The former community mirror ``dre.tretas.org`` is now behind a Cloudflare
challenge (HTTP 403 to every non-browser client, including residential
vantages), so it can no longer be scraped. The official site
``diariodarepublica.pt`` is a locked OutSystems SPA whose data sits behind
POST "screenservices" endpoints that require browser-level reverse engineering.

This scraper now sources the corpus from the open ``legalize-dev/legalize-pt``
GitHub repository, which reproduces every Série I normative act of the Diário
da República since 1911 as clean Markdown with rich YAML front matter. The
repository is regenerated daily from the official OutSystems API of
diariodarepublica.pt (MIT-licensed pipeline; the legislative content itself is
"domínio público — publicações oficiais do Estado"). Each file records the
original official ``source`` URL (dre.pt) in its front matter.

Strategy
  - Full pull: stream the repository tarball (single bulk download, well under
    2 GB — history is excluded) and iterate every ``pt/*.md`` member. Each file
    already embeds the full born-digital text, so normalize() does no network.
  - Sample: fetch a spread of individual files via raw.githubusercontent.com
    (fast, no large download).
  - Incremental update: use the GitHub commits API to find files changed since
    a date, then fetch each via raw.githubusercontent.com.

Endpoints
  - Tarball:   https://codeload.github.com/legalize-dev/legalize-pt/tar.gz/refs/heads/main
  - Raw file:  https://raw.githubusercontent.com/legalize-dev/legalize-pt/main/{path}
  - Tree API:  https://api.github.com/repos/legalize-dev/legalize-pt/git/trees/main?recursive=1
  - Commits:   https://api.github.com/repos/legalize-dev/legalize-pt/commits

Coverage
  - Série I: Lei, Lei Constitucional, Decreto-Lei, Decreto, Decreto Regulamentar,
    Portaria, Resolução, plus Azores/Madeira regional legislation (~90,000 acts).

Usage
  python bootstrap.py bootstrap --sample   # 15 sample records (raw fetch)
  python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
  python bootstrap.py bootstrap-fast       # full pull, concurrent normalize
  python bootstrap.py update 2026-01-01    # documents changed since a date
  python bootstrap.py test                 # connectivity/parse test
"""

import re
import sys
import json
import time
import logging
import tarfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.diariorepublica")

REPO = "legalize-dev/legalize-pt"
BRANCH = "main"
TARBALL_URL = f"https://codeload.github.com/{REPO}/tar.gz/refs/heads/{BRANCH}"
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"
TREE_URL = f"https://api.github.com/repos/{REPO}/git/trees/{BRANCH}?recursive=1"
COMMITS_URL = f"https://api.github.com/repos/{REPO}/commits"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "LegalDataHunter/1.0 (legal research; open data)")

OFFICIAL_HOME = "https://diariodarepublica.pt"

# Minimum body length to treat a record as real full text.
MIN_TEXT = 60

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n(.*)$", re.S)


class DiarioRepublicaScraper(BaseScraper):
    SOURCE_ID = "PT/DiarioRepublica"

    def __init__(self, source_dir=None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": UA,
            "Accept": "application/vnd.github+json, */*",
        })

    # ── Markdown parsing ──────────────────────────────────────────────
    @staticmethod
    def _parse_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
        """Split a legalize-pt Markdown file into (front-matter dict, body)."""
        m = _FM_RE.match(content)
        if not m:
            return {}, content.strip()
        fm_raw, body = m.group(1), m.group(2)
        fm: Dict[str, Any] = {}
        if yaml is not None:
            try:
                loaded = yaml.safe_load(fm_raw)
                if isinstance(loaded, dict):
                    fm = loaded
            except Exception:
                fm = {}
        if not fm:  # minimal fallback parser
            for line in fm_raw.splitlines():
                if ":" in line:
                    k, _, v = line.partition(":")
                    fm[k.strip()] = v.strip().strip('"').strip("'")
        return fm, body.strip()

    def _raw_to_record_dict(self, path: str, content: str) -> Optional[Dict[str, Any]]:
        fm, body = self._parse_frontmatter(content)
        if len(body) < MIN_TEXT:
            return None
        fm["_body"] = body
        fm["_path"] = path
        return fm

    # ── HTTP ──────────────────────────────────────────────────────────
    def _get(self, url: str, timeout: int = 60, is_json: bool = False):
        for attempt in range(4):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp.json() if is_json else resp.text
                logger.debug("GET %s -> HTTP %s", url, resp.status_code)
                if resp.status_code in (403, 429):
                    time.sleep(2 ** attempt * 2)
                    continue
                return None
            except requests.RequestException as e:
                logger.warning("GET %s attempt %d failed: %s", url, attempt + 1, e)
                time.sleep(2 ** attempt)
        return None

    # ── Enumeration ───────────────────────────────────────────────────
    def _list_md_paths(self) -> List[str]:
        """All pt/*.md paths in the repo (single tree API call)."""
        data = self._get(TREE_URL, is_json=True)
        if not data or "tree" not in data:
            return []
        return [n["path"] for n in data["tree"]
                if n.get("type") == "blob"
                and n["path"].startswith("pt/")
                and n["path"].endswith(".md")]

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Stream the repository tarball and yield every Série I act (RAW dict).

        The tarball is the working-tree snapshot (no git history) — a single
        bulk download comfortably under 2 GB. Members are processed in stream
        mode so memory stays flat.
        """
        logger.info("Downloading legalize-pt tarball (streaming): %s", TARBALL_URL)
        resp = self.session.get(TARBALL_URL, stream=True, timeout=300)
        resp.raise_for_status()
        resp.raw.decode_content = True
        produced = 0
        with tarfile.open(fileobj=resp.raw, mode="r|gz") as tar:
            for member in tar:
                if not (member.isfile()
                        and member.name.endswith(".md")
                        and "/pt/" in member.name):
                    continue
                fh = tar.extractfile(member)
                if fh is None:
                    continue
                try:
                    content = fh.read().decode("utf-8", "replace")
                except Exception as e:  # noqa: BLE001
                    logger.debug("read %s failed: %s", member.name, e)
                    continue
                # Normalise the in-tar path (top dir is legalize-pt-<sha>/pt/..)
                idx = member.name.find("/pt/")
                path = member.name[idx + 1:] if idx >= 0 else member.name
                rec = self._raw_to_record_dict(path, content)
                if rec:
                    produced += 1
                    if produced % 5000 == 0:
                        logger.info("  ... %d acts parsed", produced)
                    yield rec
        resp.close()
        if produced == 0:
            raise RuntimeError(
                "legalize-pt tarball yielded 0 pt/*.md documents — "
                "download blocked, empty, or layout changed"
            )
        logger.info("Tarball parsed: %d acts", produced)

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        """Documents changed since `since` (date or year), via the commits API."""
        since_str = str(since)[:10]
        if re.fullmatch(r"\d{4}", since_str):
            since_str = f"{since_str}-01-01"
        since_iso = f"{since_str}T00:00:00Z"
        changed: Dict[str, None] = {}
        page = 1
        while page <= 100:
            data = self._get(
                f"{COMMITS_URL}?since={since_iso}&per_page=100&page={page}",
                is_json=True,
            )
            if not data:
                break
            for commit in data:
                sha = commit.get("sha")
                if not sha:
                    continue
                detail = self._get(f"{COMMITS_URL}/{sha}", is_json=True)
                for f in (detail or {}).get("files", []):
                    p = f.get("filename", "")
                    if p.startswith("pt/") and p.endswith(".md") \
                            and f.get("status") != "removed":
                        changed[p] = None
                time.sleep(0.3)
            if len(data) < 100:
                break
            page += 1
            time.sleep(0.3)
        logger.info("Update: %d changed pt/ files since %s", len(changed), since_str)
        for path in changed:
            content = self._get(f"{RAW_BASE}/{path}")
            if content:
                rec = self._raw_to_record_dict(path, content)
                if rec:
                    yield rec
            time.sleep(0.2)

    # ── Normalization ─────────────────────────────────────────────────
    @staticmethod
    def _iso_date(value: Any) -> Optional[str]:
        if not value:
            return None
        s = str(value)[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s) and s != "1900-01-01":
            return s
        return None

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        body = raw.get("_body") or ""
        if len(body.strip()) < MIN_TEXT:
            return None

        path = raw.get("_path", "")
        identifier = raw.get("identifier") or Path(path).stem
        title = (raw.get("title") or identifier or "").strip()
        rank = (raw.get("rank") or "").strip()
        department = (raw.get("department") or "").strip() or None
        official_number = raw.get("official_number")
        dr_number = raw.get("dr_number")
        pub_date = self._iso_date(raw.get("publication_date"))
        source_url = (raw.get("source") or "").strip() or OFFICIAL_HOME

        return {
            "_id": f"PT/DiarioRepublica/{identifier}",
            "_source": "PT/DiarioRepublica",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": body.strip(),
            "date": pub_date,
            "url": source_url,
            "document_type": rank or None,
            "creator": department,
            "law_number": str(official_number) if official_number is not None else None,
            "gazette_number": str(dr_number) if dr_number is not None else None,
            "eli_uri": identifier,
            "official_url": source_url,
            "status": raw.get("status") or None,
            "series": "Série I",
            "jurisdiction": "PT",
            "language": "pt",
        }

    # ── Test ──────────────────────────────────────────────────────────
    def test(self) -> bool:
        try:
            content = self._get(f"{RAW_BASE}/pt/DRE-L-1-2020.md")
            if not content:
                logger.error("Could not fetch a raw sample file")
                return False
            rec = self.normalize(self._raw_to_record_dict("pt/DRE-L-1-2020.md", content))
            ok = bool(rec and len(rec["text"]) > MIN_TEXT)
            if ok:
                logger.info("Test OK: %s -> %d chars", rec["title"], len(rec["text"]))
            return ok
        except Exception as e:  # noqa: BLE001
            logger.error("Test failed: %s", e)
            return False

    # ── Sample seeds (spread across ranks/eras; no big download) ──────
    def _sample_paths(self, n: int = 15) -> List[str]:
        paths = self._list_md_paths()
        if not paths:
            # static fallback (verified-existing files)
            return [
                "pt/DRE-L-1-2020.md", "pt/DRE-DL-48221.md", "pt/DRE-D-1-2000.md",
                "pt/DRE-P-1030-97.md", "pt/DRE-DR-33-77.md",
            ]
        step = max(1, len(paths) // n)
        return paths[::step][:n]


if __name__ == "__main__":
    scraper = DiarioRepublicaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            for path in scraper._sample_paths(15):
                content = scraper._get(f"{RAW_BASE}/{path}")
                if not content:
                    continue
                raw = scraper._raw_to_record_dict(path, content)
                if not raw:
                    continue
                record = scraper.normalize(raw)
                if not record:
                    continue
                with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info("[%d] %s — %d chars", count,
                            record.get("eli_uri"), len(record["text"]))
                time.sleep(0.2)
            logger.info("Done: %d sample records saved", count)
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            logger.info("bootstrap-fast done: %s", stats)
        else:
            stats = scraper.bootstrap()
            logger.info("bootstrap done: %s", stats)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") \
            else str(datetime.now().year)
        count = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                count += 1
                logger.info("[%d] %s", count, record.get("eli_uri"))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
