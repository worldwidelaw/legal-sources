#!/usr/bin/env python3
"""
TW/JudicialYuanOD -- Judicial Yuan Open Data (Taiwan court judgments)

Full-text Taiwan court judgments (裁判書) from the Judicial Yuan open data
platform, via two complementary APIs -- both authenticated at RUNTIME from
durable ACCOUNT credentials (never a stored token):

  1. Historical monthly archives  (opendata.judicial.gov.tw)
       POST /api/MemberTokens { memberAccount, pwd } -> JWT (Bearer)
       GET  /api/Datasets?keyword=裁判書              -> monthly datasets
            titled "{YYYYMM}裁判書" (+ optional "--(YYYYMMDDUpdate)")
       GET  /api/FilesetLists/{fileSetId}/file        -> RAR of judgment JSON
     Used as the bulk/backfill + recovery path (newest version per month).

  2. Delta / full-text API  (data.judicial.gov.tw) -- 00:00-06:00 Asia/Taipei
       POST /jdg/api/Auth { user, password } -> 6-hour token
       GET  /jdg/api/JList                    -> recently added/corrected JIDs
       GET  /jdg/api/JDoc                     -> per-judgment full text + flags

Credentials come from TW_JUDICIAL_ACCOUNT / TW_JUDICIAL_PASSWORD. Account
membership needs email reconfirmation roughly every three months.

Usage:
  python bootstrap.py test                 # auth + list archives (needs creds)
  python bootstrap.py bootstrap --sample   # 15 sample judgments
  python bootstrap.py bootstrap --full     # full historical pull
  python bootstrap.py bootstrap-fast       # concurrent historical pull
  python bootstrap.py update               # nightly delta (window-gated)
  python test_scraper.py                   # offline regression tests
"""

import os
import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple

import urllib.request
import urllib.error
import urllib.parse

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TW.JudicialYuanOD")

OPENDATA_BASE = "https://opendata.judicial.gov.tw"
MEMBER_TOKENS = f"{OPENDATA_BASE}/api/MemberTokens"
DATASETS = f"{OPENDATA_BASE}/api/Datasets"
FILE_DOWNLOAD = f"{OPENDATA_BASE}/api/FilesetLists/{{fileSetId}}/file"
JUDGMENT_KEYWORD = "裁判書"

JDG_BASE = "https://data.judicial.gov.tw"
JDG_AUTH = f"{JDG_BASE}/jdg/api/Auth"
JDG_LIST = f"{JDG_BASE}/jdg/api/JList"
JDG_DOC = f"{JDG_BASE}/jdg/api/JDoc"

ENV_ACCOUNT = "TW_JUDICIAL_ACCOUNT"
ENV_PASSWORD = "TW_JUDICIAL_PASSWORD"

# Delta API service window (Asia/Taipei is UTC+8, no DST)
TAIPEI = timezone(timedelta(hours=8))
WINDOW_START_HOUR = 0
WINDOW_END_HOUR = 6

USER_AGENT = "LegalDataHunter/1.0 (legal research; open data)"

# Monthly archive title: "199601裁判書" or "199605裁判書--(20240315Update)"
_MONTH_TITLE_RE = re.compile(r"^(\d{4})(\d{2})裁判書")
_UPDATE_SUFFIX_RE = re.compile(r"--?\(?\s*(\d{8})\s*Update", re.IGNORECASE)


class TWAuthError(RuntimeError):
    """Authentication could not be established."""


class MissingCredentials(TWAuthError):
    """Durable account credentials are not configured."""


class ServiceWindowClosed(RuntimeError):
    """The delta API is outside its nightly service window."""


# ── token-response parsing (defensive across key spellings) ───────────

_TOKEN_KEYS = ("token", "accessToken", "access_token", "jwt", "jwtToken", "data")


def _extract_token(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for k in _TOKEN_KEYS:
        v = payload.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, dict):
            inner = _extract_token(v)
            if inner:
                return inner
    return None


def in_service_window(now: Optional[datetime] = None) -> bool:
    """True when the current Asia/Taipei time is within 00:00-06:00."""
    now = now or datetime.now(TAIPEI)
    if now.tzinfo is None:
        now = now.replace(tzinfo=TAIPEI)
    hour = now.astimezone(TAIPEI).hour
    return WINDOW_START_HOUR <= hour < WINDOW_END_HOUR


def select_newest_per_month(datasets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Given monthly-archive dataset dicts, keep the newest version per month.

    Each dataset must expose a ``title`` and a ``fileSetId``. "Newest" is
    decided by, in order: the ``--(YYYYMMDDUpdate)`` suffix date, then
    ``updateTime``/``modifiedDate``, then the numeric ``datasetId``.
    """
    best: Dict[str, Tuple[tuple, Dict[str, Any]]] = {}
    for ds in datasets:
        title = ds.get("title") or ""
        m = _MONTH_TITLE_RE.match(title)
        if not m:
            continue
        month = m.group(1) + m.group(2)  # YYYYMM
        upd = _UPDATE_SUFFIX_RE.search(title)
        upd_key = upd.group(1) if upd else "00000000"
        tstr = ds.get("updateTime") or ds.get("modifiedDate") or ""
        rank = (upd_key, str(tstr), int(ds.get("datasetId") or 0))
        if month not in best or rank > best[month][0]:
            best[month] = (rank, ds)
    return [v[1] for k, v in sorted(best.items())]


# ── judgment JSON normalization (Judicial Yuan 裁判書 schema) ──────────

def parse_judgment_obj(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map a Judicial Yuan judgment JSON object to a raw record.

    The published schema uses upper-case keys: JID, JYEAR, JCASE, JNO,
    JDATE (YYYYMMDD), JTITLE, JFULL (full text). Some feeds nest under
    "judgement"/"judgment"; deletion feeds set an IS_DELETED / REMOVE flag.
    """
    if not isinstance(obj, dict):
        return None
    # Unwrap common nesting
    for wrapper in ("judgement", "judgment", "data"):
        if isinstance(obj.get(wrapper), dict):
            obj = obj[wrapper]
            break

    def g(*keys):
        for k in keys:
            for cand in (k, k.lower(), k.upper()):
                if cand in obj and obj[cand] not in (None, ""):
                    return obj[cand]
        return None

    jid = g("JID", "id")
    if not jid:
        return None

    if _is_deleted(obj):
        return {"jid": jid, "_deleted": True}

    text = g("JFULL", "JFULLTEXT", "full", "content")
    if not text or len(str(text).strip()) < 50:
        return None
    title = g("JTITLE", "title") or str(jid)
    date = _norm_jdate(g("JDATE", "date"))
    return {
        "jid": str(jid),
        "title": str(title).strip(),
        "text": _clean(str(text)),
        "date": date,
        "court": g("JCASE", "court"),
        "year": g("JYEAR"),
        "no": g("JNO"),
        "url": None,
        "_deleted": False,
    }


def _is_deleted(obj: Dict[str, Any]) -> bool:
    for k in ("IS_DELETED", "isDeleted", "REMOVE", "deleted", "IS_PUBLIC"):
        v = obj.get(k)
        if v is None:
            continue
        if k in ("IS_PUBLIC",):
            # non-public -> withhold
            if str(v).lower() in ("false", "0", "n", "no"):
                return True
        elif str(v).lower() in ("true", "1", "y", "yes"):
            return True
    return False


def _norm_jdate(value) -> Optional[str]:
    if not value:
        return None
    s = re.sub(r"\D", "", str(value))
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", str(value))
    return m.group(0) if m else None


def _clean(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ── HTTP clients ──────────────────────────────────────────────────────

class OpenDataClient:
    """opendata.judicial.gov.tw: MemberTokens auth + catalog + downloads."""

    def __init__(self, account: Optional[str] = None, password: Optional[str] = None,
                 timeout: int = 120):
        self.account = account if account is not None else os.environ.get(ENV_ACCOUNT)
        self.password = password if password is not None else os.environ.get(ENV_PASSWORD)
        self.timeout = timeout
        self._token: Optional[str] = None

    def _request(self, url, method="GET", body=None, authed=False,
                 accept="application/json"):
        headers = {"User-Agent": USER_AGENT, "Accept": accept}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if authed:
            headers["Authorization"] = f"Bearer {self._ensure_token()}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status, resp.read()

    def _ensure_token(self) -> str:
        if self._token:
            return self._token
        if not self.account or not self.password:
            raise MissingCredentials(
                f"Set {ENV_ACCOUNT} and {ENV_PASSWORD} to authenticate to the "
                "Judicial Yuan open data platform (opendata.judicial.gov.tw)."
            )
        try:
            _, raw = self._request(
                MEMBER_TOKENS, method="POST",
                body={"memberAccount": self.account, "pwd": self.password},
                authed=False,
            )
        except urllib.error.HTTPError as e:
            raise TWAuthError(f"MemberTokens returned HTTP {e.code}") from None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise TWAuthError("Unparseable MemberTokens response") from None
        if isinstance(payload, dict) and payload.get("succeeded") is False:
            raise TWAuthError(f"MemberTokens login failed: {payload.get('message')}")
        token = _extract_token(payload)
        if not token:
            raise TWAuthError("No token in MemberTokens response")
        self._token = token
        return token

    def ensure_auth(self) -> None:
        self._ensure_token()

    def iter_dataset_page(self, page: int) -> Dict[str, Any]:
        url = f"{DATASETS}?{urllib.parse.urlencode({'keyword': JUDGMENT_KEYWORD, 'page': page})}"
        _, raw = self._request(url, method="GET", authed=False)
        return json.loads(raw.decode("utf-8"))

    def list_monthly_archives(self) -> List[Dict[str, Any]]:
        """Return newest-per-month monthly judgment archive datasets."""
        collected: List[Dict[str, Any]] = []
        page = 1
        while True:
            payload = self.iter_dataset_page(page)
            pl = payload.get("pagedList", {})
            items = pl.get("items") or []
            for it in items:
                title = it.get("title") or ""
                if not _MONTH_TITLE_RE.match(title):
                    continue
                fs = it.get("filesetLists") or []
                if not fs:
                    continue
                collected.append({
                    "datasetId": it.get("datasetId"),
                    "title": title,
                    "updateTime": it.get("updateTime"),
                    "modifiedDate": it.get("modifiedDate"),
                    "fileSetId": fs[0].get("fileSetId"),
                    "resourceFormat": fs[0].get("resourceFormat"),
                })
            if not pl.get("hasNextPage"):
                break
            page += 1
            time.sleep(1)
        return select_newest_per_month(collected)

    def download_fileset(self, file_set_id) -> bytes:
        url = FILE_DOWNLOAD.format(fileSetId=file_set_id)
        _, raw = self._request(url, method="GET", authed=True,
                               accept="application/octet-stream")
        return raw


class JudgmentApiClient:
    """data.judicial.gov.tw /jdg: nightly delta + full-text API."""

    def __init__(self, account: Optional[str] = None, password: Optional[str] = None,
                 timeout: int = 60):
        self.account = account if account is not None else os.environ.get(ENV_ACCOUNT)
        self.password = password if password is not None else os.environ.get(ENV_PASSWORD)
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_at: Optional[datetime] = None

    def _request(self, url, method="GET", body=None, params=None):
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self._token and method == "GET":
            headers["Authorization"] = f"Bearer {self._token}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def authenticate(self) -> None:
        if not self.account or not self.password:
            raise MissingCredentials(
                f"Set {ENV_ACCOUNT} and {ENV_PASSWORD} for the delta API."
            )
        payload = self._request(
            JDG_AUTH, method="POST",
            body={"user": self.account, "password": self.password},
        )
        if isinstance(payload, dict) and payload.get("error"):
            if "服務時間" in str(payload["error"]):
                raise ServiceWindowClosed(payload["error"])
            raise TWAuthError(str(payload["error"]))
        token = _extract_token(payload)
        if not token:
            raise TWAuthError("No token in JDG Auth response")
        self._token = token
        self._token_at = datetime.now(timezone.utc)

    def _valid_token(self) -> str:
        expired = (self._token_at is None or
                   datetime.now(timezone.utc) - self._token_at > timedelta(hours=5, minutes=30))
        if not self._token or expired:
            self.authenticate()
        return self._token

    def jlist(self) -> List[Dict[str, Any]]:
        self._valid_token()
        payload = self._request(JDG_LIST, method="GET")
        if isinstance(payload, dict):
            if payload.get("error"):
                raise ServiceWindowClosed(str(payload["error"]))
            return payload.get("list") or payload.get("data") or payload.get("JLIST") or []
        return payload or []

    def jdoc(self, jid: str) -> Optional[Dict[str, Any]]:
        self._valid_token()
        payload = self._request(JDG_DOC, method="GET", params={"j": jid})
        if isinstance(payload, dict) and payload.get("error"):
            return None
        return payload


# ── RAR extraction (lazy; archives contain judgment JSON files) ───────

def iter_judgments_from_rar(rar_bytes: bytes) -> Generator[Dict[str, Any], None, None]:
    """Yield judgment JSON objects from a monthly RAR archive.

    Uses `rarfile` (needs an `unrar`/`bsdtar`/`unar` backend). Each member is
    either a single JSON object or a JSON array / JSON-lines of judgments.
    """
    try:
        import rarfile  # noqa: PLC0415
    except ImportError as e:
        raise RuntimeError(
            "rarfile is required to extract monthly judgment archives "
            "(pip install rarfile; needs unrar/bsdtar/unar on PATH)."
        ) from e

    with rarfile.RarFile(io.BytesIO(rar_bytes)) as rf:
        for name in rf.namelist():
            if not name.lower().endswith((".json", ".txt")):
                continue
            try:
                raw = rf.read(name)
            except Exception as e:  # noqa: BLE001
                logger.debug("Skip %s: %s", name, e)
                continue
            yield from _iter_json_objects(raw)


def _iter_json_objects(raw: bytes) -> Generator[Dict[str, Any], None, None]:
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            yield from (o for o in obj if isinstance(o, dict))
        elif isinstance(obj, dict):
            yield obj
        return
    except ValueError:
        pass
    # JSON-lines fallback
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            o = json.loads(line)
            if isinstance(o, dict):
                yield o
        except ValueError:
            continue


class TWJudicialYuanODScraper(BaseScraper):
    """Scraper for TW/JudicialYuanOD (Judicial Yuan open judgment data)."""

    def __init__(self, opendata: Optional[OpenDataClient] = None,
                 jdg: Optional[JudgmentApiClient] = None):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.opendata = opendata or OpenDataClient()
        self.jdg = jdg or JudgmentApiClient()

    # -- historical archives ---------------------------------------------

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        archives = self.opendata.list_monthly_archives()
        logger.info("Discovered %d monthly judgment archives", len(archives))
        for ds in archives:
            fsid = ds.get("fileSetId")
            if not fsid:
                continue
            logger.info("Downloading %s (fileSetId=%s)", ds.get("title"), fsid)
            try:
                blob = self.opendata.download_fileset(fsid)
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                logger.warning("Download failed for %s: %s", ds.get("title"), e)
                continue
            for obj in iter_judgments_from_rar(blob):
                raw = parse_judgment_obj(obj)
                if raw and not raw.get("_deleted"):
                    yield raw
            time.sleep(1)

    # -- nightly delta ----------------------------------------------------

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        if not in_service_window():
            raise ServiceWindowClosed(
                "Delta API only answers 00:00-06:00 Asia/Taipei; skipping update."
            )
        for entry in self.jdg.jlist():
            jid = entry.get("JID") or entry.get("jid") or entry.get("id") \
                if isinstance(entry, dict) else entry
            if not jid:
                continue
            doc = self.jdg.jdoc(str(jid))
            if doc is None:
                continue
            raw = parse_judgment_obj(doc)
            if raw:
                yield raw
            time.sleep(1)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if raw.get("_deleted"):
            return None  # deletion/non-public signal: withhold
        if not raw.get("text"):
            return None
        return {
            "_id": raw["jid"],
            "_source": "TW/JudicialYuanOD",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or raw["jid"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url"),
            "jid": raw["jid"],
            "court": raw.get("court"),
            "year": raw.get("year"),
            "no": raw.get("no"),
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="TW/JudicialYuanOD data fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"],
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TWJudicialYuanODScraper()

    if args.command == "test":
        try:
            scraper.opendata.ensure_auth()
        except MissingCredentials as e:
            logger.error(str(e))
            sys.exit(2)
        except TWAuthError as e:
            logger.error("Auth failed: %s", e)
            sys.exit(1)
        archives = scraper.opendata.list_monthly_archives()
        logger.info("Auth OK; %d monthly archives discovered.", len(archives))
        if archives:
            logger.info("Newest month: %s", archives[-1]["title"])

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info("Bootstrap-fast complete: %s", json.dumps(stats, indent=2))

    elif args.command == "update":
        try:
            stats = scraper.update()
        except ServiceWindowClosed as e:
            logger.warning(str(e))
            sys.exit(0)
        logger.info("Update complete: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
