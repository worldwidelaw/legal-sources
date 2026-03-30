#!/usr/bin/env python3
"""
PE/ElPeruano -- Peru Official Gazette Legislation (via SPIJ API)

Fetches Peru legislation from the SPIJ (Sistema Peruano de Información Jurídica)
public REST API operated by the Ministry of Justice.

Strategy:
  - Authenticate with public credentials (embedded in the SPIJ Angular frontend)
  - Enumerate norm IDs (H1 to ~H1,400,000) and fetch full text
  - Filter out paywall/placeholder entries (225 chars = subscription notice)
  - Full text as HTML in textoCompleto field, cleaned to plain text
  - Covers laws, decrees, resolutions from 1946 to present

Source: https://spij.minjus.gob.pe (Ministry of Justice, Peru)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (enumerates all IDs)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.ElPeruano")

API_BASE = "https://spijwsii.minjus.gob.pe/spij-ext-back"
AUTH_PAYLOAD = {"usuario": "spijext", "clave": "password", "tipo": 0}

# Approximate max ID as of 2026
MAX_NORM_ID = 1_400_000


class ElPeruanoScraper(BaseScraper):
    """
    Scraper for PE/ElPeruano -- Peru Official Gazette via SPIJ.
    Country: PE
    URL: https://spij.minjus.gob.pe

    Data types: legislation
    Auth: none (public credentials)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )
        self._token = None
        self._token_time = 0

    def _get_token(self) -> str:
        """Get or refresh JWT token."""
        now = time.time()
        if self._token and (now - self._token_time) < 80000:
            return self._token

        try:
            resp = self.client.post(
                f"{API_BASE}/authenticate",
                json_data=AUTH_PAYLOAD,
                timeout=30,
            )
            if resp and resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    self._token = data["value"]
                    self._token_time = now
                    # Set auth header on session for subsequent requests
                    self.client.session.headers["Authorization"] = f"Bearer {self._token}"
                    return self._token
        except Exception as e:
            logger.error(f"Auth failed: {e}")

        raise RuntimeError("Failed to authenticate with SPIJ API")

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace from SPIJ HTML content."""
        if not text:
            return ""
        # Remove style/script blocks
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Replace block-level elements with newlines
        text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
        # Remove remaining tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode entities
        text = html.unescape(text)
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _clean_sumilla(self, sumilla: str) -> str:
        """Clean HTML from sumilla (summary/title) field."""
        if not sumilla:
            return ""
        text = re.sub(r'<[^>]+>', ' ', sumilla)
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _fetch_norm(self, norm_id: str) -> Optional[dict]:
        """Fetch a single norm by ID."""
        token = self._get_token()
        self.rate_limiter.wait()

        try:
            resp = self.client.get(
                f"{API_BASE}/api/detallenorma/{norm_id}",
                timeout=30,
            )
            if resp is None or resp.status_code != 200:
                return None

            data = resp.json()
            full_html = data.get("textoCompleto", "")

            # Skip paywall/placeholder (225 chars = subscription notice)
            if not full_html or len(full_html) <= 300:
                return None

            # Skip subscription notices
            if "suscriptores del SPIJ" in full_html:
                return None

            text = self._clean_html(full_html)
            if not text or len(text) < 50:
                return None

            title = self._clean_sumilla(data.get("sumilla", ""))
            if not title:
                title = self._clean_sumilla(data.get("titulo", ""))

            return {
                "id": data.get("id", norm_id),
                "title": title,
                "text": text,
                "norm_type": data.get("dispositivoLegal", ""),
                "norm_code": data.get("codigoNorma", ""),
                "date": data.get("fechaPublicacion", ""),
                "sector": data.get("sector", ""),
                "ruta": data.get("ruta", ""),
            }

        except Exception as e:
            logger.debug(f"Error fetching {norm_id}: {e}")
            return None

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all norms by enumerating IDs from newest to oldest."""
        total_yielded = 0
        consecutive_empty = 0

        for i in range(MAX_NORM_ID, 0, -1):
            norm_id = f"H{i}"
            norm = self._fetch_norm(norm_id)

            if norm:
                total_yielded += 1
                consecutive_empty = 0
                if total_yielded % 100 == 0:
                    logger.info(f"Progress: {total_yielded} norms fetched (at {norm_id})")
                yield norm
            else:
                consecutive_empty += 1

            # If we hit 1000 consecutive empty IDs, we've gone past the data
            if consecutive_empty > 1000:
                logger.info(f"Stopping: {consecutive_empty} consecutive empty IDs at {norm_id}")
                break

        logger.info(f"Fetch complete: {total_yielded} norms")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent norms (newest IDs first, stop when date is before since)."""
        since_str = since.strftime("%Y-%m-%d")

        for i in range(MAX_NORM_ID, 0, -1):
            norm_id = f"H{i}"
            norm = self._fetch_norm(norm_id)

            if norm:
                if norm.get("date", "") and norm["date"] < since_str:
                    break
                yield norm

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample norms from recent IDs."""
        found = 0
        consecutive_empty = 0

        for i in range(MAX_NORM_ID, 0, -1):
            if found >= count:
                break

            norm_id = f"H{i}"
            norm = self._fetch_norm(norm_id)

            if norm:
                found += 1
                consecutive_empty = 0
                title = norm.get("title", "N/A")[:60]
                text_len = len(norm.get("text", ""))
                logger.info(f"Sample {found}/{count}: [{norm.get('norm_type', '?')}] {title} ({text_len} chars)")
                yield norm
            else:
                consecutive_empty += 1

            if consecutive_empty > 200:
                logger.warning(f"Too many empty IDs, stopping at H{i}")
                break

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw norm record to standard schema."""
        norm_id = raw.get("id", "unknown")

        date_raw = raw.get("date", "")
        date_iso = None
        if date_raw and len(date_raw) >= 10:
            date_iso = date_raw[:10]

        web_url = f"https://spij.minjus.gob.pe/spij-ext-web/#/detallenorma/{norm_id}"

        return {
            "_id": f"PE-SPIJ-{norm_id}",
            "_source": "PE/ElPeruano",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": web_url,
            "norm_type": raw.get("norm_type", ""),
            "norm_code": raw.get("norm_code", ""),
            "sector": raw.get("sector", ""),
            "language": "es",
        }

    def test_api(self) -> bool:
        """Test connectivity to the SPIJ API."""
        logger.info("Testing SPIJ API access...")

        try:
            token = self._get_token()
            logger.info(f"Auth: OK (token length {len(token)})")
        except Exception as e:
            logger.error(f"Auth failed: {e}")
            return False

        # Test a known norm (Constitution of Peru)
        norm = self._fetch_norm("H682678")
        if not norm:
            # Try another known good ID
            norm = self._fetch_norm("H1288461")

        if not norm:
            logger.error("Could not fetch any known norm")
            return False

        text_len = len(norm.get("text", ""))
        logger.info(f"Norm: {norm.get('title', 'N/A')[:60]}")
        logger.info(f"Type: {norm.get('norm_type', 'N/A')}")
        logger.info(f"Text length: {text_len} chars")

        if text_len < 100:
            logger.error("Text too short")
            return False

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = ElPeruanoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Running full fetch (newest first)")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
