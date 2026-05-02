#!/usr/bin/env python3
"""
TR/SPK -- Turkish Capital Markets Board Administrative Sanctions

Fetches administrative fines and trading bans from SPK's official REST API.

Strategy:
  - GET /IdariYaptirimlar/api/TumIdariParaCezalari → all fines (JSON array)
  - GET /IdariYaptirimlar/api/IslemYasaklari → all trading bans (JSON array)
  - Both endpoints return complete structured data — no pagination needed
  - No auth required; public Swagger-documented API at ws.spk.gov.tr/help

URL patterns:
  - API base: https://ws.spk.gov.tr
  - Swagger: https://ws.spk.gov.tr/help/index.html
  - Frontend: https://idariyaptirimlar.spk.gov.tr/

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TR.SPK")

API_BASE = "https://ws.spk.gov.tr"
FRONTEND_BASE = "https://idariyaptirimlar.spk.gov.tr"

FINES_ENDPOINT = "/IdariYaptirimlar/api/TumIdariParaCezalari"
BANS_ENDPOINT = "/IdariYaptirimlar/api/IslemYasaklari"


def build_fine_text(record: dict) -> str:
    """Build full text from administrative fine record fields."""
    parts = []

    unvan = record.get("unvan", "").strip()
    if unvan:
        parts.append(f"Muhatap (Respondent): {unvan}")

    karar = record.get("kurulKarari", "").strip()
    if karar:
        parts.append(f"Kurul Kararı (Board Decision): {karar}")

    aciklama = record.get("aciklama", "").strip()
    if aciklama:
        parts.append(f"Açıklama (Description): {aciklama}")

    ihlal = record.get("ihlal", "").strip()
    if ihlal:
        parts.append(f"İhlal Edilen Düzenleme (Violated Regulation): {ihlal}")

    yasa = record.get("yasa")
    if yasa:
        parts.append(f"Yasa (Law): {yasa}")

    teblig = record.get("teblig")
    if teblig:
        parts.append(f"Tebliğ (Communiqué): {teblig}")

    madde = record.get("madde")
    if madde:
        parts.append(f"Madde (Article): {madde}")

    tutar = record.get("tutar")
    if tutar is not None:
        parts.append(f"Ceza Tutarı (Penalty Amount): {tutar:,.2f} TL")

    dava = record.get("davaBilgisi", "").strip()
    if dava:
        parts.append(f"Dava Bilgisi (Lawsuit Info): {dava}")

    yargilama = record.get("yargilamaAsamasi")
    if yargilama:
        parts.append(f"Yargılama Aşaması (Judicial Stage): {yargilama}")

    return "\n\n".join(parts)


def build_ban_text(record: dict) -> str:
    """Build full text from trading ban record fields."""
    parts = []

    unvan = record.get("unvan", "").strip()
    if unvan:
        parts.append(f"İşlem Yasaklı Kişi/Kuruluş (Banned Person/Entity): {unvan}")

    pay = record.get("pay", "").strip()
    if pay:
        parts.append(f"İlgili Şirket (Related Company): {pay}")

    pay_kodu = record.get("payKodu", "").strip()
    if pay_kodu:
        parts.append(f"Pay Kodu (Stock Code): {pay_kodu}")

    karar_no = record.get("kurulKararNo", "").strip()
    if karar_no:
        parts.append(f"Kurul Kararı (Board Decision): {karar_no}")

    tarih = record.get("kurulKararTarihi", "").strip()
    if tarih:
        parts.append(f"Karar Tarihi (Decision Date): {tarih[:10]}")

    return "\n\n".join(parts)


def parse_api_date(date_str: Optional[str]) -> Optional[str]:
    """Parse API date format '2026-04-22T00:00:00' to ISO date."""
    if not date_str:
        return None
    try:
        return date_str[:10]
    except (IndexError, TypeError):
        return None


class SPKScraper(BaseScraper):
    """Scraper for TR/SPK -- Turkish Capital Markets Board sanctions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _fetch_fines(self) -> list:
        """Fetch all administrative fines from API."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(FINES_ENDPOINT)
            if not resp or resp.status_code != 200:
                logger.error(f"Fines API error: {resp.status_code if resp else 'no response'}")
                return []
            data = resp.json()
            logger.info(f"Fetched {len(data)} administrative fines")
            return data
        except Exception as e:
            logger.error(f"Error fetching fines: {e}")
            return []

    def _fetch_bans(self) -> list:
        """Fetch all trading bans from API."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(BANS_ENDPOINT)
            if not resp or resp.status_code != 200:
                logger.error(f"Bans API error: {resp.status_code if resp else 'no response'}")
                return []
            data = resp.json()
            logger.info(f"Fetched {len(data)} trading bans")
            return data
        except Exception as e:
            logger.error(f"Error fetching bans: {e}")
            return []

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all administrative sanctions (fines + trading bans)."""
        fines = self._fetch_fines()
        for f in fines:
            f["_record_type"] = "fine"
            yield f

        time.sleep(1)

        bans = self._fetch_bans()
        for b in bans:
            b["_record_type"] = "ban"
            yield b

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch records updated since a given date."""
        for raw in self.fetch_all():
            date_str = parse_api_date(raw.get("kurulKararTarihi"))
            if date_str:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if dt >= since:
                        yield raw
                except ValueError:
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw API record into standard schema."""
        record_type = raw.get("_record_type", "fine")

        if record_type == "fine":
            return self._normalize_fine(raw)
        elif record_type == "ban":
            return self._normalize_ban(raw)
        return None

    def _normalize_fine(self, raw: dict) -> Optional[dict]:
        """Normalize an administrative fine record."""
        rec_id = raw.get("id")
        unvan = raw.get("unvan", "").strip()
        karar_no = raw.get("kurulKararNo", "").strip()
        date = parse_api_date(raw.get("kurulKararTarihi"))

        if not rec_id or not unvan:
            return None

        doc_id = f"SPK-FINE-{rec_id}"
        tutar = raw.get("tutar")
        tutar_str = f" — {tutar:,.2f} TL" if tutar else ""
        title = f"İdari Para Cezası: {unvan}{tutar_str} ({karar_no})"

        text = build_fine_text(raw)
        if len(text) < 30:
            logger.warning(f"Insufficient text for {doc_id}: {len(text)} chars")
            return None

        return {
            "_id": doc_id,
            "_source": "TR/SPK",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": FRONTEND_BASE,
            "decision_number": karar_no,
            "respondent": unvan,
            "penalty_amount_tl": tutar,
            "violation": raw.get("ihlal", "").strip(),
            "lawsuit_info": raw.get("davaBilgisi", "").strip(),
            "judicial_stage": raw.get("yargilamaAsamasi"),
            "jurisdiction": "TR",
            "language": "tr",
            "record_subtype": "administrative_fine",
        }

    def _normalize_ban(self, raw: dict) -> Optional[dict]:
        """Normalize a trading ban record."""
        unvan = raw.get("unvan", "").strip()
        karar_no = raw.get("kurulKararNo", "").strip()
        date = parse_api_date(raw.get("kurulKararTarihi"))
        pay_kodu = raw.get("payKodu", "").strip()

        if not unvan or not karar_no:
            return None

        doc_id = f"SPK-BAN-{re.sub(r'[^a-zA-Z0-9_-]', '_', karar_no)}-{re.sub(r'[^a-zA-Z0-9_-]', '_', unvan[:30])}"
        title = f"İşlem Yasağı: {unvan} — {pay_kodu} ({karar_no})"

        text = build_ban_text(raw)
        if len(text) < 30:
            logger.warning(f"Insufficient text for {doc_id}: {len(text)} chars")
            return None

        return {
            "_id": doc_id,
            "_source": "TR/SPK",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": FRONTEND_BASE,
            "decision_number": karar_no,
            "respondent": unvan,
            "related_company": raw.get("pay", "").strip(),
            "stock_code": pay_kodu,
            "jurisdiction": "TR",
            "language": "tr",
            "record_subtype": "trading_ban",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing SPK API endpoints...")

        # Test fines
        fines = self._fetch_fines()
        print(f"\nAdministrative fines: {len(fines)} records")
        if fines:
            f = fines[0]
            print(f"  First: {f.get('unvan', '')[:60]} — {f.get('kurulKarari', '')}")
            print(f"  Amount: {f.get('tutar', 0):,.2f} TL")

        # Test bans
        bans = self._fetch_bans()
        print(f"\nTrading bans: {len(bans)} records")
        if bans:
            b = bans[0]
            print(f"  First: {b.get('unvan', '')[:60]} — {b.get('payKodu', '')}")

        print(f"\nTotal records: {len(fines) + len(bans)}")
        print("Test complete!")


def main():
    scraper = SPKScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
