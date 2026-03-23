#!/usr/bin/env python3
"""
German Federal Office for Consumer Protection (BVL) - Plant Protection Products Fetcher

Official REST API: https://psm-api.bvl.bund.de/
API Documentation: OpenAPI/Swagger standard

This fetcher retrieves plant protection product (pesticide) authorization data:
- Approved products with approval dates
- Active ingredients and concentrations
- Approved applications (crops, pests, conditions)
- Regulatory requirements and conditions
- GHS hazard and safety information

Full text is constructed by combining all regulatory information for each product.
Data is public domain official government works under German law (§ 5 UrhG).
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API Configuration
BASE_URL = "https://psm-api.bvl.bund.de/ords/psm/api-v1"
BATCH_SIZE = 500


class BVLFetcher:
    """Fetcher for BVL Plant Protection Product authorization data"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json',
        })
        # Caches for lookups
        self._kode_cache: Dict[str, Dict[str, str]] = {}
        self._wirkstoff_cache: Dict[str, str] = {}

    def _fetch_json(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """Fetch all items from a paginated API endpoint"""
        url = f"{BASE_URL}{endpoint}"
        all_items = []
        offset = 0

        while True:
            query_params = params.copy() if params else {}
            query_params['limit'] = BATCH_SIZE
            query_params['offset'] = offset

            try:
                response = self.session.get(url, params=query_params, timeout=60)
                response.raise_for_status()
                data = response.json()

                items = data.get('items', [])
                all_items.extend(items)

                if not data.get('hasMore', False):
                    break

                offset += BATCH_SIZE
                time.sleep(0.3)

            except requests.RequestException as e:
                logger.error(f"Error fetching {url}: {e}")
                break

        return all_items

    def _fetch_single_page(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """Fetch a single page from API endpoint"""
        url = f"{BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            return data.get('items', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return []

    def _load_kode_cache(self) -> None:
        """Load code definitions for decoding"""
        if self._kode_cache:
            return

        logger.info("Loading code definitions...")
        kodes = self._fetch_json('/kode/', {'sprache': 'DE'})

        for kode in kodes:
            kodeliste = str(kode.get('kodeliste', ''))
            code = kode.get('kode', '')
            text = kode.get('kodetext', '')

            if kodeliste not in self._kode_cache:
                self._kode_cache[kodeliste] = {}
            self._kode_cache[kodeliste][code] = text

        logger.info(f"Loaded {len(kodes)} code definitions across {len(self._kode_cache)} code lists")

    def _load_wirkstoff_cache(self) -> None:
        """Load active ingredient names"""
        if self._wirkstoff_cache:
            return

        logger.info("Loading active ingredients...")
        wirkstoffe = self._fetch_json('/wirkstoff/')

        for ws in wirkstoffe:
            wirknr = ws.get('wirknr', '')
            name = ws.get('wirkstoffname', '')
            self._wirkstoff_cache[wirknr] = name

        logger.info(f"Loaded {len(self._wirkstoff_cache)} active ingredients")

    def _decode_kode(self, kodeliste: str, code: str) -> str:
        """Decode a code to its text description"""
        if not self._kode_cache:
            self._load_kode_cache()
        return self._kode_cache.get(str(kodeliste), {}).get(code, code)

    def _get_wirkstoff_name(self, wirknr: str) -> str:
        """Get active ingredient name by number"""
        if not self._wirkstoff_cache:
            self._load_wirkstoff_cache()
        return self._wirkstoff_cache.get(wirknr, wirknr)

    def _fetch_product_details(self, kennr: str) -> Dict[str, Any]:
        """Fetch all details for a specific product"""
        details = {}

        # Fetch active ingredient concentrations
        wirkstoff_gehalt = self._fetch_single_page('/wirkstoff_gehalt/', {'kennr': kennr})
        details['wirkstoffe'] = []
        for wg in wirkstoff_gehalt:
            ws_name = self._get_wirkstoff_name(wg.get('wirknr', ''))
            details['wirkstoffe'].append({
                'name': ws_name,
                'gehalt_rein': wg.get('gehalt_rein'),
                'gehalt_einheit': wg.get('gehalt_einheit', ''),
            })

        # Fetch approved applications
        awg = self._fetch_single_page('/awg/', {'kennr': kennr})
        details['anwendungen'] = []
        for a in awg:
            anwendung = {
                'awg_id': a.get('awg_id', ''),
                'anwendungsbereich': self._decode_kode('13', a.get('anwendungsbereich', '')),
                'einsatzgebiet': self._decode_kode('12', a.get('einsatzgebiet', '')),
                'wirkungsbereich': self._decode_kode('21', a.get('wirkungsbereich', '')),
                'anwendungen_max_je_vegetation': a.get('anwendungen_max_je_vegetation'),
                'anwenderkategorie': a.get('anwenderkategorie', ''),
            }
            details['anwendungen'].append(anwendung)

        # Fetch requirements/conditions
        auflagen = self._fetch_single_page('/auflagen/', {'ebene': kennr})
        details['auflagen'] = []
        seen_auflagen = set()
        for auf in auflagen:
            auflage_code = auf.get('auflage', '')
            if auflage_code and auflage_code not in seen_auflagen:
                seen_auflagen.add(auflage_code)
                # Decode based on the auflage prefix
                # NT = Nature conservation, NW = Water protection, etc.
                details['auflagen'].append({
                    'code': auflage_code,
                    'text': self._decode_kode('74', auflage_code) or auflage_code,
                })

        # Fetch GHS hazard information
        ghs_hazards = self._fetch_single_page('/ghs_gefahrenhinweise/', {'kennr': kennr})
        details['gefahrenhinweise'] = []
        for gh in ghs_hazards:
            code = gh.get('gefahrenhinweis', '')
            details['gefahrenhinweise'].append({
                'code': code,
                'text': self._decode_kode('70', code) or code,
            })

        # Fetch GHS safety information
        ghs_safety = self._fetch_single_page('/ghs_sicherheitshinweise/', {'kennr': kennr})
        details['sicherheitshinweise'] = []
        for gs in ghs_safety:
            code = gs.get('sicherheitshinweis', '')
            details['sicherheitshinweise'].append({
                'code': code,
                'text': self._decode_kode('71', code) or code,
            })

        return details

    def _build_full_text(self, product: Dict, details: Dict) -> str:
        """Build comprehensive text from product data and details"""
        parts = []

        # Header
        mittelname = product.get('mittelname', '')
        kennr = product.get('kennr', '')
        parts.append(f"PFLANZENSCHUTZMITTEL-ZULASSUNG / PLANT PROTECTION PRODUCT AUTHORIZATION")
        parts.append(f"{'='*70}")
        parts.append(f"")
        parts.append(f"Produktname / Product Name: {mittelname}")
        parts.append(f"Zulassungsnummer / Authorization Number: {kennr}")

        # Dates
        zul_erstmalig = product.get('zul_erstmalig_am', '')
        zul_ende = product.get('zul_ende', '')
        if zul_erstmalig:
            parts.append(f"Erstmalige Zulassung / First Authorization: {zul_erstmalig[:10]}")
        if zul_ende:
            parts.append(f"Zulassungsende / Authorization Expires: {zul_ende[:10]}")

        # Formulation
        formulierung = product.get('formulierung_art', '')
        if formulierung:
            parts.append(f"Formulierungsart / Formulation Type: {formulierung}")

        parts.append("")

        # Active ingredients
        wirkstoffe = details.get('wirkstoffe', [])
        if wirkstoffe:
            parts.append("WIRKSTOFFE / ACTIVE INGREDIENTS")
            parts.append("-" * 40)
            for ws in wirkstoffe:
                name = ws.get('name', '')
                gehalt = ws.get('gehalt_rein', '')
                einheit = ws.get('gehalt_einheit', '')
                if gehalt:
                    parts.append(f"  • {name}: {gehalt} {einheit}")
                else:
                    parts.append(f"  • {name}")
            parts.append("")

        # Approved applications
        anwendungen = details.get('anwendungen', [])
        if anwendungen:
            parts.append("ZUGELASSENE ANWENDUNGEN / APPROVED APPLICATIONS")
            parts.append("-" * 40)
            for i, anw in enumerate(anwendungen[:20], 1):  # Limit to 20 applications
                parts.append(f"  Anwendung {i}:")
                if anw.get('anwendungsbereich'):
                    parts.append(f"    Anwendungsbereich: {anw['anwendungsbereich']}")
                if anw.get('einsatzgebiet'):
                    parts.append(f"    Einsatzgebiet: {anw['einsatzgebiet']}")
                if anw.get('wirkungsbereich'):
                    parts.append(f"    Wirkungsbereich: {anw['wirkungsbereich']}")
                if anw.get('anwendungen_max_je_vegetation'):
                    parts.append(f"    Max. Anwendungen/Vegetation: {anw['anwendungen_max_je_vegetation']}")
                if anw.get('anwenderkategorie'):
                    parts.append(f"    Anwenderkategorie: {anw['anwenderkategorie']}")
            if len(anwendungen) > 20:
                parts.append(f"  ... und {len(anwendungen) - 20} weitere Anwendungen")
            parts.append("")

        # Requirements and conditions
        auflagen = details.get('auflagen', [])
        if auflagen:
            parts.append("AUFLAGEN UND BEDINGUNGEN / REQUIREMENTS AND CONDITIONS")
            parts.append("-" * 40)
            for auf in auflagen:
                code = auf.get('code', '')
                text = auf.get('text', '')
                if text and text != code:
                    parts.append(f"  [{code}] {text}")
                else:
                    parts.append(f"  [{code}]")
            parts.append("")

        # Hazard information
        gefahren = details.get('gefahrenhinweise', [])
        if gefahren:
            parts.append("GEFAHRENHINWEISE / HAZARD STATEMENTS (GHS)")
            parts.append("-" * 40)
            for gh in gefahren:
                code = gh.get('code', '')
                text = gh.get('text', '')
                if text and text != code:
                    parts.append(f"  [{code}] {text}")
                else:
                    parts.append(f"  [{code}]")
            parts.append("")

        # Safety information
        sicherheit = details.get('sicherheitshinweise', [])
        if sicherheit:
            parts.append("SICHERHEITSHINWEISE / PRECAUTIONARY STATEMENTS (GHS)")
            parts.append("-" * 40)
            for sh in sicherheit:
                code = sh.get('code', '')
                text = sh.get('text', '')
                if text and text != code:
                    parts.append(f"  [{code}] {text}")
                else:
                    parts.append(f"  [{code}]")
            parts.append("")

        # Footer
        parts.append("-" * 70)
        parts.append("Quelle / Source: Bundesamt für Verbraucherschutz und Lebensmittelsicherheit (BVL)")
        parts.append("API: https://psm-api.bvl.bund.de/")

        return "\n".join(parts)

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all plant protection product authorizations with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full regulatory text
        """
        # Preload caches
        self._load_kode_cache()
        self._load_wirkstoff_cache()

        logger.info("Fetching all approved products...")
        products = self._fetch_json('/mittel/')
        logger.info(f"Found {len(products)} approved products")

        count = 0
        for product in products:
            if limit and count >= limit:
                logger.info(f"Reached limit of {limit} documents")
                return

            kennr = product.get('kennr', '')
            mittelname = product.get('mittelname', '')

            logger.info(f"[{count + 1}] Fetching details for {kennr}: {mittelname}")

            # Fetch all related data
            details = self._fetch_product_details(kennr)

            # Build full text
            full_text = self._build_full_text(product, details)

            doc = {
                'kennr': kennr,
                'mittelname': mittelname,
                'versuchsbez': product.get('versuchsbez', ''),
                'formulierung_art': product.get('formulierung_art', ''),
                'zul_erstmalig_am': product.get('zul_erstmalig_am', ''),
                'zul_ende': product.get('zul_ende', ''),
                'mittel_mit_geringem_risiko': product.get('mittel_mit_geringem_risiko'),
                'wirkstoffe': details.get('wirkstoffe', []),
                'anwendungen': details.get('anwendungen', []),
                'auflagen': details.get('auflagen', []),
                'gefahrenhinweise': details.get('gefahrenhinweise', []),
                'sicherheitshinweise': details.get('sicherheitshinweise', []),
                'text': full_text,
            }

            yield doc
            count += 1

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Fetched {count} products with full regulatory text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent products (currently fetches recent approvals)"""
        # The API doesn't have date filtering, so we fetch all and filter
        # For now, return most recent products
        yield from self.fetch_all(limit=50)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        kennr = raw_doc.get('kennr', '')
        mittelname = raw_doc.get('mittelname', '')

        # Parse dates
        zul_erstmalig = raw_doc.get('zul_erstmalig_am', '')
        if zul_erstmalig:
            zul_erstmalig = zul_erstmalig[:10]  # YYYY-MM-DD

        zul_ende = raw_doc.get('zul_ende', '')
        if zul_ende:
            zul_ende = zul_ende[:10]

        # Build URL
        url = f"https://psm-api.bvl.bund.de/ords/psm/api-v1/mittel/?kennr={kennr}"

        return {
            '_id': f"bvl_psm_{kennr.replace('-', '_')}",
            '_source': 'DE/BVL',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': f"{mittelname} ({kennr}) - Pflanzenschutzmittel-Zulassung",
            'text': raw_doc.get('text', ''),
            'date': zul_erstmalig,
            'expiry_date': zul_ende,
            'url': url,
            'kennr': kennr,
            'mittelname': mittelname,
            'formulierung_art': raw_doc.get('formulierung_art', ''),
            'wirkstoffe_count': len(raw_doc.get('wirkstoffe', [])),
            'anwendungen_count': len(raw_doc.get('anwendungen', [])),
            'auflagen_count': len(raw_doc.get('auflagen', [])),
            'authority': 'Bundesamt für Verbraucherschutz und Lebensmittelsicherheit (BVL)',
            'language': 'de',
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BVLFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 15 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 5):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(':', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['title'][:60]}... ({text_len:,} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode
        fetcher = BVLFetcher()
        print("Testing BVL PSM-API fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:100]}")
            print(f"Date: {normalized['date']}")
            print(f"Expiry: {normalized['expiry_date']}")
            print(f"Wirkstoffe: {normalized['wirkstoffe_count']}")
            print(f"Anwendungen: {normalized['anwendungen_count']}")
            print(f"Auflagen: {normalized['auflagen_count']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview:\n{normalized.get('text', '')[:800]}...")
            count += 1


if __name__ == '__main__':
    main()
