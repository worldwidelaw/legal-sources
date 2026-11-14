#!/usr/bin/env python3
"""
BR/DataJudCNJ -- Brazilian National Council of Justice (CNJ) DataJud Public API

Collects public judicial process metadata from every Brazilian court through
the CNJ's official public API, established by Resolucao CNJ 331/2020 and
Portaria CNJ 160/2020.

WHAT THIS SOURCE PROVIDES
  Process-level metadata and the full procedural history (movimentos) for
  each case: case number (numero unico CNJ), procedural class, legal subjects
  (assuntos), judging body, court instance (grau), filing date, and the dated
  list of every procedural step, each coded against the CNJ national tables
  (Tabelas Processuais Unificadas).

WHAT THIS SOURCE DOES NOT PROVIDE
  It does NOT contain the full text of judgments. CNJ publishes metadata only.
  Anyone needing the text of a decision must go to the originating court.
  This is stated plainly so downstream users do not mistake procedural history
  for a decision on the merits.

WHY IT MATTERS
  Many Brazilian courts are unreachable for open collection: their
  jurisprudence portals sit behind CAPTCHA or WAF challenges. DataJud is the
  official, documented, machine-readable route that requires neither. A single
  source unlocks all 90+ Brazilian courts at once, including several that are
  otherwise entirely absent from the catalogue.

ACCESS
  The API key below is the public key published by CNJ for open access at
  https://datajud-wiki.cnj.jus.br/api-publica/acesso/. It is not a private
  credential. CNJ states the key may change at any time, so it can be
  overridden with the DATAJUD_API_KEY environment variable.

PRIVACY
  Records carrying nivelSigilo > 0 are skipped. Brazilian law restricts
  processes under judicial secrecy, and this collector will not republish
  them even when the API returns them.

Usage:
  python bootstrap.py bootstrap            # Full pull, every configured court
  python bootstrap.py bootstrap --sample   # Fetch 10 sample records
  python bootstrap.py update               # Incremental, by last-update date
"""

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.DataJudCNJ")

API_BASE = "https://api-publica.datajud.cnj.jus.br"

# Public key published by CNJ. Override with DATAJUD_API_KEY if CNJ rotates it.
PUBLIC_API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="

PAGE_SIZE = 100

# Court tier as used by the catalogue: 1 = supreme, 2 = appellate, 3 = first instance.
# DataJud exposes "grau" per record (G1, G2, JE, TR, SUP); the per-record value
# refines this default, which reflects the court as an institution.
SUPERIOR_COURTS = {"stj", "tst", "tse", "stm"}


def _parse_datajud_date(value: Any) -> Optional[str]:
    """Normalize the several date shapes DataJud returns to ISO 8601 (YYYY-MM-DD).

    Observed formats:
      "20260724104145"            -> compact timestamp, filing dates
      "2026-07-26T18:57:53.583Z"  -> ISO with milliseconds, update timestamps
      "2026-07-26"                -> already ISO
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) >= 14 and s[:14].isdigit():
        try:
            return datetime.strptime(s[:14], "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
        except ValueError:
            pass
    if len(s) >= 8 and s[:8].isdigit():
        try:
            return datetime.strptime(s[:8], "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _fix_double_encoded(text: Any) -> Any:
    """Repair text that DataJud serves double-encoded.

    Some courts push UTF-8 bytes that were already decoded as Latin-1 before
    being re-encoded, so "PRESIDENCIA" with a circumflex arrives as
    'PRESID\\xc3\\x83\\xc2\\x8aNCIA'. Verified against api_publica_stj on
    2026-07-28: the corruption is present in the raw HTTP response, not
    introduced here, so it has to be repaired rather than blamed downstream.

    The repair is deliberately conservative. Re-encoding to Latin-1 and
    decoding as UTF-8 only succeeds when the byte sequence really is valid
    UTF-8, which is the signature of double encoding. Correctly encoded
    Portuguese such as "SAO PAULO" with a tilde raises UnicodeDecodeError on
    the second step and is returned untouched.
    """
    if not isinstance(text, str) or not text:
        return text
    if "Ã" not in text and "Â" not in text:
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    if "�" in repaired:
        return text
    return repaired


def _format_process_number(numero: str) -> str:
    """Render the 20-digit CNJ case number in its official punctuated form.

    NNNNNNN-DD.AAAA.J.TR.OOOO, per Resolucao CNJ 65/2008. Returned unchanged
    when it is not exactly 20 digits, so malformed input is never silently
    reshaped into something that looks official.
    """
    n = "".join(ch for ch in str(numero or "") if ch.isdigit())
    if len(n) != 20:
        return str(numero or "")
    return "%s-%s.%s.%s.%s.%s" % (n[0:7], n[7:9], n[9:13], n[13:14], n[14:16], n[16:20])


class DataJudCNJScraper(BaseScraper):
    """Scraper for BR/DataJudCNJ -- CNJ DataJud public API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.api_key = os.environ.get("DATAJUD_API_KEY", PUBLIC_API_KEY)
        self.tribunals: List[str] = self.config.get("tribunals") or []
        if not self.tribunals:
            raise ValueError(
                "config.yaml must list at least one court alias under 'tribunals'"
            )

    # ── HTTP ──────────────────────────────────────────────────────

    def _post_search(self, alias: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST an Elasticsearch query to one court's endpoint.

        Retries on transient failures with linear backoff. Raises on a
        persistent failure so the caller can skip the court loudly instead of
        silently yielding an empty result set.
        """
        import requests

        url = "%s/api_publica_%s/_search" % (API_BASE, alias)
        headers = {
            "Authorization": "APIKey %s" % self.api_key,
            "Content-Type": "application/json",
        }
        last_error: Optional[Exception] = None
        for attempt in range(4):
            try:
                self.rate_limiter.wait()
                resp = requests.post(url, headers=headers, json=body, timeout=60)
                if resp.status_code == 429:
                    # DataJud rate limits rather than denying. Observed on tjmt
                    # during a 91-court sweep on 2026-07-28; it cleared on retry.
                    retry_after = resp.headers.get("Retry-After")
                    self.rate_limiter.record_429(
                        float(retry_after) if retry_after else None
                    )
                    wait = float(retry_after) if retry_after else 5 * (attempt + 1)
                    logger.warning("%s: rate limited, waiting %.0fs", alias, wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                payload = resp.json()
                if "error" in payload:
                    raise RuntimeError("API error: %s" % json.dumps(payload["error"])[:300])
                self.rate_limiter.record_success()
                return payload
            except Exception as exc:  # noqa: BLE001 - retried, then re-raised
                last_error = exc
                time.sleep(3 * (attempt + 1))
        raise RuntimeError("%s: failed after 4 attempts: %s" % (alias, last_error))

    def _scroll(
        self, alias: str, query: Dict[str, Any]
    ) -> Generator[Dict[str, Any], None, None]:
        """Page through one court exhaustively using search_after.

        from/size cannot be used here: Elasticsearch caps that at 10.000 hits,
        and every Brazilian court exceeds it. search_after sorted on
        @timestamp walks the whole index without that ceiling.
        """
        search_after: Optional[List[Any]] = None
        seen = 0
        while True:
            body: Dict[str, Any] = {
                "size": PAGE_SIZE,
                "query": query,
                "sort": [{"@timestamp": {"order": "asc"}}],
            }
            if search_after is not None:
                body["search_after"] = search_after

            payload = self._post_search(alias, body)
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return

            for hit in hits:
                source = hit.get("_source") or {}
                source["_tribunal_alias"] = alias
                yield source
                seen += 1

            sort_value = hits[-1].get("sort")
            if not sort_value:
                logger.warning("%s: no sort cursor returned, stopping at %d", alias, seen)
                return
            search_after = sort_value

            if seen % 5000 == 0:
                logger.info("%s: %d records so far", alias, seen)

    # ── Required interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every public process from every configured court."""
        for alias in self.tribunals:
            logger.info("Collecting court: %s", alias)
            try:
                count = 0
                for record in self._scroll(alias, {"match_all": {}}):
                    yield record
                    count += 1
                logger.info("%s: %d records collected", alias, count)
            except Exception as exc:  # noqa: BLE001 - one court must not kill the run
                logger.error("%s: aborted, %s", alias, exc)
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield processes updated since the given moment.

        Filters on dataHoraUltimaAtualizacao, which DataJud sets whenever a
        court pushes a new procedural step. Verified working against TJMG on
        2026-07-28.
        """
        since_iso = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        query = {"range": {"dataHoraUltimaAtualizacao": {"gte": since_iso}}}
        for alias in self.tribunals:
            logger.info("Incremental update for %s since %s", alias, since_iso)
            try:
                for record in self._scroll(alias, query):
                    yield record
            except Exception as exc:  # noqa: BLE001
                logger.error("%s: aborted, %s", alias, exc)
                continue

    def normalize(self, raw: dict) -> Optional[dict]:
        """Map one DataJud record to the catalogue's standard schema."""
        numero = str(raw.get("numeroProcesso") or "").strip()
        tribunal = str(raw.get("tribunal") or "").strip()
        alias = str(raw.get("_tribunal_alias") or "").strip()

        # Never republish a process under judicial secrecy.
        if int(raw.get("nivelSigilo") or 0) > 0:
            return None
        if not numero:
            return None

        classe = raw.get("classe") or {}
        classe_nome = _fix_double_encoded(str(classe.get("nome") or "").strip())
        classe_codigo = classe.get("codigo")

        assuntos = raw.get("assuntos") or []
        assunto_nomes = [
            _fix_double_encoded(str(a.get("nome")).strip())
            for a in assuntos
            if isinstance(a, dict) and a.get("nome")
        ]

        orgao = raw.get("orgaoJulgador") or {}
        orgao_nome = _fix_double_encoded(str(orgao.get("nome") or "").strip())

        grau = str(raw.get("grau") or "").strip()
        numero_formatado = _format_process_number(numero)

        movimentos = raw.get("movimentos") or []
        movimento_linhas: List[str] = []
        for mov in movimentos:
            if not isinstance(mov, dict):
                continue
            data_mov = _parse_datajud_date(mov.get("dataHora")) or ""
            nome_mov = _fix_double_encoded(str(mov.get("nome") or "").strip())
            if not nome_mov:
                continue
            # In complementosTabelados the fields are the reverse of what the
            # names suggest: "nome" carries the human-readable value ("certidao")
            # and "descricao" carries the category label ("tipo_de_documento").
            # Verified against api_publica_stj on 2026-07-28. Rendered as
            # "categoria: valor" so neither half is lost.
            extras: List[str] = []
            for comp in mov.get("complementosTabelados") or []:
                if not isinstance(comp, dict):
                    continue
                valor = _fix_double_encoded(str(comp.get("nome") or "").strip())
                categoria = _fix_double_encoded(str(comp.get("descricao") or "").strip())
                if valor and categoria:
                    extras.append("%s: %s" % (categoria, valor))
                elif valor or categoria:
                    extras.append(valor or categoria)
            linha = "%s %s" % (data_mov, nome_mov) if data_mov else nome_mov
            if extras:
                linha = "%s (%s)" % (linha, "; ".join(extras))
            movimento_linhas.append(linha)

        # The searchable body: identification followed by the dated procedural
        # history. This is metadata, not the text of any decision.
        blocos: List[str] = []
        if classe_nome:
            blocos.append("Classe processual: %s" % classe_nome)
        if assunto_nomes:
            blocos.append("Assuntos: %s" % "; ".join(assunto_nomes))
        if orgao_nome:
            blocos.append("Orgao julgador: %s" % orgao_nome)
        if grau:
            blocos.append("Grau: %s" % grau)
        if movimento_linhas:
            blocos.append("Movimentacoes processuais:\n%s" % "\n".join(movimento_linhas))
        text = "\n\n".join(blocos).strip()
        if not text:
            return None

        raw_id = str(raw.get("id") or "").strip()
        if raw_id:
            doc_id = "BR-DATAJUD-%s" % raw_id
        else:
            digest = hashlib.sha256(
                ("%s|%s|%s" % (tribunal, grau, numero)).encode("utf-8")
            ).hexdigest()[:16]
            doc_id = "BR-DATAJUD-%s" % digest

        title_parts = [p for p in (classe_nome, numero_formatado, tribunal) if p]
        title = " - ".join(title_parts)

        court_tier = 1 if alias in SUPERIOR_COURTS else (2 if grau == "G2" else 3)

        mapped = {
            "id",
            "numeroProcesso",
            "tribunal",
            "classe",
            "assuntos",
            "orgaoJulgador",
            "grau",
            "movimentos",
            "dataAjuizamento",
            "dataHoraUltimaAtualizacao",
            "nivelSigilo",
            "_tribunal_alias",
        }

        return {
            "_id": doc_id,
            "_source": "BR/DataJudCNJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": _parse_datajud_date(raw.get("dataAjuizamento")),
            "date_last_update": _parse_datajud_date(raw.get("dataHoraUltimaAtualizacao")),
            "case_number": numero_formatado,
            "case_number_raw": numero,
            "court": tribunal,
            "court_tier": court_tier,
            "chamber": orgao_nome,
            "jurisdiction": "Brazil",
            "language": "pt",
            "country": "BR",
            "decision_type": classe_nome,
            "classe_codigo": classe_codigo,
            "assuntos": assunto_nomes,
            "grau": grau,
            "movement_count": len(movimento_linhas),
            "url": "https://www.cnj.jus.br/sistemas/datajud/",
            "_raw_fields": {k: v for k, v in raw.items() if k not in mapped},
        }


# ── CLI Entry Point ───────────────────────────────────────────────


def main():
    scraper = DataJudCNJScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        sample_size = int(sys.argv[sys.argv.index("--sample-size") + 1])

    if command == "test":
        alias = scraper.tribunals[0]
        payload = scraper._post_search(alias, {"size": 1, "query": {"match_all": {}}})
        total = payload.get("hits", {}).get("total", {})
        print("Connectivity OK for api_publica_%s" % alias)
        print("Indexed processes: %s (%s)" % (total.get("value"), total.get("relation")))
        return

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print("\nSample complete: %s records saved to sample/"
                  % stats.get("sample_records_saved", 0))
        else:
            stats = scraper.bootstrap()
            print("\nBootstrap complete: %s new, %s updated, %s skipped"
                  % (stats["records_new"], stats["records_updated"], stats["records_skipped"]))
    elif command == "update":
        stats = scraper.update()
        print("\nUpdate complete: %s new, %s updated"
              % (stats["records_new"], stats["records_updated"]))
    else:
        print("Unknown command: %s" % command)
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
