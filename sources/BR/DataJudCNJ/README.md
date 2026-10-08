# BR/DataJudCNJ — CNJ DataJud Public API (all Brazilian courts)

**Source:** https://api-publica.datajud.cnj.jus.br
**Documentation:** https://datajud-wiki.cnj.jus.br/api-publica/acesso/
**Country:** BR
**Data types:** case_law
**Status:** Working (previously marked Blocked — see "Answering the block reason")

## What this source is

DataJud is the National Judicial Database maintained by Brazil's National
Council of Justice (CNJ), created by Resolução CNJ 331/2020. Every Brazilian
court is legally required to feed it. CNJ exposes a public read API over it,
governed by Portaria CNJ 160/2020, covering the metadata of public judicial
processes.

One source unlocks **90 courts**: 4 superior courts, 6 federal regional courts,
27 state courts, 24 labor courts, 27 electoral courts and 3 military courts.

## Answering the block reason

The previous README marked this source blocked as `no_full_text_access`:
"API provides only case metadata with no decision text field. Pure
case-tracking system."

**That assessment is factually correct.** DataJud carries no decision text and
this collector does not pretend otherwise. The question is whether that makes
it out of scope, and the case for including it is:

1. **It is the only open route into courts that are otherwise at zero.** TJMG
   is marked blocked in this repository for CAPTCHA and F5 WAF. So are TJSP,
   TJRJ, TJRS, TJSC and others. For those jurisdictions the catalogue currently
   offers nothing at all. Procedural metadata is not equivalent to a judgment,
   but it is not nothing, and it is what exists.

2. **Procedural history answers questions full text cannot.** Whether a case is
   final, when it was filed, which body holds it, what subject it was coded
   under, whether an appeal was admitted. Those are facts about the law in
   operation, retrievable and citable to an official source.

3. **It makes `resolve_reference` work for Brazilian case numbers.** The CNJ
   unified number is the canonical Brazilian citation key. Without a corpus
   carrying those numbers, a citation like `9053000-90.2013.8.13.0024` resolves
   to nothing.

If the maintainers still consider metadata out of scope, the honest outcome is
to close this and leave the block note in place, corrected to say the source is
technically reachable and excluded by policy rather than blocked. The
distinction matters for anyone reading the catalogue to decide where to spend
effort.

## What it provides

Per process: the unified CNJ case number (rendered in the official punctuated
form of Resolução CNJ 65/2008), procedural class and legal subjects coded
against the CNJ national tables, judging body, court and instance, filing date,
last-update timestamp, and the complete dated procedural history, each step
with its code, date and tabulated complements.

## What it does NOT provide

**No decision text.** A record tells you that a case exists, what it is about,
where it sits and everything that has happened in it, never what a judge wrote.
Anyone needing the reasoning must go to the originating court.

## Access and authentication

The API key is the **public key published by CNJ** at
https://datajud-wiki.cnj.jus.br/api-publica/acesso/. It is not a private
credential and no registration is required. CNJ states it may be rotated at any
time, so the collector reads `DATAJUD_API_KEY` from the environment first and
falls back to the published value.

```
Authorization: APIKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==
```

The endpoint is an Elasticsearch `_search` interface, one index per court:
`https://api-publica.datajud.cnj.jus.br/api_publica_{alias}/_search`.

No CAPTCHA, no WAF challenge, no browser automation.

## Verified behaviour

Everything below was executed against the live API on **2026-07-28**.

**All 91 published aliases were called individually.** 89 answered HTTP 200 on
the first pass; `tjmt` returned HTTP 429 and answered normally on retry, so the
real figure is **90 working of 91**.

**One documentation error found upstream at CNJ.** The wiki publishes
`tre-dft` for the Federal District electoral court. That alias returns HTTP
404. The working alias is `tre-df` (5.907 records). Corrected in `config.yaml`.

**One court is genuinely empty.** `trt21` answers correctly with 0 records.
Kept in the list so it starts producing when CNJ backfills it.

**Deep pagination.** `from`/`size` is unusable: Elasticsearch caps it at 10.000
hits and every court exceeds that. The collector uses `search_after` sorted on
`@timestamp`, verified walking past the first page.

**Incremental updates.** `fetch_updates` filters on `dataHoraUltimaAtualizacao`,
verified returning results for a date range.

**Upstream double encoding.** Some courts serve text that was decoded as
Latin-1 before being re-encoded, so `PRESIDÊNCIA` arrives as
`PRESIDÃ\x8aNCIA`. This is present in the raw HTTP response, not introduced
here. `_fix_double_encoded()` repairs it conservatively: the re-encode/decode
round trip only succeeds when the bytes really are valid UTF-8, so correctly
encoded Portuguese such as `SÃO PAULO` and `ÓRGÃO` is returned untouched.

**Field naming trap in `complementosTabelados`.** The fields are the reverse of
what their names suggest: `nome` carries the human-readable value (`Certidão`)
and `descricao` carries the category label (`tipo_de_documento`). Rendered as
`categoria: valor` so neither half is lost.

**End-to-end proof.** `bootstrap --sample` produced 10 normalized records from
STJ with 0 errors and 0 skips. The same code path was then run against
`tjmg`, the court this repository lists as blocked, returning real cases with
correct accents, coded subjects and up to 125 procedural movements each.

## Privacy

Records with `nivelSigilo > 0` are dropped in `normalize()`. Brazilian law
restricts processes under judicial secrecy, and this collector will not
republish them even when the API returns them.

## Usage

```bash
python bootstrap.py test                       # connectivity check
python bootstrap.py bootstrap --sample         # 10 sample records
python bootstrap.py bootstrap                  # full pull, all 90 courts
python bootstrap.py update                     # incremental, by last-update date
```

To collect a single court, reduce the `tribunals` list in `config.yaml`.

## Rate limiting

`config.yaml` sets 1 request per second with a burst of 3, deliberately
conservative. DataJud rate limits rather than denying: the collector honours
`Retry-After`, records the throttle with the shared rate limiter and retries up
to four times before skipping the court loudly.

## Note for maintainers: a separate blocking bug

`common/__init__.py` imports `from .pdf_extract import extract_pdf_markdown,
preload_existing_ids`, but `common/pdf_extract.py` is absent from the published
repository (HTTP 404 on raw.githubusercontent, and not listed in `.gitignore`).
Every other module under `common/` is present. The effect is that
`from common.base_scraper import BaseScraper` raises `ModuleNotFoundError`, so
**no collector runs from a clean clone**. Reported separately.

## License

Public data published by CNJ under Resolução CNJ 331/2020 and Portaria CNJ
160/2020. Open government data (https://dados.gov.br).
