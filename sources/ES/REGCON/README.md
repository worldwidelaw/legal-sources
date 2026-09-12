# ES/REGCON — Convenios colectivos (Spanish collective bargaining agreements)

Full text of every collective bargaining agreement that the **Dirección General
de Trabajo** registers in **REGCON** (Registro y depósito de convenios y
acuerdos colectivos de trabajo) and publishes in the *Boletín Oficial del
Estado*, together with its revisions, wage tables, amendments, extensions,
adhesions, arbitration awards and minutes.

Built from source request [#1624](https://github.com/ZachLaik/LegalDataHunter/issues/1624)
(upstream `worldwidelaw/legal-sources#251`).

## Access strategy

REGCON's own front end (`expinterweb.mites.gob.es/regcon/`) is a stateful JSF
consultation form that exposes registry *metadata* only — the authoritative
**text** of every state-scope agreement is its BOE publication. So this source
uses the BOE Open Data API rather than scraping the registry:

| Step | Endpoint | Purpose |
|------|----------|---------|
| 1. Index | `GET /datosabiertos/api/boe/sumario/{YYYYMMDD}` | Daily summary, JSON |
| 2. Full text | `GET /diario_boe/xml.php?id={BOE-A-YYYY-NNNNN}` | Document XML with `<texto>` |

The index filter is an **exact epigraph match** on `Convenios colectivos de
trabajo` inside Section III. That distinguishes labour agreements from the
unrelated administrative `Convenios` (agreements between public bodies) that
are published under the same section.

Days are walked **newest first**, so an interrupted crawl or a sample carries
the most recent agreements rather than 1990's.

## Metadata derived

The consumer asked to identify, classify, query and track each agreement over
time. Beyond title/date/URL, each record carries:

- `convenio_code` — the REGCON code cited in the resolution's opening clause
  (`código de convenio n.º …`). Prefix `99` = state sectoral, `90` = state
  company, otherwise the INE province code.
- `scope_type` — `sector` | `empresa` | `unknown`, from the code prefix where
  authoritative, otherwise from the wording of the title.
- `instrument_type` — `agreement`, `revision`, `wage_tables`, `amendment`,
  `extension`, `adhesion`, `arbitration_award`, `minutes`, `denunciation`.
- `territorial_scope` (and `province` when the code is not state-scope).
- `materias` — BOE subject descriptors (sector and geography tags, e.g.
  *Hostelería*, *Transportes aéreos*, *Córdoba*).
- `validity_notes` — BOE `<analisis>` notes recording vigencia and prórroga
  (e.g. *"Vigencia hasta el 31 de diciembre de 2030. Prorrogable"*).
- `publication_reference` / `boe_issue` — official publication reference.

## Coverage

- **In scope:** state-scope register (BOE), 1990-01-01 to present
  (`coverage.start_date` in `config.yaml`). Earlier editions exist only as
  scanned PDFs.
- **Out of scope:** province- and region-scope agreements, which publish in the
  *boletines provinciales* and *autonómicos* — see `ES/ProvincialGazettes` and
  the per-region `ES/*` sources.

## Updates

`fetch_updates(since)` re-walks BOE summaries from `since` minus a 7-day
overlap. BOE publication date is the date a registration became available to
us, which is the correct availability comparator; the upsert dedups the
overlap. This is a genuine incremental — not a disguised full re-crawl.

## Sample

15 records in `sample/`, 3,390–370,963 characters of full text each, no HTML
residue, ISO 8601 dates.

## Usage

```bash
python bootstrap.py test                        # connectivity + one full-text fetch
python bootstrap.py bootstrap --sample          # 12 validation records
python bootstrap.py bootstrap                   # full crawl
python bootstrap.py update                      # incremental since last run
```

## License

[Spanish PSI reuse regulations — Ley 37/2007 and RD 1495/2011](https://datos.gob.es/en/terms)
— BOE content is reusable, **including for commercial purposes**, provided the
source and the date of last update are cited and the content is not altered or
presented as official.
