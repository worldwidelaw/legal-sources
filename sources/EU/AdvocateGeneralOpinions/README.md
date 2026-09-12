# EU/AdvocateGeneralOpinions — Opinions of the Advocates General (CJEU)

Full-text **Opinions of the Advocates General** of the Court of Justice of the
European Union, retrieved from the EU Publications Office repository (CELLAR /
EUR-Lex).

Before the Court of Justice rules on a case, an **Advocate General** delivers a
reasoned, impartial **Opinion** ("conclusions") proposing a legal solution.
Advocate General Opinions are among the most influential and heavily cited legal
writings in the EU order: they analyse the applicable Treaty and secondary law in
depth, survey the Court's prior case-law, and frequently shape the judgment that
follows. They are official documents of the Court, registered with a CELEX number
of the form `6{YYYY}CC{NNNN}` (sector 6 = case-law of the EU courts; `CC` = the
"View / Opinion of the Advocate General" document class).

## Scope

- **~11,300 Advocate General Opinions**, 1954 to the present.
- `data_type`: **doctrine** (reasoned advisory legal analysis; not a binding
  decision).

## Distinct from EU/CURIA

EU/CURIA enumerates the Court's case-law by the `JUDG` resource-type only
(judgments); its own docstring excludes Advocate General views and orders because
they behave differently in CELLAR. It therefore never pulls Advocate General
Opinions. This source is fully additive; the pipeline loader dedups on the CELEX
`_id` if any overlap ever arose.

## How it works

1. **Enumerate** every Opinion via the public **CELLAR SPARQL endpoint**. Because
   the corpus (~11,300 rows) exceeds the ~10,000-row SPARQL OFFSET ceiling of the
   Publications Office Virtuoso endpoint, enumeration is **scoped per calendar
   year** (`^6{YYYY}CC[0-9]`), swept newest-first; each year holds only a few
   hundred opinions.
2. **Fetch full text** from CELLAR via HTTP content negotiation:
   - `application/xhtml+xml` (OJ/Formex) for modern Opinions,
   - `text/html` on the bare CELEX (OJ HTML) for older ones,
   - born-digital **PDF** stream (PyMuPDF) for the remainder.

   All are served by CELLAR, which bypasses the eur-lex.europa.eu AWS-WAF that
   202-challenges datacenter IPs, so it is **fleet-safe**.
3. **Normalize** to the standard schema. Metadata-only stubs that resolve to no
   usable manifestation are skipped by a minimum-length guard.

## Fields

| Field   | Description                                        |
|---------|----------------------------------------------------|
| `_id`   | CELEX number (e.g. `62019CC0311`)                  |
| `_type` | `doctrine`                                         |
| `title` | English expression title (fallback: generic label)|
| `text`  | Full clean text of the Opinion                     |
| `date`  | Document date (ISO 8601, may be null)              |
| `url`   | EUR-Lex permalink for the CELEX                    |

## Usage

```bash
python3 bootstrap.py test              # probe SPARQL + one full-text fetch
python3 bootstrap.py bootstrap --sample # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes; attribution to the source is requested.
