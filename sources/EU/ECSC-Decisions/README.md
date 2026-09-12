# EU — ECSC General Decisions

Full-text **general (normative) decisions** adopted under the **European Coal and
Steel Community (ECSC) Treaty** (Treaty of Paris, in force 1952 – 23 July 2002),
harvested from the EU Publications Office (CELLAR / EUR-Lex).

## What this covers

In the EUR-Lex CELEX numbering scheme these acts sit in **sector 3**, document
class **`S`** (`3{YYYY}S{NNNN}`, e.g. `32002S1469` = Commission Decision
No 1469/2002/ECSC). The `S` descriptor gathers the ECSC **general decisions** —
adopted first by the ECSC **High Authority** and, after the 1967 Merger Treaty, by
the **Commission** acting as ECSC-Treaty law-maker.

Unlike ordinary EEC/EC decisions (descriptor `D`, addressed to specific parties),
the ECSC general decisions were the ECSC's equivalent of **regulations** — normative
acts of general application governing the common market in coal and steel:

- levies on coal and steel production,
- State-aid rules for the coal and steel industries,
- production quotas and price/publication rules,
- external-trade and anti-dumping measures on steel imports,
- and their amendments, extensions and repeals.

The corpus runs from the 1950s until the ECSC Treaty expired on 23 July 2002
(~590 acts).

> ⚠️ **Not to be confused** with `INTL/ECSC` and the many `XX/ECSC` sources, which
> refer to the **Eastern Caribbean Supreme Court**. This source is the **European
> Coal and Steel Community**.

## Why it is additive to EU/EUR-Lex

`EU/EUR-Lex` enumerates sector 3 by the ordinary **binding resource-types only**
(REG / DIR / DEC and their implementing/delegated variants) and therefore never
pulls the `S` ECSC general decisions, which carry the dedicated ECSC-decision
resource type (not DEC). `S` is a distinct CELEX letter from every other descriptor
(D, E, F, H, …), so there is no `_id` collision. This is the CELLAR sibling of
`EU/CFSP-Acts` (descriptor `E`), `EU/FrameworkDecisions` (`F`) and
`EU/RulesOfProcedure` (`Q`).

## How it works

1. **Enumerate** every act via the public CELLAR SPARQL endpoint (CELEX matching
   `^3[0-9]{4}S[0-9]`). ~590 rows, far under the SPARQL OFFSET ceiling → a single
   paged `LIMIT`/`OFFSET` sweep.
2. **Fetch full text** from CELLAR via HTTP content negotiation on
   `/resource/celex/{CELEX}`:
   - OJ/Formex **xHTML** for modern acts,
   - **OJ HTML** via the language-suffixed CELEX (`.ENG`) for older ones,
   - born-digital **PDF** (PyMuPDF) for the remainder.

   All are served by CELLAR, which bypasses the `eur-lex.europa.eu` AWS-WAF that
   202-challenges datacenter IPs, so the scraper is fleet-safe.
3. **Normalize** to the standard schema (`_type: legislation`).

Corrigenda (`…R(01)`) and metadata-only stubs resolve to no usable manifestation and
are skipped by the 300-character minimum-length guard.

## Usage

```bash
# quick connectivity + full-text probe
python3 sources/EU/ECSC-Decisions/bootstrap.py test

# write 15 validation samples
python3 sources/EU/ECSC-Decisions/bootstrap.py bootstrap --sample

# full corpus (streams to data/records.jsonl)
python3 sources/EU/ECSC-Decisions/bootstrap.py bootstrap-fast
```

Requires `requests` and `PyMuPDF` (`fitz`) for the PDF fallback.

## License

[EU institutional reuse — Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — ECSC general decisions published in the Official Journal by the Publications Office are reusable, including for commercial purposes, under the Commission's reuse policy. Attribution to the source is requested.
