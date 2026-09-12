# EC/SRI-Tax — Ecuador Servicio de Rentas Internas Tax Guidance

**Source:** [https://www.sri.gob.ec/](https://www.sri.gob.ec/)
**Index:** [Extractos de consultas](https://www.sri.gob.ec/extractos-de-consultas)
**Data types:** doctrine

## What this collects

SRI publishes the extracts of its formal tax rulings (*Extractos de las
Absoluciones de las Consultas Tributarias*) as one PDF compilation per year,
plus the *Normativa Institucional Vigente*. The scraper:

1. Discovers the compilation URLs from the live index page rather than a frozen
   list, so a newly published year is picked up automatically. A curated list of
   known documents is merged in as a floor — the Normativa lives off a different
   page, and a page redesign must not silently shrink the corpus.
2. Unpacks year archives served as `.zip` (2025 onward).
3. Splits each compilation into **one record per ruling**, anchored on the
   ruling's own `Oficio:` line, with the signing date taken from the line above
   it (bare in the 2014–2018 layout, `Fecha:`-prefixed from 2019 on). A document
   whose layout does not split is emitted whole rather than dropped.

Each record carries the consultante, the *Referencia* subject line, the question
put to SRI, the legal basis, and SRI's answer.

## Usage

```bash
python bootstrap.py bootstrap            # 15 sample records, spread across years
python bootstrap.py bootstrap --full     # full corpus to data/records.jsonl
python bootstrap.py bootstrap-fast       # full corpus, concurrent normalize
python bootstrap.py update --since 2025-01-01
python bootstrap.py test-api
```

## Notes

- Language: Spanish. No authentication; no rate-limit problems observed.
- The compilations are re-read in full on every run (`force=True`): the
  skip-if-already-in-Neon guard is keyed on the compilation, not the individual
  ruling, so honouring it made a full bootstrap emit nothing at all (issue
  #1577). Per-ruling dedup happens at ingest.
- Oficio numbers repeat across a handful of rulings; `_id` falls back to the
  ruling's position within its compilation to stay unique.

## License

Open government data — [https://www.sri.gob.ec/](https://www.sri.gob.ec/)
