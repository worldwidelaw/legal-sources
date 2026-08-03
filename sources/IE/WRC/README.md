# IE/WRC — Workplace Relations Commission (Adjudication & Labour Court Decisions)

Full text of Ireland's employment- and equality-law decisions published by the
**Workplace Relations Commission (WRC)**:

- **Adjudication Officer decisions** (`ADJ-*`) under the Workplace Relations Act
  2015 and the employment/equality Acts it administers (Unfair Dismissals,
  Payment of Wages, Employment Equality, Equal Status, Terms of Employment, etc.).
- **Industrial Relations recommendations** (`IR-*`).
- **Labour Court determinations** (`PWD*`, `TED*`, `DWT*`, `UDD*`, …) that appear in
  the WRC decisions database on appeal.
- **Legacy Equality Tribunal decisions** (`DEC-E*`, `DEC-S*`, `DEC-P*`).

## Data access

Decisions are server-rendered HTML pages, enumerable via the decisions search
listing (10 per page, newest first, ~3,200 pages ≈ ~32,000 full-text decisions):

```
https://www.workplacerelations.ie/en/search/?query=&decisions=1&pageNumber={N}
```

Each result links to an individual decision page:

```
https://www.workplacerelations.ie/en/cases/{YYYY}/{month}/{ref}.html
```

The scraper walks the listing, fetches each decision page, extracts the main
`<div class="content">` body (trimming site chrome), and parses the reference,
the primary Act, and the decision date (`Dated:` for WRC; the signed numeric date
for Labour Court determinations).

**Note:** the host multiplexes poorly over HTTP/2 (stream `CANCEL` errors), so the
fetcher forces HTTP/1.1 with retries. Search pages are ~800 KB and can take ~10 s.

## Usage

```bash
python3 bootstrap.py bootstrap --sample     # 12 sample records -> sample/
python3 bootstrap.py bootstrap              # full corpus -> data/records.jsonl
python3 bootstrap.py bootstrap-fast         # fleet alias for full bootstrap
python3 bootstrap.py updates --since 2026-01-01
```

## License

[PSI Licence / CC BY 4.0](https://www.gov.ie/en/help/re-use-of-public-sector-information/) — Irish public sector information; attribution required, commercial use permitted. WRC and Labour Court decisions are public quasi-judicial edicts.
