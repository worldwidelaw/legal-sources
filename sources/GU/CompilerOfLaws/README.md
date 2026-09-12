# GU/CompilerOfLaws — Guam Code Annotated & Administrative Rules

**Source:** [https://col.guamcourts.gov/](https://col.guamcourts.gov/)
**Data types:** legislation

The Compiler of Laws (Judiciary of Guam) publishes the consolidated law of Guam.
It used to live under `guamcourts.gov/CompilerofLaws/`; that path was retired when
the Judiciary moved to Drupal and now 404s (issue #1335). The corpus is served from
the `col.guamcourts.gov` subdomain.

## Coverage

| Collection | Index page | Documents |
|---|---|---|
| Organic Act of Guam (note) | `/guam-code-annotated/guam-code-annotated` | 1 |
| Guam Code Annotated (GCA) — 22 titles | `/guam-code-annotated/guam-code-annotated` | 825 |
| Guam Administrative Rules & Regulations (GAR) — 32 titles | `/guam-administrative-rules-regulations/guam-administrative-rules-and-regulations` | 243 |

Each index page is a single accordion: one panel per title, each listing chapter
links to PDFs under `/sites/default/files/`. Tables of contents are skipped;
division-level PDFs are kept because several divisions (all of GCA Title 13, the
UCC, and a few GAR titles) publish their text at division level with no chapter
children. Full text is extracted with `pdfplumber`.

`date` is the edition currency date parsed from the "Updated through P.L. …" line
on the GCA index page — the code is a consolidation, so every chapter shares it.

## Document IDs

- GCA: `GU-GCA-T{title:02d}-CH{chapter}` (e.g. `GU-GCA-T07-CH003`), matching the IDs
  used before the site move so re-ingests dedupe cleanly. Decimal and part-split
  chapters get suffixes (`GU-GCA-T07-CH009-5`, `GU-GCA-T10-CH012-P1`).
- GAR: `GU-GAR-T{title:02d}-{FILENAME}` — GAR chapter numbers restart inside every
  division, so the PDF file name is the only collision-free key.

## Usage

```bash
python3 bootstrap.py test-api              # connectivity + discovery counts
python3 bootstrap.py bootstrap --sample    # 20 records strided across the corpus
python3 bootstrap.py bootstrap --full      # full corpus -> data/records.jsonl
python3 bootstrap.py bootstrap-fast --full # alias used by the fleet wrapper
```

## License

Public domain — edicts of government. Guam is a U.S. territory; its statutes and
administrative rules are not subject to copyright.
[17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
see the [Compiler of Laws disclaimer](https://col.guamcourts.gov/disclaimer).
