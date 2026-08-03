# US/OR-TaxCourt — Oregon Tax Court (Regular + Magistrate Divisions)

Full text of every decision, opinion and order of the **Oregon Tax
Court**, the specialised state court with exclusive jurisdiction over
Oregon tax appeals (personal income, corporation excise, property,
timber, etc.). Disputes are between taxpayers and the Oregon Department
of Revenue or a county assessor, so the corpus is **case_law**.

The corpus is the complete digitised run of the *Oregon Tax Reports*
(OTR) plus current Regular- and Magistrate-Division decisions — back to
volume 1 and updated as new decisions issue (~8,260 documents).

## Source & access

The Oregon Judicial Department publishes the opinions in a public
**CONTENTdm** digital collection (alias `p17027coll6`) hosted at
`ojd.contentdm.oclc.org`. The CONTENTdm web-services API is open (no
JavaScript, no CAPTCHA, no auth):

- **`dmQuery`** — enumerate every item pointer in the collection.
- **`dmGetItemInfo`** — per-item metadata **plus the full opinion text in
  the `transc` (transcript) field**, so no PDF download or OCR is needed.
  Each record also carries the official case name, citation
  (`NN OTR NNN`), decision date (ISO), judge, case number and division.

If an item's `transc` field is empty, the scraper falls back to
downloading the item PDF
(`/digital/api/collection/p17027coll6/id/{pointer}/download`) and
extracting its text layer via `common.pdf_extract`.

The court's own decision pages
(`courts.oregon.gov/publications/tax/...`) link into the same CONTENTdm
collection via an `OJDRedirect` script, but those pages only surface the
most recent decisions; querying the collection directly yields the full
historical corpus.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample decisions
python bootstrap.py bootstrap           # full pull (~8,260 decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Oregon)](https://www.law.cornell.edu/uscode/text/17/105) — Oregon Tax Court decisions are official state government works. The CONTENTdm `rights` field records "No known copyright restrictions." Commercial use OK, no attribution required.
