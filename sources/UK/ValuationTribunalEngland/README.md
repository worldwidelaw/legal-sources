# UK — Valuation Tribunal for England (VTE) — Decisions

Full-text decisions of the **Valuation Tribunal for England (VTE)**, the
independent statutory tribunal that determines appeals about **council tax**
(valuation/banding, liability, completion notices, penalties, council tax
reduction) and **non-domestic rating** (business rates — rateable value
challenges, completion notices, penalties, transitional certificates).

VTE is constituted under the Local Government Finance Act 1988 and the Valuation
Tribunal for England (Council Tax and Rating Appeals) (Procedure) Regulations
2009, and is administered by the Valuation Tribunal Service (a Crown/arm's-length
public body). Each determination is a full, reasoned written decision; many carry
a neutral citation of the form `[YYYY] VTE {ref}`.

- **Type:** case_law
- **Jurisdiction:** England (GB-ENG)
- **Coverage:** ~15,600 decided appeals — CD (Council tax, ~11,656) + ND
  (Non-domestic rating, ~3,967). Older appeals with no published written
  decision return HTTP 404 and are skipped.
- **Full text:** yes — born-digital PDFs (text layer), extracted with
  `common.pdf_extract` (no OCR).

## Source

- Appeal & decisions search: <https://appealsearch.valuationtribunal.gov.uk/>
- Decided-appeals view (paginated, per appeal-type family):
  `GET /Home/Decisions?AppealSearchType={CD|ND}&SearchByType=advanced&Skip={n}&Page={p}&PageSize={sz}&SortOn=Date&SortDesc=True&HearingId=00000000-0000-0000-0000-000000000000`
- Decision document:
  `GET /Home/Download?ApAppealNumber={id}` → decision PDF.
  The trailing 2 characters of `ApAppealNumber` are render-time decoration; the
  stable identifier is the appeal number shown as the row link text
  (e.g. `VT00034997`, `CHG100095546`). Download is cookieless.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — attribution required (acknowledge as "Valuation Tribunal Service" copyright). Commercial use permitted.

VTE's [copyright page](https://valuationtribunal.gov.uk/copyright/) explicitly
grants re-use of website material free of charge in any format under the OGL v3.0
(logos/photographs/video excluded — not relevant to text-only decisions).
