# US/OK-OCC — Oklahoma Corporation Commission — Orders

Full-text **Orders** (Final / Interim / Emergency) issued by the **Oklahoma
Corporation Commission (OCC)** — the state agency that adjudicates oil & gas
conservation dockets (pooling, spacing, unitization, increased density,
location exceptions, multiunit horizontal wells), public-utility rate &
certificate cases, and transportation matters. Each Commission Order is an
administrative adjudication of a specific cause = **case_law**.

## Source

Laserfiche **WebLink 11** public repository operated by the OCC Court Clerk:

- Repository: `public.occ.ok.gov/WebLink` (repo=`OCC`, dbid=`0`)
- Order corpus: the `ECF Document` Laserfiche template, filtered by the
  `ECF Document Type` metadata field to `Final Order`, `Interim Order`,
  `Emergency Order`.

## How it works

1. **Session** — `GET Browse.aspx?dbid=0&repo=OCC` then `GET CookieCheck.aspx`
   with a cookie jar to obtain the `WebLinkSession` cookie.
2. **Enumerate** — POST `SearchService.aspx/GetSearchListing` with
   `searchSyn = {[ECF Document]:[ECF Document Type]="Final Order"} & {LF:Name="*.pdf"}`
   (and the two other order types). `data.hitCount` gives the total; results
   carry `entryId` / `name` / `isEdoc`.
3. **Metadata** — POST `DocumentService.aspx/GetBasicDocumentInfo`
   (ECF Case Number, Division, Case Type, Docket Date, Document Type).
4. **Full text** — download the born-digital PDF from the classic edoc URL
   `/WebLink/0/edoc/{entryId}/{filename}` and extract text with **PyMuPDF**
   (`fitz`). The one-line OCC Court-Clerk filing stamp on each page is stripped;
   records with < 600 body characters (image-only scans) are dropped.

### Coverage limit (important)

Only **born-digital order PDFs** carry an extractable text layer (~76 Final +
~72 Interim orders, ≈100 with substantial text). The far larger corpus of
**historical scanned-image orders** (~25K) is stored by Laserfiche with **no
OCR text layer** — `GetTextHtmlForPage` returns empty for those — so they are
not captured here (capturing them would require OCR). The staff
`Order Processing-*` templates are permission-denied to the anonymous public.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one normalize
python bootstrap.py bootstrap --sample  # ~12 sample orders
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # high-throughput full pull (VPS)
```

Requires `PyMuPDF` (`fitz`).

## License

[Public Domain — U.S. government edict](https://www.law.cornell.edu/uscode/text/17/105) — Orders of the Oklahoma Corporation Commission are edicts of a U.S. state government body and are not subject to copyright (government edicts doctrine). Freely reusable, including commercially. No attribution required.
