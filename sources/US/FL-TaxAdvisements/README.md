# US/FL-TaxAdvisements — Florida DOR Technical Assistance Advisements

Full text of the **Technical Assistance Advisements (TAAs)** issued by the
Florida Department of Revenue's Office of Technical Assistance under
s. 213.22, F.S. and Rule 12-11, F.A.C. — binding written determinations on
the taxability of specific transactions or the applicability of a tax to a
specific set of facts. Published versions are public records with
confidential taxpayer information redacted.

TAAs are official state-government interpretive guidance (not adjudications
of a contested case), so the corpus is classified as `doctrine`.

## Source

- Tax Law Library: <https://floridarevenue.com/taxes/taxlaw>
- Search REST API: `https://floridarevenue.com/TaxLaw/_api/search/query`
- Document PDFs: `https://floridarevenue.com/TaxLaw/Documents/TAA <ID>.pdf`

The Tax Law Library is a SharePoint site. TAAs are enumerated through its
**anonymously-readable** SharePoint Search REST API with the KQL query
`path:.../TaxLaw/Documents/ IsDocument:1 Filename:TAA*` (≈2,860 TAAs),
paged via `rowlimit`/`startrow`. Each result's `Path` property is the
public PDF URL (filenames contain a space, URL-encoded before download);
`Title` is the advisement subject. Full text lives only in the PDF and is
extracted via the shared `common.pdf_extract` helper (pdfplumber → pypdf →
OCR fallback). A `<200`-char guard skips retracted/empty stubs.

A small minority of older TAAs are stored under a bare-ID filename
(e.g. `14C1-010.pdf`) without the `TAA ` prefix; these are not captured by
`Filename:TAA*` and represent a minor completeness gap.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction smoke test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~2,860 TAAs)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Technical Assistance Advisements of the Florida Department of Revenue are
official state-government works in the public domain under the
government-edicts doctrine, published as public records per s. 213.22, F.S.
Commercial use permitted; no attribution required.
