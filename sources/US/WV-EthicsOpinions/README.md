# US/WV-EthicsOpinions — West Virginia Ethics Commission, Advisory Opinions

Full text of the advisory opinions of the **West Virginia Ethics Commission**,
the independent state body created in 1989 that administers the West Virginia
Governmental Ethics Act (W. Va. Code Chapter 6B), the Open Governmental Meetings
Act (Chapter 6, Article 9A) and the codes of conduct it oversees.

Each opinion is the Commission's authoritative written interpretation of the
relevant statute, issued in response to a request and published with the
requester's identity kept confidential — **doctrine**.

## Series covered

| Prefix  | Series                                        |
|---------|-----------------------------------------------|
| `AO`    | Ethics Act Advisory Opinions (1989–present)   |
| `OMAO`  | Open Governmental Meetings Advisory Opinions  |
| `ALJAO` | Administrative Law Judge Advisory Opinions    |
| `SBAO`  | School Board Advisory Opinions                |

## Access

`ethics.wv.gov` is a Drupal site — no JavaScript, no CAPTCHA, no auth.

- Ethics Act opinions live on one page per year, `/{YYYY}-advisory-opinions-ao`,
  discovered from the master index
  `/opinions-and-exemptions/information-opinions/advisory-opinions-ao-1989-present`.
- The OMAO, ALJAO and SBAO series each live on a single listing page.
- On every page each opinion is an `<a>` whose text begins with the opinion
  number (`AO 2022-19`) and whose `href` is the born-digital PDF
  `/media/{id}/download?inline`.

Full text is extracted from the PDF via `common.pdf_extract`
(opendataloader → pdfplumber → pypdf → OCR fallback). Opinions are dedup'd by
opinion number; the issue date is parsed from the PDF body (`Issued on Month
DD, YYYY`), falling back to Jan 1 of the number's year.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — advisory opinions of the West Virginia Ethics Commission are official public records of a West Virginia state agency interpreting statute (government-edict works), published for public use under the WV Governmental Ethics Act (W. Va. Code Ch. 6B). Commercial use permitted; no attribution required.
