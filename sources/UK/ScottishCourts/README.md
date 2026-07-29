# UK/ScottishCourts - Scottish Courts and Tribunals Service Judgments

**Source:** Scottish Courts and Tribunals Service
**URL:** https://www.scotcourts.gov.uk/judgments/
**Data types:** Case law
**Auth:** None
**License:** Open Government Licence 3.0 for Crown copyright material

## Overview

This source fetches Scottish court judgments from the official SCTS judgments
search. It covers the Scottish courts missing from the National Archives Find
Case Law source, including:

- Court of Session
- High Court of Justiciary
- Sheriff Appeal Court Civil
- Sheriff Appeal Court Criminal
- Sheriff Court Civil
- Sheriff Court Criminal
- National Personal Injury Court
- Scottish Upper Tribunal chambers

The public judgments page says judgments can be searched from 1999 onwards.

## Data Access

The public page embeds a web application backed by:

- `GET https://api.pa.web.scotcourts.gov.uk/web/definition/1414`
- `POST https://api.pa.web.scotcourts.gov.uk/web/search`

Search results include metadata and a relative PDF path. The scraper downloads
the PDF from `www.scotcourts.gov.uk` and extracts full text with the shared PDF
extractor.

Python requests and curl fail certificate chain/revocation checks for
`scotcourts.gov.uk` in this environment. The scraper disables TLS verification
for this source only so the public API and PDF endpoints can be reached.

## License

The SCTS Crown copyright policy states that Crown copyright information on the
site, excluding logos and photographs, may be reused under the Open Government
Licence. Third-party material is excluded.

## Usage

```bash
python bootstrap.py test-api
python bootstrap.py bootstrap --sample --sample-size 10
python bootstrap.py bootstrap
```
