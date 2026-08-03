# CA/CNSC — Canadian Nuclear Safety Commission: Records of Decision

The Canadian Nuclear Safety Commission (CNSC) is Canada's independent,
quasi-judicial federal nuclear regulator and tribunal. Its **Records of
Decision** set out the Commission's reasons on licensing hearings,
confidentiality rulings and other adjudicative matters — these are `case_law`.

## Data access

Enumeration uses the CNSC **"Search hearing documents"** index
(`/eng/the-commission/hearings-meetings/search-hearing-documents/`). The page is
a Gatsby build whose complete document table is pre-rendered into the HTML. Each
row carries `[date, reference, hearing-type, applicant, facility, document-type,
<a>title</a>]`; rows whose document-type column is `Decision` are Records of
Decision. The linked PDFs are served by `api.cnsc-ccsn.gc.ca/dms/digital-medias/`
and full text is extracted with the shared `common.pdf_extract` backend.

- ~541 deduped Records of Decision, 2006–present, bilingual EN + FR.
- No authentication, no CAPTCHA.

## Usage

```bash
python bootstrap.py test                # verify listing + one PDF download
python bootstrap.py bootstrap --sample  # fetch 15 sample records
python bootstrap.py bootstrap           # full run
```

## License

[Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada) — Crown Copyright, Government of Canada. Commercial use permitted; attribution required.
