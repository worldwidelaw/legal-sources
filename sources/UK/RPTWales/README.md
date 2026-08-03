# UK/RPTWales — Residential Property Tribunal Wales (Tribiwnlys Eiddo Preswyl)

Full-text decisions of the **Residential Property Tribunal Wales (RPTW)**, the
independent statutory tribunal for Wales that resolves private-rented and
leasehold residential-property disputes under the Housing Act 2004, Landlord and
Tenant Act 1985/1987, Commonhold and Leasehold Reform Act 2002, Rent Act 1977,
Leasehold Reform, Housing and Urban Development Act 1993 and related legislation.

RPTW written decisions (with statement of reasons) are binding and appealable to
the Upper Tribunal (Lands Chamber). They are **case law for the Wales (GB-WLS)
jurisdiction** and are **not** covered by `UK/CaseLaw` (the National Archives
*Find Case Law* service, which indexes England & Wales superior courts and
reserved UK tribunals only — RPTW is not on that service).

## Coverage

The tribunal sits in three panels, each with its own decision series
(April 2012–present):

| Case-type id | Panel | Slug prefix |
|---|---|---|
| 2 | Leasehold Valuation Tribunals | `lvt` |
| 1 | Rent Assessment Committees | `rac` |
| 4 | Residential Property Tribunals | `rpt` |

(Case-type id `3` exists in the URL scheme but publishes no decisions.)

Total: several thousand full-text decisions.

## How it works

Site: `https://residentialpropertytribunal.gov.wales` (Drupal 11).

1. `/decisions/{case_type}/%2A` lists the April–March "tribunal year" windows.
2. Each year window (`/decisions/{case_type}/{YYYY-04--YYYY-04}`) lists the
   per-decision detail-page slugs.
3. Each detail page (`/{slug}`) carries the reference number / Act / case type /
   property metadata (in its `<meta name="description">`) and one or more
   born-digital decision PDFs under `/sites/residentialproperty/files/`.
4. Each PDF is downloaded and its full text extracted with PyMuPDF (shared
   pdfplumber/pypdf fallback). No OCR is required — the PDFs carry a text layer.

Decision date is taken from the signed date at the foot of the decision text,
falling back to the month/year encoded in the reference number (`NNNN/MM/YY`).

## Usage

```bash
python bootstrap.py test               # connectivity + one-decision extraction
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent tribunal years)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
— Crown copyright. The site's copyright statement permits free use and re-use of
the material in any format or medium under the OGL (reproduce accurately, not in
a misleading context; logos excluded). Commercial use permitted.
