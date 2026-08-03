# UK/AdjudicationPanelWales — The Adjudication Panel for Wales (Panel Dyfarnu Cymru)

Full-text **Decision Reports** of the **Adjudication Panel for Wales (APW)**, the
independent statutory tribunal established under Part III of the Local Government
Act 2000. APW determines allegations that elected and co-opted members of Welsh
county/community councils, national park authorities and fire and rescue
authorities have breached their authority's statutory code of conduct.

Cases are referred by the **Public Services Ombudsman for Wales** to a **Case
Tribunal** for initial determination, or reach an **Appeal Tribunal** on appeal
against a local standards committee's finding. Decision Reports set out the
findings and any sanction (disqualification, suspension, etc.). These are binding,
appealable **case law for the Wales (GB-WLS) jurisdiction** and are **not**
covered by `UK/CaseLaw` (the National Archives *Find Case Law* service indexes
England & Wales superior courts and reserved UK tribunals only).

## Coverage

| Case-type id | Tribunal | Reference suffix |
|---|---|---|
| 1 | Case Tribunal | `/CT` |
| 2 | Appeal Tribunal | `/AT` |

Total: a few hundred full-text decision reports, 2003–present.

## How it works

Site: `https://adjudicationpanel.gov.wales` (Drupal 11 — same platform as
`UK/RPTWales`).

1. `/decisions/{case_type}/%2A` lists the April–March "tribunal year" windows.
2. Each year window lists the per-decision detail-page slugs (`/apw{digits}-…`).
3. Each detail page carries the respondent name / reference number / relevant
   authority / nature of allegation / hearing date / tribunal decision (packed in
   the `<meta name="description">`) and the decision-report PDF under
   `/sites/adjudicationpanel/files/`.
4. Each PDF is downloaded and its full text extracted with PyMuPDF (shared
   pdfplumber/pypdf fallback). No OCR is required.

Decision date is taken from the metadata `Hearing:` date where present, falling
back to the year encoded in the reference number, then the last date in the body.

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
