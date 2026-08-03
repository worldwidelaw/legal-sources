# INTL/OHCHR-Jurisprudence — UN Treaty Body Jurisprudence

Quasi-judicial decisions of the UN human-rights treaty bodies on individual
communications (complaints) against States — the **Views** (merits) and
**Decisions** (inadmissibility / discontinuance) adopted under the Optional
Protocols / complaint procedures of:

| Code | Committee |
|------|-----------|
| CCPR | Human Rights Committee |
| CAT | Committee against Torture |
| CED | Committee on Enforced Disappearances |
| CEDAW | Committee on the Elimination of Discrimination against Women |
| CERD | Committee on the Elimination of Racial Discrimination |
| CESCR | Committee on Economic, Social and Cultural Rights |
| CMW | Committee on Migrant Workers |
| CRC | Committee on the Rights of the Child |
| CRPD | Committee on the Rights of Persons with Disabilities |

Each decision is an authoritative interpretation of a core human-rights treaty
and is classified as **case_law**. This is distinct from `INTL/OHCHR-TBInternet`,
which is doctrine (UHRI recommendations / concluding observations).

## Access strategy

The public front-end at <https://juris.ohchr.org/> is a **Blazor Server SPA**
(SignalR WebSocket, Telerik UI) with no REST/JSON API. The same corpus is fully
enumerable through the older, server-rendered **Treaty Body Database** search:

```
https://tbinternet.ohchr.org/_layouts/15/TreatyBodyExternal/TBSearch.aspx
```

1. `GET TBSearch.aspx` → collect the WebForms ViewState / EventValidation.
2. `POST` with the **Jurisprudence** DocTypeCategory checked (Telerik RadListBox
   index 5) → a RadGrid of decisions (title, committee, State party, UN symbol,
   date, language, filename).
3. Paginate the RadGrid via its numeric page-link `__doPostBack` targets, using
   the forward `...` link to cross page windows; the current page is read from
   the `rgCurrentPage` marker.
4. For each unique symbol, `GET Download.aspx?symbolno=<symbol>` — an interstitial
   listing every language/format file with a `DownloadDraft.aspx?key=<enc>` link —
   pick the English file (prefer `.docx`, then `.pdf`), download and extract full
   text (docx: `word/document.xml`; pdf: PyMuPDF/fitz).

All endpoints are unauthenticated and reachable from an EU vantage.

## Usage

```bash
# Quick connectivity test (search + one download)
/usr/bin/python3 bootstrap.py test

# Sample (writes 12 records to sample/)
/usr/bin/python3 bootstrap.py bootstrap --sample

# Full corpus (streams to data/records.jsonl)
/usr/bin/python3 bootstrap.py bootstrap-fast
```

Requires `requests` and `PyMuPDF` (fitz). On this project's build machine use
`/usr/bin/python3` (Python 3.9), which has both.

## License

[United Nations Open Access](https://www.un.org/en/about-us/terms-of-use) — UN
treaty-body jurisprudence documents are official public documents of an
intergovernmental organization, issued as UN documents (Distr. General) for
public distribution. Treated as open government/IGO data. Attribution to the
United Nations / OHCHR expected. Commercial use permitted.
