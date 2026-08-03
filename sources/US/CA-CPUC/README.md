# US/CA-CPUC — California Public Utilities Commission Decisions & Resolutions

Full text of the **California Public Utilities Commission's** adjudicatory
dispositions — **Final Decisions** and **Final Resolutions** — that resolve
numbered CPUC proceedings affecting electric, gas, water, sewer,
telecommunications and transportation utilities (rate cases, applications,
complaints, investigations, rulemakings and enforcement). These are
`case_law`.

## Source

- **Publisher:** California Public Utilities Commission (CPUC)
- **Repository:** https://docs.cpuc.ca.gov/advancedsearchform.aspx
- **Coverage:** documents issued after ~June 2000 (born-digital, online)

## How it works

The CPUC document repository at `docs.cpuc.ca.gov` is a classic ASP.NET
WebForms application with **no** documented REST/JSON API, but it renders
full result tables server-side, so it is driven with plain HTTP POSTs:

1. `GET /advancedsearchform.aspx` → read `__VIEWSTATE` /
   `__VIEWSTATEGENERATOR` / `__EVENTVALIDATION`.
2. `POST /advancedsearchform.aspx` with `ddlCpuc01Types` set to the
   document-type id (`19` = *Final Decision*, `55` = *Final Resolution*)
   and a `PubDateFrom` / `PubDateTo` month window. The response is the
   first page of a result table — each row carries the decision number +
   caption + `Proceeding: <num>`, the doc type, a **direct** PublishedDocs
   PDF link and the published date.
3. Page through the rest by `POST /SearchRes.aspx` with
   `__EVENTTARGET=lnkNextPage` (viewstate carried forward) until no
   `lnkNextPage` control remains.
4. Download each decision PDF straight from
   `docs.cpuc.ca.gov/PublishedDocs/...` and extract its text with
   `common.pdf_extract`. A `<200`-char guard skips the rare scanned/empty
   document.

Discovery walks month by month from the present back to June 2000.

Draft/proposed dispositions (*Agenda Decision*, *Comment Decision*) are
intentionally excluded to avoid duplicating the final adopted decisions.
ALJ *Rulings* (`ddlCpuc01Types=58`) are an easy future extension.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 recent samples
python bootstrap.py bootstrap           # full pull (present -> 2000-06)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain — US Government Work](https://www.law.cornell.edu/uscode/text/17/105) — Decisions and resolutions of the California Public Utilities Commission are official government works in the public domain under the government-edicts doctrine (state adjudicatory dispositions). No copyright is asserted; commercial use permitted, no attribution required.
