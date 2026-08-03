# US/OR-EthicsOpinions — Oregon Government Ethics Commission (OGEC), Advisory & Staff Opinions

Full text of the ethics guidance issued by the **Oregon Government Ethics
Commission (OGEC)**, the state agency created under **ORS chapter 244** to
interpret and enforce the Oregon Government Ethics law (standards of conduct for
public officials), the public-meetings law and lobbying regulation.

## What this collects

Two written opinion series, each the agency's written interpretation of the
ethics statutes (**doctrine**):

- **Commission Advisory Opinion** (`A` series) — guidance adopted by the
  Commission under ORS 244.280; binding, and the requester may rely on it.
- **Staff Advisory Opinion** (`S` series) — the Executive Director's written
  assessment of how the ethics laws apply to a set of stated facts.

The pre-2017 historical corpus (~68 distinct opinions) is published as a
Microsoft **SharePoint list**, enumerated in a single call through the public,
unauthenticated SharePoint REST API. Each opinion is served as a born-digital or
scanned PDF under `/ogec/public-records/Documents/{PublicationNo}.pdf`.

## How it works

1. `GET .../public-records/_api/web/lists/getbytitle('Advisory & Staff Opinions
   ~ Prior to 2017')/items?$top=1000` — one JSON call returns every list row
   (publication number, issue date, opinion type, subject metadata, and the
   PDF href in a "View Details" anchor).
2. Dedup by PDF href (a single opinion is indexed under several subject rows);
   aggregate the subject topics/summaries onto the one record.
3. Download each PDF, verify the `%PDF` magic (a missing file returns a `200`
   HTML soft-404 page), and extract full text via the shared PDF extractor
   (text layer for older opinions, OCR for the scanned newer ones).

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 samples, newest first
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Scope note

Opinions issued **2017–present** live in the OGEC Case Management System
(`apps.oregon.gov/OGEC/CMS/Advice`). Its metadata is public via a DataTables
endpoint (~414 records) but the opinion **files** are served only through an
authenticated `File/GetFile` route (HTTP 500 for anonymous callers), so full
text is not retrievable for that set. This source covers the fully-public
pre-2017 corpus; revisit if OGEC exposes the CMS files publicly.

## License

[Public Domain](https://www.law.cornell.edu/uscode/text/17/105) — advisory and
staff opinions of the Oregon Government Ethics Commission are official public
records of an Oregon state agency interpreting state statute (ORS chapter 244);
government-edict works published for public use. No copyright is asserted.
Commercial use permitted.
