# AU/NTLegislation — Northern Territory Legislation (legislation.nt.gov.au)

**Source:** [https://legislation.nt.gov.au/](https://legislation.nt.gov.au/)
**Data types:** legislation
**Jurisdiction:** AU-NT (Northern Territory)
**Auth:** none

Consolidated in-force Northern Territory Acts and subordinate legislation
(regulations, rules, by-laws) from the official legislation register, ~687
documents in total (~385 Acts + ~303 subordinate instruments).

## How it works

1. **Discover** document slugs from the two browse-by-title listings
   (`/en/LegislationPortal/Acts/By-Title` and `.../Subordinate-Legislation/By-Title`).
   Sitecore serves links as `/en/Legislation/SLUG` on a cookie-less first hit
   but drops the language prefix to `/Legislation/SLUG` once an
   `ASP.NET_SessionId` exists, so both spellings are accepted.
2. **Resolve** each slug's numeric download ID from its document page.
3. **Download** the PDF via `/api/sitecore/Act/PDF?id={NUMERIC_ID}` — Acts and
   subordinate legislation both use the `Act` endpoint.
4. **Extract** full text with pdfplumber (fallback: pypdf), and parse the
   `As in force at <date>` currency line into an ISO 8601 date.

The register renders each PDF on demand with no server-side cache (~3 s for a
small regulation, ~20 s for a large consolidated Act). That work therefore
lives in `normalize()`, so `bootstrap-fast` overlaps it across worker threads
rather than serializing 687 renders. Every request runs under a wall-clock
deadline, and a run that dies part-way resumes from `data/records.jsonl`.

## Usage

```bash
python bootstrap.py test               # connectivity + extraction check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap-fast     # full corpus, 5 concurrent downloads
```

## License

[Northern Territory legislation copyright policy](https://legislation.nt.gov.au/en/Footer/Terms-of-Use)
— Crown copyright in NT legislation is owned by the Northern Territory, which
grants blanket permission to republish it (commercial use included) provided
the publication does not indicate it is an official version, does not use the
arms of the Northern Territory, and reproduces the material accurately in a
context that does not mislead. The Northern Territory reserves the right to
revoke or vary that permission on reasonable notice.

This is **not** a Creative Commons licence: the legislation register carries
its own copyright policy, adopted 8 October 1996.
