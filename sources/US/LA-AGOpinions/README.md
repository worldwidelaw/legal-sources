# US/LA-AGOpinions — Louisiana Attorney General Opinions

Formal advisory legal opinions of the **Louisiana Attorney General**. Under
La. R.S. 49:251–253 the Attorney General renders written legal opinions, on
request, to state and local public officials and bodies, construing the
constitution, statutes and regulations of Louisiana. Each opinion is an
authoritative statement of the state's legal position on a question of law =
**doctrine** (an official advisory opinion of the state, the sibling of
`US/OR-AGOpinions`, `US/CA-AGOpinions`, `US/NC-AGOpinions`, and the other US
state AG-opinion sources).

## Source

The corpus is published by the **State Library of Louisiana** as part of its
CONTENTdm *Louisiana Public Documents Digital Archive* (collection alias
`p267101coll4`) on the OCLC-hosted CONTENTdm instance at
`https://cdm16313.contentdm.oclc.org/`. CONTENTdm exposes a public,
un-authenticated JSON web-services API (`dmwebservices`). The Attorney
General's own website (`ag.louisiana.gov`) is WAF-gated for datacenter/foreign
clients, but this State Library CONTENTdm route is open.

## Access recipe

1. **Enumerate** — the archive holds ~44,700 heterogeneous public documents, so
   the AG opinions are isolated with a server-side title search:

   ```
   GET /digital/bl/dmwebservices/index.php?q=dmQuery/p267101coll4/
       title^Attorney General^all^and/dmrecord!title!date/date/{max}/{start}/1/0/0/0/json
   ```

   The hit set (~1,793) contains the individual opinions, the weekly opinion
   *summaries* (`... [Summary]`) and the AG's monthly press *columns*. Only the
   records whose title is exactly **"Attorney General's Opinions"** (~1,163) are
   the individual opinions — those are kept; summaries and columns are dropped.

2. **Per item** —

   ```
   GET /digital/bl/dmwebservices/index.php?q=dmGetItemInfo/p267101coll4/{dmrecord}/json
   ```

   returns the Dublin-Core metadata: `title`, `date` (ISO), `descri` (caption,
   e.g. *"July 1, 2013; Opinion 13-0112"*), `subjec` (subject headings), `find`
   (PDF file name). The opinion number is parsed from the caption (or the body).

3. **Full text** — the `transc` field is empty for this collection, so the
   opinion text is extracted from the item PDF, downloaded from the CONTENTdm
   download endpoint:

   ```
   GET /digital/api/collection/p267101coll4/id/{dmrecord}/download
   ```

   The archived opinion PDFs carry an embedded text layer (the State Library's
   own OCR); extraction uses the shared, OOM-hardened `common.pdf_extract`
   helper (pdfplumber → pypdf → OCR fallback). The rare pure image scans
   (0 chars) are skipped.

No CAPTCHA, no auth, reachable locally.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (~1,163 opinions)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Louisiana Attorney General opinions are official works of Louisiana state
government (edicts of government) and are not subject to copyright. Free to use,
including commercially.
