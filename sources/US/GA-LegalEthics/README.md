# US/GA-LegalEthics — State Bar of Georgia — Formal Advisory Opinions

Legal-ethics advisory opinions published by the **State Bar of Georgia**
interpreting the **Georgia Rules of Professional Conduct** to advise **lawyers**.
Three co-published series are folded into this source:

| Series | Label | Issuer | Numbering |
|--------|-------|--------|-----------|
| **FAO** | Formal Advisory Opinion | Formal Advisory Opinion Board, approved/issued by the Supreme Court of Georgia under Bar Rule 4-403 (SCOG-approved FAOs are **binding**) | `YY-N` (e.g. 86-2, 05-2, 23-1) |
| **SDBAO** | Advisory Opinion | State Disciplinary Board (older numbered series) | `N` (e.g. 5, 16, 49) |
| **UPL** | UPL Advisory Opinion | Standing Committee on the Unlicensed Practice of Law | `YYYY-N` (e.g. 2012-1) |

The State Bar of Georgia is the state's **integrated (mandatory) bar**, so these
opinions are the work of a government-authorized body → treated as public domain.

## Data type

`doctrine` — advisory interpretations of the rules governing lawyers (SCOG-approved
FAOs are binding). This is the **attorney legal-ethics** series, distinct from
`US/GA-EthicsOpinions` (executive Georgia Government Transparency & Campaign
Finance Commission — public officials / campaign finance), `US/GA-Courts` and
`US/GA-Legislation`.

## Access & method

The opinions are published inside the **gabar.org online Handbook** (Sitefinity CMS).
The page

```
https://www.gabar.org/handbook/?handbook=Formal_Advisory_Opinions
```

returns a single **~16 MB HTML document** that embeds the entire handbook as an
HTML-entity-encoded JSON tree inside `<data id="bar-rules-data" value="...">`.
The `?rule=...` query fragments are client-side JS navigation only — the server
always renders the whole handbook and there are **no per-opinion server-side URLs**.

Method:

1. `GET` the handbook page **once** (generous timeout, ~16 MB).
2. Extract the `bar-rules-data` value, HTML-unescape it, `json.loads` the tree.
3. Walk the tree; classify each node by its `Title` into FAO / SDBAO / UPL.
4. De-duplicate on `(series, number)` (opinions appear ~4× as they are
   cross-referenced from several RPC rules), keeping the longest `Content`.
5. De-tag the `Content` HTML (BeautifulSoup) → full text. **No PDF, no OCR.**
6. Skip superseded/withdrawn FAOs that the CMS reduces to a boilerplate
   "replaced opinion" pointer with no body.

Result ≈ **84 substantive opinions** (FAO 52, SDBAO 25, UPL 7).

## Record shape

```json
{
  "_id": "US/GA-LegalEthics/FAO-05-2",
  "_source": "US/GA-LegalEthics",
  "_type": "doctrine",
  "opinion_number": "FAO-05-2",
  "series": "FAO",
  "title": "Formal Advisory Opinion No. 05-2: ...",
  "text": "FORMAL ADVISORY OPINION NO. 05-2 ...",
  "date": "2006-04-25",
  "issuer": "State Bar of Georgia — Formal Advisory Opinion Board / Supreme Court of Georgia",
  "jurisdiction": "US-GA",
  "url": "https://www.gabar.org/handbook/?handbook=Formal_Advisory_Opinions#..."
}
```

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US government edict — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — no attribution required.

State Bar of Georgia Formal Advisory Opinions are issued by the Formal Advisory
Opinion Board and approved/issued by the Supreme Court of Georgia under Bar Rule
4-403 (SCOG-approved FAOs are binding); the State Disciplinary Board and UPL
advisory opinions are likewise the work of Georgia's integrated (mandatory) bar.
Published free to the public in the gabar.org online Handbook with no login,
paywall or terms prohibiting reuse. Treated as public domain under the government-
edicts rationale, consistent with the other state-bar legal-ethics sources.
Commercial use permitted.
