# DE/BadenWürttemberg - Baden-Württemberg State Law (Landesrecht BW)

## Overview
Fetches state legislation from the official Baden-Württemberg Landesrecht portal
via the juris jPortal REST API.

## Data Source
- **URL**: https://www.landesrecht-bw.de
- **API**: jPortal REST API at `/jportal/wsrest/recherche3/`
- **Coverage**: ~63K individual norms across 1000+ laws
- **Categories**: Gesetze (63K), Rechtsprechung (19K), VV (11K), Verkündungsblätter (8K)

## Access Method
The portal is a React SPA backed by a REST API. The scraper:
1. Establishes a session via the portal page (gets JWT + session cookies)
2. Initializes the API to get a CSRF token
3. Paginates search results to discover unique law IDs
4. Fetches each law as a Gesamtausgabe (complete edition) with full HTML text
5. Cleans HTML to plain text

### docId scheme
The portal uses opaque document IDs. A law is `jlr-` plus a 13-character key;
its individual norms are that key plus `NN` plus an 11-digit sequence:

```
jlr-NNLBW00007BCC                 Landesbauordnung (the whole law)
jlr-NNLBW00007BCCNN00000000080    § 58 LBO
```

So the ~63K norms collapse to ~1,100 laws by truncating to the 17-character
law id, and each is fetched once with `docPart: "X"` (Gesamtausgabe).
Appending `rahmen` to one of these — which the older mnemonic scheme required
— produces a docId the API answers with HTTP 500 (issue #1384).

## Authentication
No user authentication required. The portal uses a service account
(`BuergerserviceBW2023`) for anonymous public access.

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Usage
```bash
# Fetch 15 sample records
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all laws) -> data/records.jsonl
python3 bootstrap.py bootstrap-fast

# Check status
python3 bootstrap.py status
```

`bootstrap-fast` appends to `data/records.jsonl` and skips laws already written
there, so a relaunch resumes instead of re-crawling. `bootstrap` (without
`--sample`) is an alias for the same path.

## Notes
- The jPortal API is shared across 9+ German state Landesrecht portals
- Document IDs follow pattern: `jlr-<Abbreviation>[Year]V<Version>P<Paragraph>`
- The `rahmen` suffix identifies the root/framework document for each law
- Rate limited to 1.5s between requests to respect server capacity
