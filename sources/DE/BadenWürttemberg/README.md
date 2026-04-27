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

## Authentication
No user authentication required. The portal uses a service account
(`BuergerserviceBW2023`) for anonymous public access.

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Usage
```bash
# Fetch 15 sample records
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all laws)
python3 bootstrap.py bootstrap

# Check status
python3 bootstrap.py status
```

## Notes
- The jPortal API is shared across 9+ German state Landesrecht portals
- Document IDs follow pattern: `jlr-<Abbreviation>[Year]V<Version>P<Paragraph>`
- The `rahmen` suffix identifies the root/framework document for each law
- Rate limited to 1.5s between requests to respect server capacity
