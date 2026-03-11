# DE/Sachsen - Saxon State Law (REVOSAX)

## Overview

This source fetches Saxon state legislation from REVOSAX (Recht und Vorschriftenverwaltung Sachsen),
the official legal database of the Free State of Saxony.

## Data Source

- **Portal**: https://www.revosax.sachsen.de
- **Sitemap**: https://www.revosax.sachsen.de/sitemap.xml
- **Coverage**: 6,232+ laws, ordinances, and administrative regulations
- **Format**: HTML full text
- **License**: Public Domain (amtliche Werke § 5 UrhG)

## Document Types

- Gesetze (Laws)
- Verordnungen (Ordinances)
- Verwaltungsvorschriften (Administrative Regulations)
- Staatsverträge (Interstate Treaties)
- Bekanntmachungen (Announcements)

## Technical Details

- Uses sitemap.xml for document discovery
- Fetches full HTML text from `/vorschrift/{id}` URLs
- Extracts text content from `<article id="lesetext">` element
- Rate limit: 2 requests/second

## Sample Records

Run bootstrap to fetch sample records:

```bash
python3 bootstrap.py bootstrap --sample
```

## Notes

- First German Länder (state) source in the project
- Can serve as model for other German state portals
- Historical versions available via `/vorschrift/{id}.{version}` URLs
