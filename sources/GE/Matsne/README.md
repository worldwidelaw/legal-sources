# GE/Matsne - Georgian Legislative Herald

Georgian legislation from the Legislative Herald of Georgia (Matsne).

## Source Information

- **URL**: https://matsne.gov.ge
- **Country**: Georgia (GE)
- **Data Type**: Legislation
- **Language**: Georgian (ka), English (en), Russian (ru)
- **Authentication**: None required
- **License**: Open Government Data

## About Matsne

The Legislative Herald of Georgia (სსიპ "საქართველოს საკანონმდებლო მაცნე") is a
legal entity under public law within the governance of the Ministry of Justice
of Georgia. It provides access to consolidated Georgian primary and secondary
legislation.

## Data Access Strategy

This scraper uses a combination of approaches:

1. **RSS Feed** (`/en/document/feed`): For recent documents and updates
2. **Paginated Search** (`/en/document/search?page=N`): For comprehensive discovery
3. **Document View** (`/en/document/view/{id}`): For full text extraction

Full text is extracted from the HTML content within the `#maindoc` div on each
document page.

## Document Types

- Laws (საქართველოს კანონი)
- Organic Laws (საქართველოს ორგანული კანონი)
- Government Resolutions (საქართველოს მთავრობის დადგენილება)
- Presidential Decrees (საქართველოს პრეზიდენტის ბრძანებულება)
- Ministerial Orders (ბრძანება)
- International Agreements

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Output

Each normalized record contains:

```json
{
  "_id": "GE/Matsne/31252",
  "_source": "GE/Matsne",
  "_type": "legislation",
  "_fetched_at": "2026-02-19T12:00:00+00:00",
  "title": "Law of Georgia on Public Procurement",
  "text": "[Full text of the law...]",
  "date": "2005-04-20",
  "url": "https://matsne.gov.ge/en/document/view/31252",
  "doc_id": "31252",
  "issuer": "Parliament of Georgia",
  "language": "en"
}
```

## Notes

- The site requires a browser-like User-Agent header to avoid blocks
- English translations are available for many major laws
- Documents include consolidated versions with amendment history
- Rate limiting is set to 1 request/second to be respectful to the server

## Estimated Coverage

- Primary legislation: ~5,000+ documents
- Secondary legislation: ~50,000+ documents
- Historical laws from independence (1991) onwards
