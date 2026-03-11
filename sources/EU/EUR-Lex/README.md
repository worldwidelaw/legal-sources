# EU/EUR-Lex Data Source

## Overview

EUR-Lex is the official portal for European Union law, providing access to:
- EU treaties
- Legislation (regulations, directives, decisions)
- Case law from the Court of Justice of the EU
- Preparatory documents
- International agreements

## Data Access Methods

This fetcher uses a hybrid approach:

1. **EUR-Lex Search Interface**: Discovers documents by type and year
2. **Direct HTML Retrieval**: Fetches full text in HTML format
3. **CELLAR API**: Available as fallback for structured metadata

## Implementation Details

### Discovery
- Uses EUR-Lex search with filters for document type and year
- Extracts CELEX numbers (unique document identifiers)
- Paginated results with rate limiting

### Full Text Retrieval
- Fetches HTML version of documents using CELEX identifiers
- Extracts clean text from HTML, removing navigation and formatting
- Preserves document structure and readability

### Document Types Covered
- **REG**: Regulations (binding legislative acts)
- **DIR**: Directives (goals for member states to achieve)
- **DEC**: Decisions (binding on specific parties)

## API Endpoints Used

- Search: `https://eur-lex.europa.eu/search.html`
- HTML Documents: `https://eur-lex.europa.eu/legal-content/{LANG}/TXT/HTML/?uri=CELEX:{ID}`
- SPARQL (backup): `http://publications.europa.eu/webapi/rdf/sparql`

## Rate Limiting

- 1 second delay between requests
- Exponential backoff on failures
- Maximum 3 retries per request

## Data Schema

Documents are normalized to include:
- `celex_id`: Unique CELEX identifier
- `document_type`: Type of legal document
- `title`: Document title
- `text`: Full text content
- `date`: Publication/adoption date
- `url`: Link to original document

## Usage

```bash
# Fetch sample data (10 documents)
python3 bootstrap.py bootstrap --sample

# Fetch more documents
python3 bootstrap.py bootstrap

# Test the fetcher
python3 bootstrap.py
```

## Notes

- EUR-Lex provides multilingual content; this fetcher defaults to English
- Some older documents may only be available as PDFs
- The CELLAR repository provides additional metadata via SPARQL
- Web service access requires registration for bulk operations

## Resources

- [EUR-Lex Data Reuse](https://eur-lex.europa.eu/content/help/data-reuse/reuse-contents-eurlex-details.html)
- [CELLAR Documentation](https://op.europa.eu/en/web/cellar/cellar-data)
- [EUR-Lex Web Services](https://eur-lex.europa.eu/content/help/data-reuse/webservice.html)