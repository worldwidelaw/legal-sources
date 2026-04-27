# IT/GarantePrivacy

Italian Data Protection Authority (Garante per la protezione dei dati personali) decisions fetcher.

## Data Source

- **Authority**: Garante per la protezione dei dati personali
- **Country**: Italy
- **URL**: https://www.garanteprivacy.it
- **Data Types**: case_law (enforcement decisions, sanctions), doctrine (guidelines, opinions)
- **License**: Public (Italian government data)
- **Language**: Italian

## Access Method

The scraper uses the Garante's docweb document management system:

1. **Search Page**: Paginated search at `/home/ricerca/-/search/tipologia/Provvedimenti`
2. **Document Pages**: Individual documents at `/home/docweb/-/docweb-display/docweb/{id}`
3. **Full Text**: Extracted from `<div id="interna-webcontent">` on each page

## Document Types

The Garante publishes various types of provvedimenti:

- **Sanctions** (sanzioni): GDPR fines and penalties
- **Warnings** (ammonimenti): Formal warnings to data controllers
- **Authorizations** (autorizzazioni): Data transfer authorizations
- **Opinions** (pareri): Advisory opinions
- **Prescriptions** (prescrizioni): Corrective measures
- **Guidelines** (linee guida): Guidance documents

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10-15 documents)
python bootstrap.py bootstrap --sample

# Full fetch (all ~13,000+ documents)
python bootstrap.py bootstrap

# Incremental update (last 30 days)
python bootstrap.py update
```

## Data Volume

- Approximately 13,000+ provvedimenti available
- 1,336 pages of search results (as of 2026)
- Average document length: 5,000-20,000 characters

## License

Public government data — Italian Data Protection Authority official publications. No formal open data license specified; reuse is generally permitted for public government information.

## Rate Limiting

- 0.5 requests per second (2 second delay)
- Respectful crawling of government website
