# FR/ARCEP - French Telecommunications Regulatory Authority

ARCEP (Autorité de régulation des communications électroniques, des postes et de la distribution de la presse) is France's independent regulatory authority for electronic communications, postal services, and press distribution.

## Data Source

- **Website**: https://www.arcep.fr
- **Decisions page**: https://www.arcep.fr/la-regulation/avis-et-decisions-de-larcep.html
- **RSS feed**: https://www.arcep.fr/actualites/suivre-actualite-regulation-arcep/avis-et-decisions/rss.xml
- **License**: Licence Ouverte (Open Government License)

## Coverage

- **Documents**: ~46,000+ regulatory decisions
- **Period**: 1997 to present
- **Updates**: Daily (new decisions published continuously)

## Document Types

- **Frequency authorizations**: Mobile network spectrum, radio equipment permits
- **Network access**: Interconnection rules, wholesale access obligations
- **Universal service**: Postal service coverage, emergency services
- **Numbering**: Telephone number allocation and portability
- **Experiments**: 5G trials, drone communications, satellite networks

## Data Access Method

1. **CSV export**: Full catalog with metadata (decision number, dates, category, description, PDF URL)
2. **PDF download**: Direct links to official decision PDFs at `/uploads/tx_gsavis/{number}.pdf`
3. **Text extraction**: Full text extracted from PDFs using pdfplumber

## Usage

```bash
# Generate sample data (12 documents)
python3 bootstrap.py bootstrap --sample

# Fetch all decisions (JSON to stdout)
python3 bootstrap.py bootstrap

# Fetch updates since a date
python3 bootstrap.py updates --since 2026-01-01
```

## Sample Output

```json
{
  "_id": "ARCEP-26-0385",
  "_source": "FR/ARCEP",
  "_type": "doctrine",
  "title": "Décision n° 2026-0385...",
  "text": "RÉPUBLIQUE FRANÇAISE...",
  "date": "2026-02-18",
  "category": "Réseaux ouverts au public",
  "number": "26-0385",
  "pdf_url": "https://www.arcep.fr/uploads/tx_gsavis/26-0385.pdf"
}
```

## Notes

- Most decisions are 1-10 pages (1,000-30,000 characters)
- Some large decisions (market analyses, consultations) can be 100+ pages
- Annexes are listed separately and linked when available
