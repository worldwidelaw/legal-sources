# MC/JournalMonaco - Monaco Official Journal

## Source Information

- **Country**: Monaco (MC)
- **Name**: Journal de Monaco
- **URL**: https://journaldemonaco.gouv.mc
- **Data Types**: Legislation
- **Language**: French
- **License**: Open Government Data (Monaco)

## Description

The Journal de Monaco is the official gazette of the Principality of Monaco, published weekly since 1858. It contains all official legal texts including laws, sovereign ordinances, ministerial decrees, and other regulatory instruments.

## Data Coverage

- **Archive**: 1858-present (digitized)
- **Official Electronic Version**: 2015-present
- **Publication Frequency**: Weekly (Fridays)
- **Estimated Records**: ~50 legislative documents per year × ~10 years = 500+ documents

## Document Types Collected

| Type | French Name | Description |
|------|-------------|-------------|
| Law | Loi | Acts passed by the National Council |
| Sovereign Ordinance | Ordonnance Souveraine | Executive decrees by the Prince |
| Ministerial Decree | Arrêté Ministériel | Regulations by Ministers |
| Municipal Decree | Arrêté Municipal | Local government regulations |
| Sovereign Decision | Décision Souveraine | Princely decisions |

## Technical Details

### Strategy
1. Lists all journal issues for each year via `/Journaux?year={year}`
2. Parses table of contents from each journal issue page
3. Fetches full HTML text from individual article pages
4. Extracts text from `<div class="body">` element
5. Filters for legislation-relevant documents

### Rate Limiting
- 1.5 second delay between requests
- Respectful crawling of official government site

### Full Text Extraction
Full text is extracted from the HTML body of each article page. The text is cleaned of HTML tags and formatting while preserving paragraph structure.

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (10-12 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Output

```json
{
  "_id": "MC_JDM_2026_8786_Arrete-Ministeriel-n-2026-59...",
  "_source": "MC/JournalMonaco",
  "_type": "legislation",
  "title": "Arrêté Ministériel n° 2026-59 du 5 février 2026...",
  "text": "NOUS, Ministre d'État de la Principauté...",
  "date": "2026-02-05",
  "url": "https://journaldemonaco.gouv.mc/Journaux/2026/Journal-8786/...",
  "document_type": "ministerial_decree",
  "document_number": "2026-59",
  "journal_number": "8786",
  "journal_year": "2026",
  "language": "fr"
}
```

## Notes

- No authentication required
- Electronic official version since 2015
- Historical archives available but earlier documents may be PDF-only
- Excludes non-legislative content (job postings, legal notices, corporate announcements)

## License

[Open Government Data](https://journaldemonaco.gouv.mc) — official gazette of the Principality of Monaco.
