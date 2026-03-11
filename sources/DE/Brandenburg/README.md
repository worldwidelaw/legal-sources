# DE/Brandenburg - Brandenburg State Law (BRAVORS)

## Source Information

- **URL**: https://bravors.brandenburg.de
- **Country**: Germany
- **Jurisdiction**: Brandenburg (state)
- **Data Types**: Legislation, Regulations
- **Language**: German

## Description

This source fetches Brandenburg state legislation from BRAVORS (Brandenburgisches Vorschriftensystem), the official state law database.

### Coverage

- **Laws (Gesetze)**: State laws passed by Brandenburg parliament
- **Regulations (Verordnungen)**: Administrative regulations
- **Administrative directives (Verwaltungsvorschriften)**: Administrative guidance

### Volume

Approximately 1,800+ legal documents across 9 subject areas:
1. State and constitutional law
2. Administration and defense
3. Justice
4. Civil and criminal law
5. Health, youth, family, sports, environment, culture
6. Finance
7. Economy and commerce
8. Labor law, social security, welfare
9. Post/telecommunications, transport, construction, housing

## Technical Details

### Access Method

HTML scraping of the BRAVORS portal:
1. Navigate subject area pages to discover law URLs
2. Fetch individual law pages
3. Extract full text from `reiterbox_innen_text` div

### URL Patterns

Laws are accessible via two URL patterns:
- `/de/gesetze-{numeric_id}` - Numeric ID format
- `/gesetze/{short_name}` - Short name format (e.g., `/gesetze/swg`)

### Rate Limiting

- 0.5 second delay between requests
- Respects server load

## License

German official works (§ 5 UrhG) - public domain. Legal texts published by government authorities are not protected by copyright in Germany.

## Usage

```bash
# Test mode (fetch 3 documents)
python3 bootstrap.py

# Bootstrap mode (fetch 12 sample documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (fetch 50 documents)
python3 bootstrap.py bootstrap
```

## Sample Output

```json
{
  "_id": "BB-gesetze_212792",
  "_source": "DE/Brandenburg",
  "_type": "legislation",
  "title": "Verfassung des Landes Brandenburg",
  "text": "Der Landtag hat am 14. April 1992...",
  "date": "1992-06-14",
  "url": "https://bravors.brandenburg.de/de/gesetze-212792",
  "jurisdiction": "Brandenburg",
  "language": "de"
}
```
