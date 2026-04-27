# DE/Bundeskartellamt - German Federal Cartel Office

## Source Information

- **Name**: Bundeskartellamt (German Federal Cartel Office)
- **URL**: https://www.bundeskartellamt.de
- **Country**: Germany (DE)
- **Data Types**: Competition decisions (regulatory_decisions)
- **Language**: German

## Coverage

- **Categories**:
  - Kartellverbot (cartel prohibition)
  - Missbrauchsaufsicht (abuse of dominance)
  - Fusionskontrolle (merger control)
- **Years**: 2000-present
- **Format**: PDF documents with full text

## Data Access

Decisions are published at:
- PDF: `/SharedDocs/Entscheidung/DE/Entscheidungen/{category}/{year}/{case_number}.pdf`
- HTML: `/SharedDocs/Entscheidung/DE/Entscheidungen/{category}/{year}/{case_number}.html`

Case numbers follow pattern: `B{division}-{number}-{year}` (e.g., B6-27-21)

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Usage

```bash
# Test with 3 documents
python bootstrap.py test

# Bootstrap with 15 sample documents
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap --limit 100
```
