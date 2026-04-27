# DE/BKartG - German Cartel Court (BGH Kartellsenat)

Case law from the Cartel Senate (Kartellsenat) of the German Federal Court of Justice (Bundesgerichtshof).

## Data Source

Official open data from [rechtsprechung-im-internet.de](https://www.rechtsprechung-im-internet.de), the German federal court decision database operated by the Federal Office of Justice (Bundesamt für Justiz).

## Coverage

- **Court**: Bundesgerichtshof (BGH) - Kartellsenat
- **Subject matter**: Competition law, antitrust, cartel enforcement, merger control
- **Time period**: 2000 - present
- **Documents**: ~571 decisions (as of 2026)

## Data Access

Documents are retrieved from the official Table of Contents XML which provides:
- Complete decision metadata
- Links to ZIP files containing full text in XML format

Filter: `gericht="BGH Kartellsenat"`

## Fields

| Field | Description |
|-------|-------------|
| _id | Document ID (doknr) |
| ecli | European Case Law Identifier |
| aktenzeichen | German case number |
| date | Decision date |
| title | Decision title |
| text | Full text (headnote, tenor, facts, reasoning) |
| headnote | Leitsatz (summary/headnote) |
| tenor | Operative part |
| norms | Cited legal norms |

## Usage

```bash
# Test with sample
python3 bootstrap.py bootstrap --sample

# Full bootstrap
python3 bootstrap.py bootstrap
```

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).
