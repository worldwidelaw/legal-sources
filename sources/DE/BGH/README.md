# DE/BGH - German Federal Court of Justice (Bundesgerichtshof)

## Overview

The Federal Court of Justice (Bundesgerichtshof, BGH) is the highest court of civil and criminal jurisdiction in Germany. This data source fetches case law decisions from the official German legal information portal.

## Data Source

- **Portal**: [rechtsprechung-im-internet.de](https://www.rechtsprechung-im-internet.de)
- **RSS Feed**: `https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bgh.xml`
- **Coverage**: Decisions from 2010 onwards
- **Format**: HTML full text with ECLI identifiers

## Court Structure

The BGH has the following senates:

**Civil Senates (Zivilsenate)**:
- I. Zivilsenat - Intellectual property
- II. Zivilsenat - Corporate law, partnerships
- III. Zivilsenat - Public law, state liability
- IV. Zivilsenat - Insurance law
- V. Zivilsenat - Real estate law
- VI. Zivilsenat - Tort law, personality rights
- VIa. Zivilsenat - Automotive/diesel cases
- VII. Zivilsenat - Contract and construction law
- VIII. Zivilsenat - Consumer protection, sales law
- IX. Zivilsenat - Insolvency law
- X. Zivilsenat - Patent law (nullity)
- Xa. Zivilsenat - Patent law (infringement)
- XI. Zivilsenat - Banking law
- XII. Zivilsenat - Family law

**Criminal Senates (Strafsenate)**:
- 1. Strafsenat
- 2. Strafsenat
- 3. Strafsenat
- 4. Strafsenat
- 5. Strafsenat

## Data Fields

| Field | Description |
|-------|-------------|
| `_id` | Internal document ID (e.g., KORE702612026) |
| `ecli` | European Case Law Identifier |
| `aktenzeichen` | Case file number (e.g., VIa ZR 232/23) |
| `court` | Full court name with senate |
| `decision_type` | Urteil (judgment), Beschluss (order) |
| `date` | Decision date (ISO 8601) |
| `text` | Full text of the decision |
| `norms` | Referenced legal provisions |
| `summary` | Brief description from RSS |

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Usage

```bash
# Test fetch (3 documents)
python3 bootstrap.py

# Bootstrap sample data (12 documents)
python3 bootstrap.py bootstrap --sample
```
