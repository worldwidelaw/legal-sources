# IT/CorteCostituzionale -- Italian Constitutional Court

Fetches Constitutional Court (Corte Costituzionale) decisions from the official
open data portal.

## Data Source

- **Portal**: https://dati.cortecostituzionale.it
- **Main Site**: https://www.cortecostituzionale.it
- **License**: CC BY SA 3.0 (Creative Commons Attribution ShareAlike)
- **Coverage**: All decisions from 1956 to present (~21,000 decisions)
- **Update Frequency**: Periodically updated with new decisions

## Data Access Methods

### 1. JSON Downloads (Primary)
The portal provides pre-packaged JSON datasets organized by time period:
- `P_json2001_oggi.zip` - 2001 to present
- `P_json1981_2000.zip` - 1981 to 2000
- `P_json1956_1980.zip` - 1956 to 1980

Each ZIP contains nested yearly ZIPs with JSON files.

### 2. SPARQL Endpoint
For metadata queries and filtering:
- Endpoint: `https://dati.cortecostituzionale.it/sparql/endpoint`
- Query interface: `https://dati.cortecostituzionale.it/sparql/snorql`

### 3. CSV/XML Downloads
Alternative formats available at:
`https://dati.cortecostituzionale.it/Scarica_i_dati/Scarica_i_dati`

## Decision Types

- **Sentenza (S)**: Judgment on constitutional legitimacy
- **Ordinanza (O)**: Procedural order (inadmissibility, etc.)
- **Decreto (D)**: Decree

## ECLI Format

Decisions use the European Case Law Identifier:
```
ECLI:IT:COST:YYYY:N
```
Example: `ECLI:IT:COST:1956:1` (First decision of the Constitutional Court)

## JSON Record Structure

```json
{
  "collegio": "Court composition...",
  "numero_pronuncia": "1",
  "anno_pronuncia": "1956",
  "data_decisione": "05/06/1956",
  "epigrafe": "Case summary...",
  "relatore_pronuncia": "Judge name",
  "testo": "FULL TEXT OF THE DECISION",
  "ecli": "ECLI:IT:COST:1956:1",
  "dispositivo": "Ruling/disposition...",
  "data_deposito": "14/06/1956",
  "redattore_pronuncia": "Drafter name",
  "tipologia_pronuncia": "S",
  "presidente": "President name"
}
```

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (downloads ~55MB of JSON data)
python bootstrap.py bootstrap

# Incremental update (recent years only)
python bootstrap.py update
```

## License

[CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/) — attribution required, share-alike.

## Notes

- Full text is available in the `testo` field (average ~15K characters)
- Dates are in DD/MM/YYYY format, normalized to ISO 8601
- HTML entities (&#13; etc.) are cleaned from text
- Court composition (`collegio`) lists all judges for each decision
