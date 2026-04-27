# IT/ConsiglioDiStato - Italian Administrative Courts

This source provides access to decisions from the Italian Council of State (Consiglio di Stato) and all Regional Administrative Courts (TAR).

## Coverage

- **Council of State (CdS)**: Supreme administrative court, appellate jurisdiction
- **CGA Sicily**: Administrative Justice Council of the Sicily Region
- **TAR**: 29 Regional Administrative Courts (first instance)

## Data Access

### OpenGA Portal (Metadata)

The OpenGA portal (https://openga.giustizia-amministrativa.it) provides CKAN-based access to decision metadata:

- Sentenze (judgments)
- Ordinanze (orders)
- Decreti (decrees)
- Pareri (opinions)

Datasets are organized by court and year. Example:
- `cds-sentenze` - Council of State judgments
- `tar-lazio-roma-sentenze` - TAR Lazio (Rome) judgments

### Full Text Endpoint

Full text is available via the MDP endpoint:

```
https://mdp.giustizia-amministrativa.it/visualizza/?nodeRef=&schema={court}&nrg={case_number}&nomeFile={decision_number}_11.html&subDir=Provvedimenti
```

Returns XML with full decision text including:
- Epigrafe (header with court, judges, parties)
- Premessa (facts)
- Motivazione (legal reasoning)
- Dispositivo (operative part)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (validation)
python bootstrap.py bootstrap --sample

# Full bootstrap (all courts, all years)
python bootstrap.py bootstrap

# Incremental update (last 30 days)
python bootstrap.py update
```

## Data Schema

| Field | Description |
|-------|-------------|
| `_id` | `IT:GA:{court}:{year}:{decision_number}` |
| `ecli` | European Case Law Identifier |
| `court` | Court name |
| `section` | Court section |
| `decision_type` | Sentenza, Ordinanza, Decreto, Parere |
| `decision_number` | Decision number |
| `case_number` | Case registration number (NRG) |
| `date` | Publication date |
| `text` | Full decision text |

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.

## Notes

- Average document size: ~30K-60K characters
- Some personal data may be redacted (`-OMISSIS-`)
- OpenGA portal launched in 2024 with PNRR funding
- Historical decisions may require different access methods
