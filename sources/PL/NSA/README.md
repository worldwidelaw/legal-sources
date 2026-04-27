# PL/NSA - Polish Supreme Administrative Court

**Naczelny Sąd Administracyjny (NSA)** - Supreme Administrative Court of Poland

## Source

- **URL**: https://orzeczenia.nsa.gov.pl
- **Database**: Centralna Baza Orzeczeń Sądów Administracyjnych (CBOSA)
- **Coverage**: 2004-present (with selected earlier decisions)
- **Total records**: 427,000+ administrative court judgments

## Courts Covered

- **NSA** - Naczelny Sąd Administracyjny (Supreme Administrative Court)
- **WSA** - 16 Wojewódzkie Sądy Administracyjne (Regional Administrative Courts)

## Data Access

The database provides public access to anonymized court decisions. No authentication required.

### Endpoints

- Search: `POST /cbo/search` with form data
- Document: `GET /doc/{HEXID}` returns HTML page
- RTF export: `GET /doc/{HEXID}.rtf` returns RTF file

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records (12+)
python bootstrap.py bootstrap --sample

# Full bootstrap (427K+ records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update

# Check checkpoint status
python bootstrap.py status

# Clear checkpoint
python bootstrap.py clear-checkpoint
```

## Data Fields

| Field | Description |
|-------|-------------|
| `_id` | Unique document ID (hex string) |
| `case_number` | Case signature (e.g., "III FSK 24/25") |
| `title` | Judgment title |
| `text` | Full text (Sentencja + Uzasadnienie) |
| `date` | Judgment date |
| `court` | Court name |
| `decision_type` | Type (Wyrok, Postanowienie, etc.) |
| `judges` | List of judges |
| `keywords` | Subject matter keywords |
| `legal_bases` | Referenced legal provisions |

## Subject Areas

Administrative court decisions cover:
- Tax law
- Environmental law
- Building permits
- Social security
- Administrative procedures
- Public procurement
- Local government

## License

[Public Domain](https://dane.gov.pl) — official court decisions published by the Polish judiciary.

## Notes

- Text is anonymized (personal data redacted)
- The database has informational and educational character only
- Not the official publisher of court decisions (Dziennik Urzędowy)
