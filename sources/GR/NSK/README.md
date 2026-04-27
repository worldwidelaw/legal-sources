# GR/NSK - Greek Legal Council of the State

## Overview

The **Νομικό Συμβούλιο του Κράτους** (NSK - Legal Council of the State) is the official
legal advisory body to the Greek government. It has been providing binding legal opinions
(γνωμοδοτήσεις) since 1951.

## Data Source

- **Website**: https://www.nsk.gr
- **Search endpoint**: https://www.nsk.gr/web/nsk/anazitisi-gnomodoteseon
- **Coverage**: 1951 - present
- **Data type**: doctrine (official government legal opinions)
- **Authentication**: None (open data)

## Opinion Structure

Each opinion contains:

| Field | Description |
|-------|-------------|
| `consult_id` | Internal database ID |
| `opinion_number` | Official opinion number (Αριθμός) |
| `year` | Year of the opinion |
| `title` | The legal question posed (Τίτλος) |
| `summary` | The legal conclusion/answer (Περίληψη) |
| `president` | Presiding official |
| `rapporteur` | Rapporteur/reporter |
| `provisions` | Related legal provisions (Διατάξεις) |
| `keywords` | Subject matter tags (Λήμματα) |
| `status` | Acceptance status (Αποδεκτή, Μη αποδεκτή, etc.) |

## Status Values

| Greek | English | Meaning |
|-------|---------|---------|
| Αποδεκτή | Accepted | Opinion accepted by the requesting authority |
| Μη αποδεκτή | Not Accepted | Opinion rejected |
| Εν μέρει αποδεκτή | Partially Accepted | Opinion partially accepted |
| Εκκρεμεί αποδοχή | Pending | Awaiting acceptance decision |
| Ανακλήθηκε το ερώτημα | Withdrawn | Question was withdrawn |

## Full Text

The full text is composed of:
1. **Question (ΕΡΩΤΗΜΑ)**: The legal question from the `title` field
2. **Answer (ΑΠΑΝΤΗΣΗ)**: The legal conclusion from the `summary` field

PDF documents are also available for download but the HTML metadata provides
the essential content.

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all opinions from 1951)
python bootstrap.py bootstrap

# Update (fetch recent opinions)
python bootstrap.py update
```

## Rate Limits

The scraper is configured for 1 request per second with a burst of 3.
The NSK website uses a Liferay portal and may have session timeouts.

## License

Public domain — official government legal opinions of the Hellenic Republic.
