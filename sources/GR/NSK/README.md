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

Each opinion carries a `ΛΗΨΗ ΑΡΧΕΙΟΥ` link to the signed PDF, served from the
Liferay portlet's resource phase (`p_p_lifecycle=2`) keyed on `consultId`.
Opinions from roughly **2021 onwards are born-digital** and extract to
20,000-75,000 characters of real opinion text — that is what lands in `text`.

Earlier PDFs are **scanned images** (`DCTDecode` bitmaps, 0 characters without
OCR), so they are not downloaded at all. For those years `text` falls back to
the listing content, which is still substantive:

1. **ΕΡΩΤΗΜΑ** — the legal question put to NSK (`title`)
2. **ΑΠΑΝΤΗΣΗ** — the reasoned conclusion (`summary` / Περίληψη)
3. **ΔΙΑΤΑΞΕΙΣ** — the provisions construed

The cutoff year is `PDF_TEXT_FROM_YEAR`, overridable with the
`NSK_PDF_FROM_YEAR` environment variable if OCR becomes available.

A handful of PDFs embed a non-standard Greek font cmap and extract as
transliterated mojibake (`ΓΗΜΟΚΡΑΣΙΑ` for `ΔΗΜΟΚΡΑΤΙΑ`). Those fail a Greek
sanity check and fall back to the listing text rather than storing garbage.

## Enumeration and the 500-result cap

There is no API. The search portlet is a form POST whose **result listing
already carries every field** (number, year, question, Περίληψη, Διατάξεις,
Λήμματα, president, rapporteur, status), so no detail-page round trip is needed.

The server truncates every query at the first 500 hits
(`ΕΜΦΑΝΙΖΟΝΤΑΙ ΤΑ ΠΡΩΤΑ 500 ΑΠΟΤΕΛΕΣΜΑΤΑ`). Most years before ~2010 exceed
that — 1990 alone runs to opinion no. 400+ while the listing stops at no. 191.
Filtering by ΚΑΤΑΣΤΑΣΗ does not help (legacy years are all one status), so a
capped year is re-walked one `ΑΡΙΘΜΟΣ ΓΝΩΜΟΔΟΤΗΣΗΣ` at a time from the last
fully-listed number until 40 consecutive numbers come back empty.

Completed years are checkpointed to `data/nsk_checkpoint.json`, so a killed or
re-launched run resumes without re-crawling.

## Usage

```bash
# Test connectivity (also reports the 500-cap behaviour)
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all opinions from 1951)
python bootstrap.py bootstrap

# Same, concurrent normalize — the fleet entry point
python bootstrap.py bootstrap-fast

# Update (fetch recent opinions)
python bootstrap.py update
```

## Rate Limits

The scraper is configured for 1 request per second with a burst of 3.
The NSK website uses a Liferay portal and may have session timeouts.

## License

Public domain — official government legal opinions of the Hellenic Republic.
