# INTL/ADNDRC — Asian Domain Name Dispute Resolution Centre (UDRP Decisions)

Full-text panel decisions of the **Asian Domain Name Dispute Resolution Centre
(ADNDRC)**, an ICANN-accredited UDRP dispute-resolution provider. ADNDRC
operates through four offices, reflected in the case-number prefixes:

| Office | Prefix | Operator |
|--------|--------|----------|
| Beijing | `CN` | CIETAC |
| Hong Kong | `HK` | HKIAC |
| Seoul | `KR` | KIDRC |
| Kuala Lumpur | `KL` / `KLRCA` / `AIAC` | AIAC |

It is the third-largest ICANN UDRP provider (after WIPO and Forum/NAF) and the
principal provider for `.asia` and many Asian ccTLD disputes.

## Data

- **Type:** `case_law` (administrative panel decisions)
- **Coverage:** ~2,700 decisions, 2002–present
- **Full text:** yes — extracted from the official decision PDFs (typically
  7K–40K chars each: parties, procedural history, factual background, parties'
  contentions, panel findings on the three UDRP elements, and the decision).
- **Language:** mostly English; some bilingual (Chinese/English, Korean/English).

## How it works

The single index page `https://www.adndrc.org/decisions/udrp` server-renders
four HTML tables (one per office) listing every decision with case number,
complainant + domicile, respondent + domicile, disputed domain name(s), outcome,
and a link to the decision PDF. Two historical URL layouts coexist:

- `/storage/files/udrp/{OFFICE}/{ID}_Decision.pdf` (older)
- `/storage/uploads/decisions/udrp/udrp_{TIMESTAMP}.pdf` (newer)

`bootstrap.py` parses both, downloads each PDF (a browser `User-Agent` **and**
`Referer` header are required — the server drops the connection otherwise) and
extracts the text via the shared `common.pdf_extract` pipeline. The decision
date is taken from the PDF body ("Date of Decision DD-MM-YYYY" / "Dated: D Month
YYYY"), falling back to the year encoded in the case number.

A minority of older KL/HK decisions are scanned image-only PDFs; these yield no
text layer and are skipped (would need OCR).

## Usage

```bash
python bootstrap.py test                 # connectivity test
python bootstrap.py bootstrap --sample   # 15 sample records -> sample/
python bootstrap.py bootstrap            # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # pipeline alias (full pull)
```

## License

[Custom terms — publicly published decisions](https://www.adndrc.org/) —
ADNDRC panel decisions are public records published openly on adndrc.org
pursuant to the UDRP Rules (paragraph 4(j) requires publication of the full
decision). No explicit reuse licence is asserted; treated as publicly published
decisions, consistent with the project's other UDRP sources
(`INTL/CAC-UDRP`, `INTL/Forum-DomainDecisions`). Commercial use: permitted.
