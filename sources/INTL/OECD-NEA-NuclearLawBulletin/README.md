# INTL/OECD-NEA-NuclearLawBulletin — OECD/NEA Nuclear Law Bulletin

The **Nuclear Law Bulletin** (ISSN 1609-7378) is the OECD Nuclear Energy
Agency's international nuclear-law journal, published free online twice a year
in English and French. Each issue carries topical articles by legal experts,
national legislative and regulatory digests, case-law notes, and updates on
international nuclear-law instruments and organisations. This is `doctrine`.

## Data access

Issues are enumerated from the NLB landing page
(`/jcms/pl_21586/nuclear-law-bulletin-nlb`) and the Archive page
(`/jcms/pl_77708/nuclear-law-bulletin-archive`, Nos. 1–94). Both carry
per-issue pages (`.../nuclear-law-bulletin-no-N-volume-YYYY/H`); each issue page
links to the free full-text PDF under `/upload/docs/application/pdf/...`. Full
text is extracted from the PDF with the shared `common.pdf_extract` backend.

- ~102 English issues (Nos. 1–114), each a complete free-to-read bulletin.
- No authentication, no CAPTCHA.
- The parallel French series (Bulletin de droit nucléaire) is not ingested here.

## Usage

```bash
python bootstrap.py test                # verify listing + one PDF download
python bootstrap.py bootstrap --sample  # fetch 15 sample records
python bootstrap.py bootstrap           # full run
```

## License

> ⚠️ **Commercial use restricted.** Free to read; commercial redistribution needs OECD permission.

[OECD Terms and Conditions](https://www.oecd.org/termsandconditions/) — the Nuclear Law Bulletin is published free-to-read by the OECD Nuclear Energy Agency. Personal and non-commercial reproduction is permitted with attribution.
