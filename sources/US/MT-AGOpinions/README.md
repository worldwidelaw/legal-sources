# US/MT-AGOpinions — Montana Attorney General Opinions

Full text of formal opinions issued by the **Montana Attorney General**. Each
opinion answers a legal question posed by a public official and is an
authoritative interpretation of Montana law — classified as **doctrine**.

## Source

- **Index:** https://courts.mt.gov/library/mr/agopinions/
- **PDF host:** `https://courts.mt.gov/external/ag-opinions/{vol}/{num}.pdf`
- **Publisher:** Montana Judicial Branch — State Law Library.

## Strategy

The State Law Library hosts the historical **Opinions of the Attorney General**
(Volumes 1–44, covering **1899–1992**) as clean, text-layer PDFs. The index is
two shallow levels:

1. `GET /library/mr/agopinions/` → links to each volume page (`vol{N}`).
2. `GET /library/mr/agopinions/vol{N}` → one HTML `<table>` of opinions; each
   row is `[opinion no. (links the PDF), "Held" summary, date]`.
3. Download each `/external/ag-opinions/{vol}/{num}.pdf` and extract its text
   layer via `common.pdf_extract` (no OCR needed).
4. Normalize into the standard doctrine schema (`text` = PDF body).

No pagination beyond the volume list, no JavaScript, no CAPTCHA.

> **Note:** Opinions from **1993–present** are published on `dojmt.gov`, which
> is WAF/Cloudflare-gated (HTTP 403 to non-browser clients). Only the historical
> 1899–1992 corpus on `courts.mt.gov` is built here; the recent set needs
> browser automation / a VPS pass.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all volumes)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Coverage

- Volumes 1–44 (1899–1992), ~145 opinions per volume.
- Jurisdiction: US-MT (State of Montana).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Montana Attorney General opinions are official state government works in the
public domain. Commercial use permitted; no attribution required.
