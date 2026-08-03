# US/IA-AGOpinions — Iowa Attorney General Opinions

Full text of the Iowa Attorney General's opinions — the AG's authoritative
interpretations of Iowa law, issued at the request of state officers and
county attorneys. The Iowa Attorney General's own site routes opinion full
text through Westlaw (paywalled), but the **Iowa Legislature** republishes
the opinions openly as born-digital **annual compilation volumes**. These are
official state government legal interpretations — classified as **doctrine**.

## Source

- **Publisher:** Iowa Legislature (republishing Iowa Dept. of Justice / AG opinions)
- **Site:** https://www.legis.iowa.gov/publications/attorneyGeneralOpinions
- **Coverage:** ~98 annual volumes, 1896–present (Opinions / Report / Unpublished Opinions)
- **Auth:** none

## How it works

1. The listing page is one server-rendered HTML table; each row links an
   annual volume PDF at `/docs/publications/AGO/{id}.pdf` with its name and
   year.
2. `normalize()` downloads each volume PDF and extracts full text with
   `fitz`/PyMuPDF (Tesseract OCR fallback for the rare image-only scan). Even
   the 800+ page mid-century volumes carry a text layer.
3. **One record = one annual volume.** Individual opinions within a volume
   are delimited only by inline `#YY-N-N` syllabus markers (not by document
   boundaries), so the volume is the granular unit the state publishes.
   Records are keyed by the unique numeric doc id, so a year's "Opinions" and
   "Unpublished Opinions" volumes are kept distinct.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample volumes
python bootstrap.py bootstrap            # Full pull (all volumes)
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain — 17 U.S.C. § 105 / Iowa state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Iowa Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
