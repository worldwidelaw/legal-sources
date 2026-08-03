# FR/OrdreMasseursKine

Disciplinary jurisprudence of the **Ordre National des Masseurs-Kinésithérapeutes**
(French National Order of Massage Therapists / Physiotherapists).

- **Source:** https://jurisprudence.ordremk.fr
- **Type:** `case_law`
- **Country:** FR
- **Auth:** none

## Coverage

Anonymised disciplinary decisions published on the Order's dedicated
jurisprudence database (~2092 decisions), including:

- **Chambre disciplinaire nationale** (national appeal chamber)
- **Chambres disciplinaires de première instance** (regional)
- **Sections des assurances sociales** (national and first-instance)
- Reproduced higher-court decisions involving the profession (Conseil d'État,
  Cour de cassation, cours administratives d'appel, etc.)

The chamber/instance is captured from the WordPress category taxonomy and
stored in the `instance` / `chambers` fields.

## How it works

The site is a WordPress install exposing the standard REST API:

1. `/wp-json/wp/v2/posts` — one post per decision. The post title carries the
   decision number(s) and decision date; the post body is empty.
2. `/wp-json/wp/v2/media?parent={post_id}` — the attached **PDF** holding the
   full anonymised decision text (under `wp-content/uploads/`).
3. `/wp-json/wp/v2/categories` — the chamber/instance taxonomy.

Full text is downloaded from the PDF and extracted with `pdfplumber` (per-page
cache flush to bound memory). Records are de-duplicated by post id.

## Usage

```bash
python bootstrap.py bootstrap --sample          # 15 sample records
python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
python bootstrap.py updates --since YYYY-MM-DD   # posts published since a date
```

`updates --since` uses the WP REST API `after=` filter.

## License

[Mentions légales — ONMK](https://www.ordremk.fr/mentions-legales/)
— Public anonymised disciplinary decisions of a body charged with a
public-service mission. The site declares no explicit open-data reuse licence;
these are treated as public jurisprudence (comparable to French court decisions,
which are freely reusable). Attribution to the Order is appropriate.
