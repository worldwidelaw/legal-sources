# FR/BulletinsOfficielsSociaux

**Bulletins officiels des ministères chargés des affaires sociales**
(Santé – Protection sociale – Solidarités & Travail – Emploi – Formation professionnelle)

- **URL:** https://bulletins-officiels.social.gouv.fr/
- **Country:** France (FR)
- **Data types:** legislation, doctrine
- **Auth:** none

## What this source covers

The official bulletins of the French social-affairs ministries. The single site
publishes two bulletins:

- **BO Santé – Protection sociale – Solidarités**
- **BO Travail – Emploi – Formation professionnelle**

Published acts include circulaires, instructions, arrêtés, décisions, notes
d'information, avis, conventions and délibérations. Binding regulatory acts
(arrêtés, décrets, décisions…) are tagged `legislation`; explanatory/instructional
texts (circulaires, instructions, notes, guides…) are tagged `doctrine`.

## How it works

The site is a Drupal install that exposes each node as JSON via the
`?_format=json` query parameter. The scraper:

1. Reads `/sitemap.xml` to enumerate every node path.
2. Fetches `<url>?_format=json` for each.
3. Keeps only real legal documents (nodes carrying a `field_institutional`
   document-type taxonomy term); utility/section pages are skipped.
4. Reads the full text from `field_body_text` (HTML stripped) and derives the
   date, document type, NOR reference and issuer from structured fields.

No PDF extraction is required for the binding body — annexes are linked PDFs but
the act text itself is in `field_body_text`.

Covers public-repo source requests **#1036** (Ministères chargés des affaires
sociales) and **#1037** (Ministère de la Santé — BO Santé).

## Usage

```bash
python bootstrap.py test               # connectivity test
python bootstrap.py bootstrap --sample # fetch sample records
python bootstrap.py bootstrap-fast     # full pull (VPS)
python bootstrap.py update             # re-scan sitemap (idempotent via Neon)
```

## License

[Licence Ouverte 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — French open licence. Commercial use permitted; attribution required.
