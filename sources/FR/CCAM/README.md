# FR/CCAM — CCAM en ligne (Classification Commune des Actes Médicaux)

Billing rules and nomenclature of the French **Classification Commune des Actes
Médicaux (CCAM)**, published by *l'Assurance Maladie* (Caisse nationale de
l'Assurance Maladie, CNAM) on the "CCAM en ligne" portal
(<https://www.ameli.fr/accueil-de-la-ccam/>).

The CCAM is the binding classification used to **code, price and bill medical
procedures** under French statutory health insurance.

## What this source collects

The portal's `règles-de-facturation/*` and `telechargement/*` pages are thin
navigation stubs; the real legal content is distributed as PDFs under
`/fileadmin/user_upload/documents/*.pdf`. The scraper crawls every CCAM
sub-page, collects the distinct PDF documents (~19), and extracts their full
text via `common.pdf_extract` (OOM-hardened pdfplumber).

Documents include:

- **Liste des actes et des prestations** (dispositions générales / diverses) —
  the consolidated billing rules → `legislation`
- **Per-version nomenclature** `CCAM_V*.pdf` (act codes, libellés, tariffs;
  large and partly tabular) → `legislation`
- **Définitions, contextes et principes**, **CAMNOTE** release notes,
  methodology fiches, and topic fiches (radiotherapy, ACP, radiology
  associations, feuille de soins, principes généraux, aide-mémoire…) →
  `doctrine`

## Implementation notes

- The TYPO3 site soft-errors (HTTP 500) on some sub-pages but still returns the
  fully rendered body, so the scraper reads the body regardless of status code.
- Page charset is **ISO-8859-1**.
- `date` is a best-effort parse of the PDF filename and is not a required field.

## Usage

```bash
python bootstrap.py test               # connectivity + first-doc check
python bootstrap.py bootstrap --sample # sample records
python bootstrap.py bootstrap --full   # full pull
```

## License

[Licence Ouverte 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) —
official texts of a French public-service body (CNAM / l'Assurance Maladie);
commercial use permitted, attribution required.
