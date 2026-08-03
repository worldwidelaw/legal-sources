# FR/AssuranceMaladie — Circulaires réseau de l'Assurance Maladie (CNAM)

Official regulatory circulars (« directives réseau » / « directives extranet ») of
the French statutory health-insurance fund (**Caisse nationale de l'Assurance
Maladie**, CNAM), published at <https://circulaires.ameli.fr/>.

These circulaires instruct the CPAM / CGSS / CARSAT / CRAMIF network on how to
apply social-security and health-insurance law: gestion du risque, prestations,
prévention des risques professionnels (AT/MP), relations avec les professionnels
de santé, etc. There are roughly **3,060 circulaires spanning 1999–present**,
each with a full-text PDF.

## Data type

`doctrine` — binding administrative directives/circulaires interpreting and
applying social-security law for the assurance-maladie network.

## Access method

Drupal site, no auth:

1. Enumerate `/directives?page=N` (numeric pager, ~153 pages × 20) for every
   `/circulaire/{slug}` link.
2. Fetch each detail page, parse structured metadata (date, objet, résumé,
   domaine, plan de classement, mots clés) and the PDF URL.
3. Download the PDF and extract its full text (`common.pdf_extract`, OOM-hardened
   pdfplumber backend).

The Drupal `?_format=json` endpoint is WAF-rejected (`Request Rejected`), so the
HTML detail page + PDF extraction is the supported path.

## Usage

```bash
python bootstrap.py test               # connectivity + first doc
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap-fast     # full pull (VPS)
```

## License

[Licence Ouverte 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) —
official texts of a French public-service body (CNAM); commercial use permitted,
attribution required.
