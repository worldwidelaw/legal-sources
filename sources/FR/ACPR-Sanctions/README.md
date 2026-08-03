# FR/ACPR-Sanctions — ACPR Commission des sanctions (Décisions)

Full-text decisions of the **Commission des sanctions** of the **Autorité de
contrôle prudentiel et de résolution (ACPR)** — the French prudential supervisor
for the banking and insurance sectors (an independent administrative authority
backed by the Banque de France, *Code monétaire et financier*, art. L.612-1
et seq.).

The Commission des sanctions is an independent adjudicatory body. After
adversarial proceedings it imposes disciplinary sanctions — *blâme*,
*avertissement*, *sanction pécuniaire*, *retrait d'agrément*, *radiation* — on
supervised entities (credit institutions, insurers, payment / e-money
institutions, insurance and banking intermediaries) for breaches of their
prudential, anti-money-laundering / counter-terrorist-financing (LCB-FT),
governance and customer-protection obligations. Each decision is a reasoned
adjudication of a specific case = **case_law**.

## Coverage

- **~110 decisions**, from 2010 (creation of the ACP, later ACPR) to present.
- Banking, insurance, payment / e-money institutions and intermediaries.
- Companion to `FR/AMF_Sanctions` (the AMF markets-regulator sanctions).

## Access & method

- The **"Recueil des sanctions"** page
  (`/fr/reglementation/recueil-des-sanctions`) is a single server-rendered
  listing linking every decision to a publication page under
  `/fr/publications-et-statistiques/publications/decision-de-la-commission-des-sanctions-...`.
- Each publication page carries a *"Télécharger le document"* paragraph with the
  born-digital decision PDF under `/system/files/...` (the href is rendered with
  spaces around `=` by the Drupal/Twig template).
- Download each PDF and extract full text with **PyMuPDF** (pdfplumber / pypdf
  fallback). No OCR needed.
- Decision number (`n° YYYY-NN`) and decision date are parsed from the page
  title, the PDF body (`Décision rendue le ...`) and the file path.

A small number of very old publication pages carry no attached PDF; those are
skipped.

## Fields

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text`
(full decision), `date`, `url`, `pdf_url`, `decision_number`, `court`,
`jurisdiction` (`FR`), `language` (`fr`).

## License

[Licence Ouverte / Open Licence 2.0 (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free re-use including commercial use; attribution to the ACPR required.

Decisions of the ACPR Commission des sanctions are official acts of a French
independent administrative authority (public information). Like the sibling
`FR/AMF_Sanctions` corpus, they fall under the French open public-information
regime (Licence Ouverte / Etalab 2.0). Public access, no login/paywall.
Decisions are anonymised by the ACPR where required.
