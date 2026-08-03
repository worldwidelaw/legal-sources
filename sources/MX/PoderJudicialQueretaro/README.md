# MX/PoderJudicialQueretaro — Querétaro State Court Decisions

Full-text court decisions (*sentencias en versión pública*) from the **Poder
Judicial del Estado de Querétaro**, Mexico, published through its public
"Sentencias Públicas" portal.

- **Portal:** https://poderjudicialqro.gob.mx/APP_UT69ii/sentencias-publicas.php
- **Type:** case_law
- **Coverage:** Civil, Mercantil, Familiar, Penal, Justicia para adolescentes,
  Laboral, Responsabilidad Administrativa, and Constitucional matters. Tens of
  thousands of decisions (27,000+ in a single recent year), each a full-text
  versión pública PDF.
- **Jurisdiction:** MX-QUE (Querétaro)

## How it works

The portal's search form posts to `leeSent.php`, returning an HTML table of
decisions. Although the browser form is wrapped in a reCAPTCHA, the
`leeSent.php` endpoint performs **no server-side captcha validation**, so the
listing is retrievable directly. The scraper iterates year by year
(`fecINI`/`fecFIN`), paginates every result page, and for each row downloads the
full-text PDF from `leeDoc.php?cual=<key>`, extracting the text layer.

## Usage

```bash
python bootstrap.py test                  # connectivity + sample PDF check
python bootstrap.py bootstrap --sample    # fetch a small sample
python bootstrap.py bootstrap --full      # fetch everything
python bootstrap.py bootstrap-fast --full # concurrent full fetch
```

## License

[LIBRE USO MX — Términos de Libre Uso de la Información](https://datos.gob.mx/libreusomx) — attribution required; commercial use permitted.

Public-version court decisions published by a Mexican state judiciary as
transparency obligations (*versiones públicas*). Mexican government public
information is freely reusable under the Términos de Libre Uso de la Información
(LIBRE USO MX).
