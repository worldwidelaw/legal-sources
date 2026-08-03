# MX/PoderJudicialCampeche — Campeche State Court Decisions

Full-text court decisions (*versiones públicas de resoluciones jurisdiccionales y
precedentes*) from the **Poder Judicial del Estado de Campeche**, Mexico,
published through its public "Sistema de Versiones Públicas" portal.

- **Portal:** https://tribunalvirtual.poderjudicialcampeche.gob.mx/precedentes/precedentes/publico/
- **Type:** case_law
- **Coverage:** ~28,000 decisions across Familiar, Mercantil, Penal, Oral Penal,
  Oral Familiar, and other matters, each a full-text versión pública PDF.
- **Jurisdiction:** MX-CAM (Campeche)

## How it works

The portal is a Django + DataTables application. A server-side DataTables handler
(`tabla-sentencias-handler-publico-renewed`) returns paginated rows as JSON; the
last cell of each row is the `id_resolucion`. The full-text PDF for a decision is
served at `/precedentes/precedentes/sentencias/sentenciapublica/<id>/`. The
scraper paginates through all ~28,000 rows, downloads each PDF, and extracts the
text layer.

CSRF: Django sets a `csrftoken` cookie on the listing page; the scraper sends it
back as both the `csrfmiddlewaretoken` POST field and the `X-CSRFToken` header.

Note: some source rows carry a scrambled `fecha` year (e.g. `7202-10-30`); the
scraper coerces dates with an implausible year to `null` while keeping the full
text intact.

## Usage

```bash
python bootstrap.py test                  # connectivity + sample PDF check
python bootstrap.py bootstrap --sample    # fetch a small sample
python bootstrap.py bootstrap --full      # fetch everything
python bootstrap.py bootstrap-fast --full # streaming full fetch
```

## License

[LIBRE USO MX — Términos de Libre Uso de la Información](https://datos.gob.mx/libreusomx) — attribution required; commercial use permitted.

Public-version court decisions published by a Mexican state judiciary as
transparency obligations (*versiones públicas*). Mexican government public
information is freely reusable under the Términos de Libre Uso de la Información
(LIBRE USO MX).
