# MX/SecretariaFiscalizacion — Secretaría Anticorrupción y Buen Gobierno (ex SFP)

Internal administrative norms (*Normateca Interna*) of Mexico's federal
comptroller / anti-corruption secretariat — the **Secretaría Anticorrupción y
Buen Gobierno**, formerly the **Secretaría de la Función Pública (SFP)**, the
federal Office of the Comptroller (audit / accountability). The corpus is the
secretariat's binding internal provisions: *ACUERDOs*, *manuales de organización
y de procedimientos*, *lineamientos*, and *disposiciones*.

- **Publisher:** Secretaría Anticorrupción y Buen Gobierno (ex SFP)
- **Register:** SANI — Sistema de Administración de Normas Internas de la APF
- **Listing page:** https://normasapf.buengobierno.gob.mx/NORMASAPF/SFP.jsf
- **Data type:** doctrine
- **Auth:** none

## How it works

1. Fetch the SANI register page `SFP.jsf`. Its PrimeFaces datatable embeds every
   norm row — id, name, issuer, emission date — and a per-norm download URL
   (`/NORMASAPF/Descarga?id={id}`) directly in the rendered HTML, so no stateful
   JSF postback is required.
2. Download each norm PDF and extract its full text via `common/pdf_extract.py`.
3. Skip PDFs without a text layer (scanned images, < 500 chars).

The register lists ~232 current internal norms; most carry an extractable text
layer.

## Usage

```bash
python bootstrap.py test                 # connectivity + first-norm check
python bootstrap.py bootstrap --sample   # save sample records
python bootstrap.py bootstrap-fast       # full high-throughput pull (VPS)
```

## License

[Open Government Data](https://normasapf.buengobierno.gob.mx/NORMASAPF/SFP.jsf) —
official internal norms of a Mexican federal secretariat, published for public
consultation through the SANI register under Mexico's transparency framework
(LGTAIP). No explicit open licence is attached; treated as open government data
(commercial use permitted, attribution appreciated).
