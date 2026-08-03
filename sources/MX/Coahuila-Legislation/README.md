# MX/Coahuila-Legislation

Consolidated, currently-in-force legislation of the **State of Coahuila de
Zaragoza**, Mexico (ISO 3166-2: **MX-COA**), published by the Congreso del
Estado on its "Leyes Estatales Vigentes" page.

## Coverage

- ~247 documents: the state constitution, codes (Civil, Fiscal, Penal,
  Municipal, Electoral, etc.) and individual laws currently in force.
- Each document is a consolidated PDF reflecting the latest reform.
- Full text in Spanish, extracted with pdfplumber.

## Source

- Catalog: https://www.congresocoahuila.gob.mx/portal/leyes-estatales-vigentes/
- Documents: `https://www.congresocoahuila.gob.mx/transparencia/03/Leyes_Coahuila/coa*.pdf`

## Strategy

1. Scrape the catalog page. Each table row holds a law title and a link to its
   consolidated PDF.
2. Download each PDF with a browser-like User-Agent (the server 403s the
   default requests UA) and extract full text.
3. Derive the publication / last-reform date from the document header
   ("ÚLTIMA REFORMA PUBLICADA EN EL PERIODICO OFICIAL: DD DE MES DE YYYY").

## Usage

```bash
python bootstrap.py test                # connectivity + first-PDF check
python bootstrap.py bootstrap --sample  # 12 samples -> sample/
python bootstrap.py bootstrap           # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast      # VPS alias for full pull
```

## Notes

- A handful of catalog links (e.g. `coa298.pdf`) 404 upstream; these are
  skipped gracefully.
- Foreign/datacenter IPs may be geo-throttled (Mexico-only); works locally and
  may need a Mexico region proxy on the VPS.

## License

[Public Domain — official legal text](https://www.congresocoahuila.gob.mx/portal/leyes-estatales-vigentes/) — Mexican legislation (laws, decrees and official texts) is not subject to copyright under art. 14 fracc. I of the Ley Federal del Derecho de Autor. Commercial use permitted; no attribution required.
