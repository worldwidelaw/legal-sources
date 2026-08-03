# FR/OrdrePedicuresPodologues

Disciplinary jurisprudence of the **Ordre National des Pédicures-Podologues**
(ONPP), the French professional order of chiropodists/podiatrists.

## Source

- Consolidated database: https://www.onpp.fr/juridique/jurisprudence/
- Decision PDFs: https://www.onpp.fr/assets/jurisprudence/

## What it covers

Anonymised disciplinary decisions across all ordinal jurisdictions:

- **Chambres disciplinaires de première instance** (regional / inter-regional)
- **Chambre disciplinaire nationale** (appeal)
- **Sections des assurances sociales**
- Reproduced **Conseil d'État** cassation decisions involving the profession

## How it works

The jurisprudence page embeds, in an inline `<script>` block, a JavaScript
`const data = [ ... ]` array with one object per decision:

```js
{
  date: '29-04-2026',
  titre: 'CDPI 29 avril 2026 n°2025-03',
  region: 'Pays de la Loire',
  link: 'CDPI_PDL_29042026_C.pdf',
  juridiction: 'Chambre disciplinaire de première instance',
  abstract: '...',
  keywords: [ 'Hygiène et sécurité', 'Qualité des soins' ],
}
```

`bootstrap.py` downloads the page, parses that array, then fetches each decision
PDF from `/assets/jurisprudence/{link}` and extracts the **full text** with
pdfplumber (per-page cache flush to bound memory). The `abstract` and `keywords`
are retained as metadata. Records are de-duplicated by PDF link (~265 decisions).

## Usage

```bash
python bootstrap.py bootstrap --sample          # 15 sample records
python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
python bootstrap.py updates --since 2025-01-01   # decisions dated since a date
```

## License

[Custom terms — public anonymised jurisprudence](https://www.onpp.fr/mentions-legales.html) —
public anonymised disciplinary decisions of an ordinal body with a public
service mission. No explicit open-data reuse licence is declared; the corpus is
treated as public jurisprudence, comparable to published French court decisions.
Attribution to the ONPP is appropriate.
