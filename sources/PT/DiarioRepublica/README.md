# PT/DiarioRepublica - Portuguese Official Journal

## Overview

Fetches Portuguese legislation from the Diário da República, Portugal's official
gazette where all laws, decrees, and official acts are published.

## Data Source

- **Official Site**: https://diariodarepublica.pt
- **Data Mirror**: https://dre.tretas.org (community archive with structured access)
- **ELI Base**: https://data.dre.pt/eli/

The official diariodarepublica.pt uses a JavaScript-heavy frontend (OutSystems)
that's not suitable for programmatic access. This fetcher uses dre.tretas.org,
a community-maintained mirror that provides:

- Faithful copy of all official gazette content
- Clean HTML pages with full document text
- JSON-LD structured metadata
- RSS feeds for recent documents
- Browse-by-date functionality

## Data Access Methods

### RSS Feed
```
https://dre.tretas.org/dre/rss/
```
Returns recent documents in RSS 2.0 format with:
- Title
- Description/summary
- Publication date
- Document ID
- Issuing entity

### Browse by Date
```
https://dre.tretas.org/dre/data/{yyyy}/{m}/{d}/
```
Lists all documents published on a specific date.

### Document Pages
```
https://dre.tretas.org/dre/{id}/{slug}
```
Full HTML page containing:
- Complete document text in `<div itemprop="articleBody">`
- Structured metadata (Schema.org markup)
- Cross-references to related legislation

### JSON-LD Metadata
```
https://dre.tretas.org/dre/{id}.jsonld
```
Returns Schema.org/Legislation structured data:
```json
{
  "@context": "https://schema.org/",
  "@type": "Legislation",
  "name": "Decreto-Lei 34/2024, de 17 de Maio",
  "legislationType": "Decreto-Lei",
  "legislationDate": "2024-05-17",
  "abstract": "...",
  "encoding": [...]
}
```

## Coverage

- **Series I** (legislation): From 1910 to present
- **Series II** (administrative acts): From 1990 to present
- **Updates**: Daily, synchronized with official gazette

## Document Types

- Lei (Law)
- Decreto-Lei (Decree-Law)
- Decreto (Decree)
- Portaria (Ordinance)
- Resolução (Resolution)
- Despacho (Order)
- And many more...

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents from 2020+)
python bootstrap.py bootstrap

# Incremental update (recent RSS items)
python bootstrap.py update
```

## Output Schema

```json
{
  "_id": "5752809",
  "_source": "PT/DiarioRepublica",
  "_type": "legislation",
  "_fetched_at": "2026-02-10T19:00:00+00:00",
  "title": "Decreto-Lei 34/2024, de 17 de Maio",
  "text": "A regulação do sistema de depósito...",
  "date": "2024-05-17",
  "url": "https://dre.tretas.org/dre/5752809/decreto-lei-34-2024-de-17-de-maio",
  "document_type": "Decreto-Lei",
  "creator": "Presidência do Conselho de Ministros",
  "summary": "Altera o regime de licenciamento...",
  "eli_uri": "https://data.dre.pt/eli/dec-lei/34/2024/05/17/p/dre/pt/html",
  "language": "pt"
}
```

## License

[Public Domain](https://www.law.cornell.edu/uscode/text/17/105) — Diário da República content is *domínio público* (official state publications) under Portuguese law (Código do Direito de Autor, art. 7, which excludes official state texts from copyright). Commercial use permitted.

Sourced via the open [legalize-dev/legalize-pt](https://github.com/legalize-dev/legalize-pt) GitHub mirror. The mirror's generation pipeline is MIT-licensed; the legislative content it reproduces is public-domain state publication. Each file records the original official `source` URL (dre.pt) in its YAML front matter.

## Data Source (2026-07 rewrite)

The former `dre.tretas.org` mirror is now behind a Cloudflare challenge (HTTP 403 to all non-browser clients, including residential vantages), and the official `diariodarepublica.pt` is a locked OutSystems SPA. This source now reads the ~90,000-act Série I corpus from `legalize-dev/legalize-pt`, which is regenerated daily from the official OutSystems API and published as clean Markdown with rich YAML front matter.

## Notes

- Full pull streams the repo tarball (single bulk download < 2 GB, no git history) and iterates `pt/*.md`; each file embeds full born-digital text.
- Sample mode fetches individual files via `raw.githubusercontent.com`; incremental updates use the GitHub commits API.
- For legal purposes, always verify against the official source (dre.pt link in each record's `url`).
