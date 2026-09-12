# GN/GuineeLex — Le droit guinéen en vigueur

Consolidated Guinean legislation, article by article: the Constitution, the
codes, ordinary and organic laws, ordonnances, décrets and arrêtés, ministerial
décisions, ratified treaties, and the OHADA uniform acts as applied in Guinea.

- **Site:** https://guineelex.com
- **Methodology / corpus transparency:** https://guineelex.com/methodologie
- **Language:** French
- **Type:** legislation
- **Auth:** none

## Why this source

Guinea is a data-poor jurisdiction and the existing GN sources are all partial:
`GN/JournalOfficiel` carries gazette issues as published (chronological, not
consolidated), `GN/CourSupremeLegislation` has 89 legislative posts, and
`GN/BCRG-Regulations` 15 banking texts. GuinéeLex is the only route we have to
Guinean law in **consolidated, article-level** form — 90,446 articles in force
across 4,043 texts, with the amendment history kept alongside.

It overlaps `GN/DroitGuineen` (a different private aggregator, 4,931 documents)
at the level of *which* texts are covered, but not in shape: DroitGuineen serves
whole documents, GuinéeLex serves them split into articles with verified
cross-references between texts.

## Access route

The site is a statically rendered Astro build, so there is no API to reverse and
no JavaScript to execute — every document page ships its complete text in the
HTML.

1. `GET /sitemap-index.xml` → nine per-category sitemaps (5,620 document pages).
2. `GET /{category}/{slug}` → the document.
   - Body text is in `<article data-pagefind-body>`, the element the site's own
     search index is built from — i.e. exactly the document and none of the
     chrome.
   - Metadata comes from a schema.org **`Legislation`** JSON-LD block:
     `legislationIdentifier`, `legislationDate`, `legislationType`,
     `legislationPassedBy`, `legislationLegalForce`. Structured markup, not
     prose scraping.

`robots.txt` allows every crawler, explicitly including AI agents. The crawl
runs at 2 req/s.

## Documentary stubs are dropped

The corpus deliberately retains **"référence documentaire"** pages: texts that
are cited by other documents but whose full text has not been transcribed. They
render a placeholder paragraph ("son texte intégral et son statut juridique
n'ont pas encore été vérifiés") instead of articles.

These are metadata-only and are **dropped**, not emitted as records. The tell is
structural rather than a phrase match: a real document has one `article-head`
heading per article, a stub has none.

## Categories

| Category | Pages | Note |
|---|---|---|
| `decrets-arretes` | 4,506 | décrets and arrêtés |
| `lois` | 721 | ordinary laws |
| `ordonnances` | 239 | |
| `codes` | 59 | consolidated codes |
| `decisions` | 33 | ministerial/regulatory decisions — general acts, so `legislation`, not `case_law` |
| `lois-organiques` | 31 | |
| `ohada` | 17 | uniform acts |
| `traites` | 13 | ratified international instruments |
| `constitution` | 1 | |

Categories are crawled round-robin rather than end to end: 80% of the corpus is
décrets/arrêtés, so in sitemap order a crawl that stops early — or a 15-record
sample — would see one document type and nothing else.

## Incremental updates

The sitemaps carry no `<lastmod>`, so there is no server-side "changed since"
signal. `fetch_updates` compares **availability** instead: a URL absent from the
last crawl is new to us (`data/seen_urls.json`). Consolidated texts are amended
in place under a stable URL, so `constitution`, `codes`, `lois-organiques` and
`ohada` are always re-read and the loader dedups them when nothing moved.

⚠️ A revision to an already-seen *dated* act (a décret, a law) is only picked up
by a full re-crawl. Those texts are not normally edited after publication, but
the limitation is real.

## Record schema

| Field | Description |
|---|---|
| `_id` | `GN/GuineeLex/{category}/{slug}` |
| `title` | Document title from JSON-LD `name` |
| `text` | Full text, article by article |
| `date` | `legislationDate` (ISO 8601), null where the site has none |
| `url` | Canonical document URL |
| `identifier` | Official reference, e.g. `L/2016/059/AN` |
| `legislation_type` | `Code`, `Loi`, `Décret`, `Acte uniforme`, … |
| `passed_by` | Enacting body, e.g. Assemblée Nationale de Guinée |
| `in_force` | Whether `legislationLegalForce` is `InForce` |
| `article_count` | Number of articles on the page |
| `category` / `category_label` | Site section |

## Failure modes guarded

- **Truncated enumeration** — if the sitemaps return fewer than 3,000 documents
  the run raises instead of quietly ingesting a partial corpus.

## Testing

```bash
python bootstrap.py test              # enumerate + normalize one document
python bootstrap.py bootstrap --sample
python bootstrap.py update
```

## License

[Guinean official legal texts — no re-use terms published](https://guineelex.com/methodologie)
— the documents are official acts of the Republic of Guinea (Constitution,
codes, laws, decrees, OHADA uniform acts), reproduced from the Journal Officiel
and the Secrétariat Général du Gouvernement. Official acts of state are not
protected by copyright.

GuinéeLex is a **private publisher**, not a government site. It publishes no
terms of use, no CGU and no copyright notice — only a privacy policy covering
its AI assistant — and its `robots.txt` explicitly allows every crawler,
including AI agents. Commercial use of the underlying legal texts is therefore
treated as permitted. Publisher-added compilation metadata (cross-references,
status flags) is not re-used as such. Only the version published in the *Journal
Officiel de la République de Guinée* is authoritative.
