# BE/Wallex — Walloon Region Consolidated Legislation

[Wallex](https://wallex.wallonie.be/home.html) is the official consolidated
legislation database of the **Walloon Region** (Belgium), published by the
Service public de Wallonie (SPW) on a Jahia CMS that exposes
[ELI](https://eur-lex.europa.eu/eli) URIs.

It contains the consolidated text of Walloon **decrees** (`décrets`) and
**government orders** (`arrêtés du Gouvernement wallon`), covering regional
competences such as the environment, spatial planning, energy, agriculture,
tourism, employment and local government.

## What this scraper collects

- **Type:** legislation (consolidated)
- **Jurisdiction:** BE-WAL (Walloon Region)
- **Language:** French
- **Full text:** yes — clean structured article text from the ELI pages
  (`article-title` + `article-paragraph`), no PDFs / OCR.

## How it works

1. **Enumerate** acts via the JSON search action
   `POST /sites/wallex/home/search/content-area/rercherce-acte.wallexSearch.do`
   (requires a homepage-primed session + AJAX headers + Referer). It returns
   `acts.results[]` (`path`, `promulgationDate`, `actId`), sorted newest-first
   and **hard-capped at 100 results per query**, so the scraper walks
   `promulgationDateUntil` backwards in time and de-duplicates on `actId`.
2. **Resolve** each result path to its ELI href / title / date via
   `GET /fr{path}.listing.html.ajax`.
3. **Extract** the full consolidated text from the ELI page.

The Wallex server is slow (~10-20 s/request) and throttles bursts, so requests
are rate-limited (0.5 req/s) with retries.

## Usage

```bash
python bootstrap.py test                 # connectivity check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # concurrent full-text downloads
python bootstrap.py update               # incremental (since last run)
```

## License

[Open Data — Service public de Wallonie](https://www.wallonie.be/fr/conditions-generales-dutilisation)
— official Walloon government legislation, open reuse of public-sector
information (attribution requested). Commercial use permitted.
