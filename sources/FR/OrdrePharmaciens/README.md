# FR/OrdrePharmaciens

Disciplinary jurisprudence of the **Conseil National de l'Ordre des Pharmaciens**
(CNOP), the French professional order of pharmacists.

## Source

- Consolidated database: https://www.ordre.pharmacien.fr/l-ordre/jurisprudence
- Decision PDFs: https://www.ordre.pharmacien.fr/mediatheque/fichiers/jurisprudence/

## What it covers

Anonymised disciplinary decisions across all ordinal jurisdictions:

- **Chambres de discipline de première instance** (central / regional councils)
- **Chambre de discipline du Conseil national** (appeal)
- **Sections des assurances sociales**
- Reproduced **Conseil d'État** cassation decisions involving the profession

## How it works

The jurisprudence listing is a Symfony POST-search form (`name="jurisprudence"`)
protected by a CSRF token. Submitting an **empty keyword** returns the full
corpus, 15 decisions per page, paginated by appending `?page=N` to the POST URL
(paging past the last page returns HTTP 404, which signals the end).

Each result links to a detail page `/jurisprudence/{id}-{slug}` that carries:

- a structured **summary** (résumé), the relevant **CSP code articles**, the
  **keywords**, and a **"Chronologie des décisions"** table (decision date,
  plaignant, jurisdiction level);
- one or more links to the **full anonymised decision PDF(s)** under
  `/mediatheque/fichiers/jurisprudence/decision.pdfNNN`.

`bootstrap.py` pages through the search, visits each detail page, downloads every
decision PDF and extracts the **full text** with pdfplumber (per-page cache flush
to bound memory). The summary, articles and keywords are retained as metadata.
Records are de-duplicated by jurisprudence page id (~100 decisions).

## Usage

```bash
python bootstrap.py bootstrap --sample          # 15 sample records
python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
python bootstrap.py updates --since 2020-01-01   # decisions dated since a date
```

## License

[Custom terms — public anonymised jurisprudence](https://www.ordre.pharmacien.fr/mentions-legales) —
public anonymised disciplinary decisions of an ordinal body with a public
service mission. No explicit open-data reuse licence is declared; the corpus is
treated as public jurisprudence, comparable to published French court decisions.
Attribution to the CNOP is appropriate.
