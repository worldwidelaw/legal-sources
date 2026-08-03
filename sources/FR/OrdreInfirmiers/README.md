# FR/OrdreInfirmiers

Disciplinary jurisprudence of the **Ordre National des Infirmiers**
(French National Order of Nurses).

- **Source:** https://www.ordre-infirmiers.fr/jurisprudence-0
- **Type:** `case_law`
- **Country:** FR
- **Auth:** none

## Coverage

Anonymised disciplinary decisions of the **Chambre disciplinaire nationale**
(national disciplinary chamber of the Order), published on the Order's public
jurisprudence database. Decisions are grouped into yearly pages
(`/decisions-de-l-annee-{YYYY}`), currently covering 2022–2026 (91 unique
decisions discovered).

## How it works

1. The jurisprudence index links to one page per year
   (`/decisions-de-l-annee-{YYYY}`).
2. Each yearly page lists decisions as anchors linking to the **full anonymised
   decision PDF** under `/system/files/inline-files/`. The anchor text carries
   the decision number(s) and decision date.
3. Full text is downloaded from that PDF and extracted with `pdfplumber`
   (per-page cache flush to bound memory). Records are de-duplicated by PDF URL.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full bootstrap -> data/records.jsonl
python bootstrap.py bootstrap-fast       # VPS wrapper alias for full
```

`updates --since` is not supported (the portal exposes no date-filtered API).

## License

[Mentions légales — ONI](https://www.ordre-infirmiers.fr/mentions-legales)
— Public anonymised disciplinary decisions of a body charged with a
public-service mission. The site declares no explicit open-data reuse licence;
these are treated as public jurisprudence (comparable to French court decisions,
which are freely reusable). Attribution to the Order is appropriate.
