# FR/OrdreChirurgiensDentistes

Disciplinary jurisprudence of the **Ordre National des Chirurgiens-Dentistes**
(French National Order of Dental Surgeons).

- **Source:** https://www.ordre-chirurgiens-dentistes.fr/decisions/
- **Type:** `case_law`
- **Country:** FR
- **Auth:** none

## Coverage

Anonymised disciplinary decisions published by the Order, including:

- **Chambre disciplinaire nationale** (national appeal chamber, presided by a
  member of the Conseil d'État)
- **Chambres disciplinaires de première instance** (regional)
- **Section des assurances sociales** du Conseil national

Decisions are organised into 45 thematic categories (e.g. *Compétence*,
*Secret médical*, *Exercice illégal*, *Soins – mutilations*). The same decision
can appear under several themes; records are de-duplicated by dossier number and
the themes are merged into the `keywords` field.

## How it works

The decisions portal is a plain PHP site:

1. The index page lists all themes as `decision.php?categorie={theme}` links.
2. Each category page returns decision "cards" with the dossier number, decision
   date, instance type, and a relative link to the **full anonymised decision
   PDF** under `decisions/Fichiers/{name}.pdf`.
3. Full text is downloaded from that PDF and extracted with `pdfplumber`.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full bootstrap -> data/records.jsonl
python bootstrap.py bootstrap-fast       # VPS wrapper alias for full
```

`updates --since` is not supported (the portal exposes no date-filtered API).

## License

[Informations légales — ONCD](https://www.ordre-chirurgiens-dentistes.fr/informations-legales/)
— Public anonymised disciplinary decisions of a private body charged with a
public-service mission. The site declares no explicit open-data reuse licence;
these are treated as public jurisprudence (comparable to French court decisions,
which are freely reusable). Attribution to the Order is appropriate.
