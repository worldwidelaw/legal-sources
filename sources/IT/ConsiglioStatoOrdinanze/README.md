# IT/ConsiglioStatoOrdinanze — Council of State: Ordinanze & Decreti

**Source:** [giustizia-amministrativa.it](https://www.giustizia-amministrativa.it) (OpenGA open data)
**Data type:** case_law
**Language:** Italian

Interlocutory administrative decisions of the Italian **Consiglio di Stato**
(Council of State) — **ordinanze** (orders, incl. ordinanze cautelari / interim
relief) and **decreti** (monocratic decrees). This is the companion to
[`IT/ConsiglioDiStato`](../ConsiglioDiStato/) which covers *sentenze* (final
judgments). The two corpora do not overlap.

## How it works

1. Query the OpenGA CKAN datastore (`cds-ordinanze`, `cds-decreti`) for
   per-year JSON metadata, paginated.
2. Build the mdp full-text URL from `schema=cds`, `NUMERO_RICORSO` and
   `NUMERO_PROVVEDIMENTO` with the document-model suffix (ordinanze `_15`,
   decreti `_35`).
3. Fetch the decision XML from
   `https://mdp.giustizia-amministrativa.it/visualizza/`, extract full text.

Only `schema=cds` resolves full text on the mdp endpoint; TAR/CGA datasets use
internal schema codes that are not derivable from the CKAN slug and are out of
scope here.

## Usage

```bash
python bootstrap.py test-api            # connectivity + full-text check
python bootstrap.py bootstrap --sample  # validation sample -> sample/
python bootstrap.py bootstrap-fast      # full concurrent pull (fleet entry)
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required. OpenGA (giustizia-amministrativa.it) open data.
