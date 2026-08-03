# IT/Trento — Trentino Provincial Legislation (Codice provinciale)

Provincial laws and regulations (*leggi e regolamenti provinciali*) of the
**Autonomous Province of Trento** (Provincia autonoma di Trento) from 1951 to
present, from the official **Codice provinciale** maintained by the Consiglio
della Provincia autonoma di Trento.

The Autonomous Province of Trento is a legally-distinct legislating
sub-jurisdiction (ISO 3166-2 `IT-TN`) within the Trentino-Alto Adige/Südtirol
special-statute region; together with the sibling Autonomous Province of Bolzano
(`IT-BZ`) it holds the primary provincial legislative power.

## Data source

- **Enumeration:** official open-data XML index, published on
  [dati.trentino.it](https://dati.trentino.it/dataset/leggi-e-regolamenti-della-provincia-autonoma-di-trento)
  and served at `https://www.consiglio.provincia.tn.it/normattiva/normattiva.xml`
  (updated daily). Each `<LEGGE>` element carries `TIPO` (`legger` = provincial
  law, `rego` = regulation), `TESTO` (`vigente`/`abrogato`), `NUMERO`, `DATA`,
  `TITOLO`, and `URNTESTO`.
- **Full text:** each act's `<URNTESTO>` points to a plain-text consolidated
  body at `/doc-txt/CLEX_*.TXT` (cp1252-encoded). The scraper downloads and
  decodes it into the `text` field.
- **Coverage:** ~2,700 acts (both in-force and repealed), 1951–present.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
python bootstrap.py bootstrap-fast       # full pull, concurrent downloads
python bootstrap.py update 2024          # acts changed since a year
python bootstrap.py test                 # connectivity/parse test
```

`fetch_all()` yields raw metadata dicts from the XML (no per-item network);
`normalize()` performs the full-text download, so `bootstrap_fast` can
parallelize the downloads.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.

The Codice provinciale open-data XML is published on dati.trentino.it under the
Creative Commons Attribution licence. The underlying acts are official acts of a
public administration and are additionally not protected by copyright under
Art. 5 of the Italian Copyright Law (Legge 22 aprile 1941, n. 633). Commercial
use is permitted.
