# IT/Bolzano — South Tyrol Provincial Legislation (LexBrowser Bolzano/Bozen)

Consolidated provincial legislation of the **Autonomous Province of
Bolzano/Südtirol** (Provincia autonoma di Bolzano – Alto Adige, `IT-BZ`) from the
official **LexBrowser** system: <https://lexbrowser.provinz.bz.it/>.

Covers provincial laws (*leggi provinciali*), decrees of the President of the
Province (*decreti del Presidente della Provincia*), legislative decrees and
regulations (*regolamenti*) from **1946 to present** — ~2,000+ acts, born-digital
consolidated full text.

## How it works

1. **Enumeration** — the chronological index `/chrono/it/{YYYY}/` (1946–present)
   lists every act as `/doc/it/{docId}/{slug}.aspx`. The slug encodes the act
   type, date and number (e.g. `legge_provinciale_3_gennaio_2020_n_1`). Only
   clean-legislation types are kept (laws, presidential decrees, legislative
   decrees, regulations, testi unici, statuti); *delibere*, *contratti* and
   court decisions are skipped.
2. **Full text** — `GET /doc/it/{docId}/{slug}.aspx?view=1` renders the full
   consolidated article text inline. The `?view=1` parameter is required — without
   it the page shows only collapsed article headers. The body is extracted from
   the `<div id="documento" class="documentoesteso">` container.
3. **Throttle handling** — the doc endpoint enforces a per-IP request queue;
   a blocked request 302-redirects to `/TooManyRequests.aspx`. The scraper warms
   a session against the chrono index (setting the throttle cookie) and polls the
   doc URL with polite backoff until the real body arrives.

A German-language variant is available at `/chrono/de/` and `/doc/de/`.

## Usage

```bash
python bootstrap.py test                 # connectivity/parse test
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
python bootstrap.py bootstrap-fast       # full pull, concurrent downloads
python bootstrap.py update 2024          # acts of 2024 onward
```

## License

[CC0 1.0 / Public Domain](https://creativecommons.org/publicdomain/zero/1.0/) —
official acts of a public administration.

Under **Art. 5 of the Italian Copyright Law** (Legge 22 aprile 1941, n. 633),
official acts of the State and public administrations are not protected by
copyright. LexBrowser publishes these consolidated texts as public informational
content with no access restriction or authentication. Commercial use permitted;
no attribution required.
