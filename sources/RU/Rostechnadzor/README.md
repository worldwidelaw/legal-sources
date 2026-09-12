# RU/Rostechnadzor — Federal Environmental, Industrial and Nuclear Supervision Service

Normative acts of **Ростехнадзор** (Федеральная служба по экологическому,
технологическому и атомному надзору), the Russian federal regulator for the
use of atomic energy, hazardous industrial facilities, hydraulic structures and
energy plant.

Under art. 6 of Federal Law 170-FZ of 21 November 1995 «Об использовании
атомной энергии» and Federal Law 116-FZ of 21 July 1997 «О промышленной
безопасности», the service enacts the **федеральные нормы и правила** (ФНП) —
the mandatory safety requirements binding on every licensee — together with the
administrative procedures, risk-indicator lists and licensing rules that govern
its own supervision. Each act is issued as a Rostechnadzor приказ and
registered with the Ministry of Justice, so it is a normative legal act in the
full sense.

## Access path

`www.gosnadzor.ru`, the service's own site, answers **HTTP 403 to every request
from outside Russia** — a whole-site edge block returning a bare
`<h1>Forbidden</h1>` and a request id, not a page-level restriction. The acts
themselves are therefore read from **«Законодательство России»**, the state's
official legal-information retrieval system (ИПС) at
`http://pravo.gov.ru/proxy/ips/`, which is reachable worldwide and anonymous.

ИПС is a better artefact than the service's own site would be: it publishes the
**consolidated text** of each act — the operative wording as currently in force,
with the amendment chain stated in the preamble — as HTML, whereas
`publication.pravo.gov.ru` carries only scanned signature-page PDFs with no text
layer.

Discovery is by **issuing body**, not by keyword: the `a6` classifier filter
returns exactly the acts the service itself issued, where a free-text search for
"Ростехнадзор" caps out and mixes in acts of other bodies that merely mention
it. Three classifier ids cover the whole lineage of the service and its
predecessors (~930 acts).

Everything is `GET`, windows-1251:

| Step | Request |
|------|---------|
| Classifier ids | `POST ?autocomplete&bpa=cd00000&nclassif=6&area=110`, body `query=<prefix>` |
| Listing | `?list_itself=&bpas=cd00000&a6=<id>&a6type=1&a6value=<label>&flagFind=0&sort=7&start=<n>` (20 rows/page) |
| Full text | `?doc_itself=&nd=<nd>&page=<k>` — omitting `rdk` serves the current consolidated edition |

Note that ИПС nests a complete Word-exported HTML document inside
`#text_content`, so the act's own `<head>` sits in the middle of the page; it is
stripped, otherwise its `<title>` and the `<!--[if gte mso 9]><xml>` settings
block prepend `Complex Print false false false MicrosoftInternetExplorer4` to
every record.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap --full     # full corpus
python bootstrap.py bootstrap-fast       # high-throughput full pull (fleet)
python bootstrap.py test-api             # connectivity test
```

## Record shape

`_id`, `_source`, `_type`, `_fetched_at`, `title`, `text` (full consolidated
act), `date`, `url`, plus `issuing_body`, `instrument_type`,
`instrument_number`, `heading`, `in_force_state`, `official_publication` and
`ips_id`.

`_type` is `legislation` for приказ / постановление / распоряжение and
`doctrine` for письмо / информация / разъяснение.

## Related sources

The nuclear-safety subset is also published, with richer enactment metadata and
in born-digital PDF/HTML renditions, by ФБУ «НТЦ ЯРБ» — the service's designated
scientific-support centre — at `www.secnrs.ru/science/development/{fnp,rb}/`
(registers of ФНП and safety guides in force) and `docs.secnrs.ru`
(«Библиотека ЯРБ», the full texts). Both hosts are reachable worldwide. They are
not collected here because ИПС already carries the enacting приказ with the ФНП
text as its appendix; the secnrs.ru route is documented as the fallback should
the ИПС classifier filter break.

## License

[Russian official document — public domain (Civil Code art. 1259(6))](http://pravo.gov.ru/) — no restrictions.

Article 1259(6)(1) of the Civil Code of the Russian Federation excludes official
documents of state bodies — laws, other normative acts, judicial decisions and
their official translations — from copyright protection. Every record here is a
normative act of a federal executive body, so the text is in the public domain.
`pravo.gov.ru` is the state's own official publication and legal-information
system. Commercial use is permitted; no attribution is required.
