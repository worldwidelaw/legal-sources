# NL/CVDR — Dutch Decentralized Regulations (CVDR)

**Source:** [https://lokaleregelgeving.overheid.nl/](https://lokaleregelgeving.overheid.nl/)
**API:** SRU 1.2 — `https://zoekdienst.overheid.nl/sru/Search?x-connection=cvdr`
**Data types:** legislation

## Overview

The **Centrale Voorziening Decentrale Regelgeving (CVDR)** is the central register
of all decentralized (local) regulations in the Netherlands. This source fetches
the full-text corpus enacted by Dutch decentralized authorities:

| Authority type | Approx. count |
|----------------|---------------|
| Gemeenten (municipalities) | ~299,000 |
| Provincies (provinces) | ~16,900 |
| Waterschappen (water boards) | ~6,800 |
| **Total** | **~322,700** |

It is the decentralized counterpart to `NL/wetten.overheid.nl` (national law) and
is scoped by `organisatietype` so it does **not** overlap the Caribbean CVDR
subsets already covered by `AW/Legislation`, `CW/Legislation` and `SX/Legislation`
(those filter on `koninkrijksdeel`).

## Method

1. SRU `searchRetrieve`, query `organisatietype=Gemeente OR organisatietype=Provincie OR organisatietype=Waterschap`, paginated 100/page.
2. Each record's `enrichedData/publicatieurl_xml` points to the born-digital regulation XML in the officiele-overheidspublicaties repository.
3. Download the XML and extract plain text from the `<lichaam>` body (`al`, `kop`, `artikel`, etc.). No OCR required.

## License

[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) — public domain.

Dutch legislation and decisions by public authorities carry no copyright
(art. 11 Auteurswet). The KOOP official-publications data behind the CVDR is
provided as open government data under CC0. Commercial use permitted.
