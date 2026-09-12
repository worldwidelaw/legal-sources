# BE/KULeuven — KU Leuven Law Library Open-Access Archive

> ⛔ **BLOCKED — `terms_of_use_prohibit_scraping`. Do not run this scraper.**
> The publisher forbids exactly the reuse this project makes. See [License](#license).

KU Leuven Bibliotheken (Rechtsgeleerdheid en Criminologische Wetenschappen) republishes,
via `rechtsreeks.be`, a large archive of Belgian legal monographs ("juridische klassiekers")
and older journal volumes as scanned/born-digital PDFs — mostly titles predating 2010 that
are absent from Jura, Stradalex and Lexnow.

Suggested in issue [#1360](https://github.com/ZachLaik/LegalDataHunter/issues/1360)
(credit @prenier).

## Status

Blocked on **terms of use**, not on infrastructure. The site is reachable and the corpus
extracts cleanly — the block is a licensing decision, so re-probing the endpoint will not
change it.

What was verified before the block (2026-08-03):

| Check | Result |
|---|---|
| Listing enumeration | 2,815 direct PDF hrefs from one request, no JS, no pagination |
| PDF availability | 200 / `application/pdf`, born-digital |
| Text extraction | Clean, 658K–2.3M chars per volume via `common.pdf_extract` |
| Journals (`/tijdschriften`) | Catalogue-only, links no PDFs |

No sample records or extracted text are retained in this repository, because public
redistribution is one of the two prohibited limbs.

## License

> ⛔ **Commercial use prohibited AND public redistribution prohibited.**
> Personal use only. This source cannot be ingested without written permission.

[KU Leuven Libraries digital archive terms](https://bib.kuleuven.be/rbib/collectie/archieven)
— personal use only; no commercial reuse; no public reuse of anything not in the public domain.

The prohibition is stated on all three archive pages:

- `/rbib/collectie/archieven` — *"Wat mag je doen met deze bestanden? Vrij gebruiken voor
  persoonlijke doeleinden, maar uiteraard geen commercieel hergebruik, evenmin publiek
  hergebruik voor de documenten die nog niet in het publiek domein zijn."*
- `/rbib/collectie/archieven/boeken` — *"Commercieel en publiek (her)gebruik van deze
  bestanden is niet toegelaten."*
- `/rbib/collectie/archieven/tijdschriften` — *"Het is niet toegestaan deze data te
  hergebruiken voor commerciële doeleinden."*

**The per-title `open access` tag means free-to-read, not free-to-reuse.** These are
in-copyright monographs from Larcier, Intersentia, die Keure and others, digitised under a
permission granted *to KU Leuven Libraries* — that permission does not travel to downstream
redistributors. Both prohibited limbs apply to Legal Data Hunter: it is a commercial product
and it republishes document text.

## Unblocking

This needs a licensing conversation, not a code change:

1. Written permission from KU Leuven Bibliotheken Rechtsgeleerdheid en Criminologische
   Wetenschappen (contact is linked from the archive page as "Suggesties voor titels welkom").
2. Clearance from the underlying publishers for the in-copyright titles, since KU Leuven's
   own permission is scoped to its hosting.

Public-domain-only titles (pre-1930 or otherwise expired) would be a narrower, more
defensible subset if a full licence proves unobtainable — but the archive does not label
copyright status per title, so that subset would have to be established by hand.
