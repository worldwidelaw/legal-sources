# TJ/SupremeCourt-Plenum — Қарорҳои Пленуми Суди Олии Ҷумҳурии Тоҷикистон

Full text of the Plenum resolutions of the Supreme Court of Tajikistan, published at [sud.tj/sanadho/karorhoi-plenumi-sudi-oli](https://sud.tj/sanadho/karorhoi-plenumi-sudi-oli/).

The Plenum issues resolutions that give binding interpretive guidance to the lower courts on how to apply the law — the post-Soviet equivalent of a practice direction carrying the force of settled doctrine. Because individual Tajik judgments are not published, these resolutions are the closest thing the jurisdiction has to a public body of authoritative case law.

**Coverage:** 67 resolutions, 2002–2023 — criminal cases 39, civil and family cases 25, questions of judicial activity 3
**Language:** Tajik (Cyrillic)
**Data type:** doctrine
**Text:** 2.3K–63K chars per resolution (median 21K), all born-digital

Distinct from [TJ/SupremeCourt](../SupremeCourt/), which scrapes only `/nashriyai-sudi-oli/` (the Supreme Court bulletin) — different path, no overlap.

## Access

Three static section pages, no API, no auth, no JavaScript:

```
GET https://sud.tj/sanadho/karorhoi-plenumi-sudi-oli/pgo/   # civil and family
GET https://sud.tj/sanadho/karorhoi-plenumi-sudi-oli/pj/    # criminal
GET https://sud.tj/sanadho/karorhoi-plenumi-sudi-oli/mfs/   # judicial activity
```

Each is a flat list of links whose text carries the date, the resolution number and the subject:

```html
<a href="/upload/documents/plenum/{dir}/{file}.pdf">
  Қарори Пленуми Суди Олӣ аз {DD.MM.YYYY} №{N} {subject}
</a>
```

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # ~15 sample resolutions
python bootstrap.py bootstrap            # Full pull (67 resolutions)
python bootstrap.py bootstrap-fast       # Full pull (VPS)
```

## Gotchas

- **The PDF filenames are Tajik Cyrillic with spaces.** The hrefs in the HTML are already percent-encoded, so they are used byte-for-byte; retyping a filename risks an NFC/NFD mismatch and a 404 (the same trap as OM/SJC-CaseLaw).
- Every section page also links the Judicial Code of Conduct (`/upload/documents/Кодекси_одоби_судя.pdf`) from its sidebar. Only hrefs under `/plenum/` are collected.
- Dates in the link text are written both `29.05.2003` and `29.09. 2014` (stray space); both are parsed.
- A resolution is amended by later Plenum resolutions rather than replaced, so the published PDF is the consolidated text and its head lists the amending resolutions.
- Resolution numbers restart each year, so `_id` is `tj-plenum-{date}-{number}`.

## License

[Public Domain (Government)](https://sud.tj/) — official acts of a state body of the Republic of Tajikistan, published by the Supreme Court itself with no terms-of-use restriction. Treated as public-domain government data.
