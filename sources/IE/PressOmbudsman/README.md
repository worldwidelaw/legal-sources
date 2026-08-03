# IE/PressOmbudsman — Office of the Press Ombudsman & Press Council of Ireland

Published decisions of Ireland's independent press-regulation system.

## What this is

The **Office of the Press Ombudsman** and the **Press Council of Ireland**
handle complaints that a member publication (newspaper, magazine or online news
outlet) has breached the industry **Code of Practice**. The Press Ombudsman
decides each complaint; a dissatisfied party may appeal to the Press Council,
and complaints raising significant issues may be referred directly to the
Council. Each published decision sets out the complaint, the publication's
response, the Principle(s) of the Code of Practice engaged, the reasoning and
the outcome. These quasi-judicial adjudications are stored as `case_law`.

## Corpus

~825 published decisions (2008–present):

| Category | WP cat id | Count (approx.) |
|----------|-----------|-----------------|
| Press Ombudsman decisions | 37 | ~577 |
| Press Council appeal decisions | 32 | ~231 |
| Decisions on referral to the Press Council | 33 | ~17 |

Language: English. Auth: none.

## Access method

`pressombudsman.ie` is a WordPress site. Decisions are ordinary posts filed
under the three categories above. The public **WP REST API** returns each
decision's full body in `content.rendered` (born-digital HTML — no OCR/PDF):

```
GET /wp-json/wp/v2/posts?categories=37&per_page=100&page=N
```

`bootstrap.py` pages through each category, deduplicates by post id, strips the
HTML to plain text, and extracts the `OMB NNNN/YYYY` case reference from the
title where present.

## Usage

```bash
python bootstrap.py test               # connectivity check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (WP `after` filter)
```

## License

> ⚠️ **Commercial use restricted.** The Press Council of Ireland is a private
> independent regulator (not a public sector body), and its website asserts full
> copyright over all materials. Decisions are published for public information;
> commercial re-use is restricted.

[© Press Council of Ireland — all rights reserved](https://pressombudsman.ie/copyright/) — attribution required, commercial use restricted.
