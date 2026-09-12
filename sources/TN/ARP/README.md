# TN/ARP — Assemblée des représentants du peuple (Tunisian Parliament)

Bills before the Tunisian parliament — government bills (*مشاريع قوانين* / projets
de loi), private members' bills (*مقترحات قوانين* / propositions de loi), and the
laws they became once published in the *Rāʾid Rasmī* (JORT). This adds the
pre-enactment stage on top of [TN/JORT-Legislation](../JORT-Legislation), which
only carries enacted texts.

- **Publisher:** Assemblée des représentants du peuple, Bardo, Tunisia
- **Site:** https://www.arp.tn
- **Language:** Arabic
- **Type:** `legislation`
- **Auth:** none

## Access method

`arp.tn` runs Odoo 12 and exposes its website models over the standard JSON-RPC
endpoint, unauthenticated. The site's own search widget
(`.spec-website-search`, `/sws/render`) calls exactly this route, so we consume
the same data the pages render — no HTML scraping.

```
POST https://www.arp.tn/web/dataset/call_kw
{"jsonrpc":"2.0","method":"call","params":{
   "model":"gpl.law.project","method":"search_read",
   "args":[[], ["ref","sujet","date","state"], 0, 100, "date desc"],"kwargs":{}}}
```

Model `gpl.law.project` holds every bill (584 at time of writing). The three
site listings — `/loi/project/list`, `/loi/proposition/list` and
`/loi/project/loi` — are the same model under different domains, so a single
sweep covers all of them.

## Full text

Each record's `text` is assembled from two routes, in order:

1. **`gpl.law.item`** — the structured, article-by-article text of the bill
   (HTML `item` field). Populated for ~150 bills.
2. **`ir.attachment` PDFs** at `/document/download/<id>` — the bill as
   deposited, committee reports, and the JORT page carrying the enacted law.

Many of the ~2,800 attached PDFs are born-digital and extract cleanly (the JORT
pages and the more recent committee reports in particular); the remainder are
scans of paper originals with no text layer. Since OCR is not available in this
pipeline, bills that yield no text from either route are **dropped** rather than
emitted as metadata-only records, which leaves roughly a third of the 584 bills.

`/document/download/<id>` is served by Odoo's HTTP dispatcher, which answers a
GET carrying a JSON `Content-Type` with `400 Invalid JSON data: ''` instead of
the file. The session therefore must not set a JSON content type globally — the
RPC POST sets its own. `_check_download_route` raises if the first dozen
downloads all come back non-PDF, so a future break in this route fails loudly
rather than quietly shrinking the corpus to the article-text bills.

Arabic PDFs are run through the shared `common/pdf_extract` path, which repairs
visual-order RTL output and normalises Arabic presentation forms.

## Record schema

| Field | Notes |
|---|---|
| `_id` | `TN/ARP/<project_id>` |
| `_type` | `legislation` |
| `title` | `sujet` — the bill's full descriptive title |
| `text` | articles + extracted attachment text |
| `date` | date of deposit (*تاريخ الإيداع*) |
| `url` | `https://www.arp.tn/loi/project/<project_id>` |
| `ref` | parliamentary number, e.g. `2026/053` |
| `law_type` | organic / ordinary / constitutional / convention |
| `initiator` | Presidency of the Republic, or a group of deputies |
| `committee` | committee seized of the bill |
| `state` / `state_label` | stage in the legislative path |
| `jort_number`, `jort_date`, `jort_title` | once enacted and published |

## Incremental updates

`fetch_updates(since)` filters on Odoo's `write_date` — when the record last
changed on arp.tn — not on the bill's deposit date, so amendments, committee
reports and enactment of an old bill are all picked up.

## License

[Organic Law 2016-22 on the right of access to information](https://www.arp.tn/relation_societe_civile/access_information) — Tunisian official
legal texts (bills, committee reports, laws published in the JORT). Public
sector information, re-use permitted; attribution to the ARP requested. No
restrictive terms of use are published on arp.tn.
