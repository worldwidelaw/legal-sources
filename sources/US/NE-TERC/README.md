# US/NE-TERC — Nebraska Tax Equalization and Review Commission (Decisions & Orders)

Full text of the decisions and orders of the **Nebraska Tax Equalization and
Review Commission (TERC)**, Nebraska's independent quasi-judicial commission
that hears and decides appeals of property-tax valuation and equalization,
tax exemptions, and related ad-valorem tax controversies — typically a
taxpayer or political subdivision versus a County Board of Equalization, plus
statewide equalization proceedings. Each decision resolves a specific appeal,
so the corpus is **case_law**.

## Source

- **Listing:** https://terc.nebraska.gov/decisions-search
- Server-rendered Drupal site. The decisions-search page is a Search-API view
  whose results come from the Drupal views AJAX endpoint
  (`/views/ajax`, view `decisions`). A single AJAX response lists every
  `decisions-week-ending-{month}-{d}-{yyyy}` index page (~1,100 weeks,
  April 2003 to present).
- Each weekly index page links the decision PDFs issued that week under
  `/sites/default/files/...`; the anchor text is the docket + parties
  (e.g. `22R 0633 Smolsky v. Douglas County Bd. of Equal.`).
- PDFs are born-digital text-layer documents. No JavaScript, no CAPTCHA,
  no auth.

## How it works

1. `discover_week_pages()` POSTs the views AJAX endpoint and parses every
   weekly index page URL (each carries its own week-ending date).
2. `discover_documents()` fetches each weekly page and extracts the decision
   PDF anchors (href + case caption). The decision date is the week-ending
   date of the index page.
3. `_build_raw()` downloads each PDF (curl, browser UA, ~1 req/s) and extracts
   the text layer via `common.pdf_extract`.
4. `normalize()` maps to the standard `case_law` schema with full `text`.

Note: many weeks list only one-line summary dispositions (confessions of
judgment, dismissals, default judgments) with no linked PDF; only weeks with
linked PDFs yield full-text records (typically 1–5 substantive decisions per
such week).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions -> sample/
python bootstrap.py bootstrap            # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Nebraska commission decision)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Nebraska Tax Equalization and Review Commission are official quasi-judicial government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
