# CA/HumanRightsTribunal — Canadian Human Rights Tribunal (CHRT) Decisions

Full text of every published decision, ruling and reasons of the **Canadian
Human Rights Tribunal** (Tribunal canadien des droits de la personne), the
independent quasi-judicial body that adjudicates complaints of discrimination
referred to it by the Canadian Human Rights Commission under the **Canadian
Human Rights Act**, the **Employment Equity Act** and the **Pay Equity Act**.

Each decision resolves a specific contested case → `case_law`.

- **Source:** https://decisions.chrt-tcdp.gc.ca/chrt-tcdp/en/nav.do
- **Platform:** Lexum *Decisia* (same platform/recipe as `CA/TCC` Tax Court of Canada)
- **Coverage:** ~4,000+ decisions, 1979–present, English collection
- **Access:** no CAPTCHA, no auth; born-digital HTML (no OCR needed)

## How it works

The corpus is browsed by year under the `decisions` collection:

```
/chrt-tcdp/decisions/en/{YYYY}/nav_date.do?page={N}&iframe=true   # 25 items/page
/chrt-tcdp/decisions/en/item/{id}/index.do?iframe=true            # one decision
```

> **Note:** the path needs the `/decisions/` collection segment.
> `/chrt-tcdp/en/nav_date.do` (without it) returns 404. The site's default
> search box is JS/AJAX (browser-bound), but the per-year `nav_date.do`
> listing and the `?iframe=true` item view are fully static and scrapable.

The `?iframe=true` item view returns a metadata table (Collection, Date,
Neutral citation, File number(s), Decision-maker(s), Decision type, Grounds)
followed by the full decision body inside `<div id="document-content">`. The
scraper strips tags, decodes entities and preserves paragraph breaks.

## Usage

```bash
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py test-api             # connectivity + extraction test
```

## License

[Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada) — attribution to the source required. Commercial use permitted.

CHRT decisions are Government of Canada works, reproducible under the Open
Government Licence — Canada.
