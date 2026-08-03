# IE/ASAI — Advertising Standards Authority for Ireland — Complaint Adjudications

Full text of complaint adjudications ("Decisions") issued by the
[Advertising Standards Authority for Ireland (ASAI)](https://adstandards.ie/),
the independent self-regulatory body for advertising in Ireland. Each
adjudication determines whether a specific advertisement breached the ASAI Code
of Standards for Advertising and Marketing Communications, and is published in
full — a quasi-judicial determination on a specific case (`case_law`).

## Coverage

- **~1,066 adjudications** (adstandards.ie WordPress `complaint` custom post type).
- Each record captures the full published adjudication: the meta header
  (Reference, Product, Advertiser, Influencer, Medium, Code sections invoked)
  and the body (Advertisement / Complaint / Response / Conclusion / Action
  Required), plus a derived outcome (upheld / not upheld / resolved).

## Access

1. **List** — public WordPress REST API custom post type:
   `https://adstandards.ie/wp-json/wp/v2/complaint?per_page=100&page=N`
   returns `id`, publish `date`, `slug`, canonical `link`, category `title`, and
   `class_list` (advertiser / medium / bulletin taxonomy slugs). ~11 pages.
2. **Full text** — the REST `content.rendered` is empty; the adjudication body is
   server-rendered on each canonical page (`/complaint/{slug}/`) inside the
   Elementor `single-post` region, isolated between
   `data-elementor-type="single-post"` and the footer. Born-digital HTML — no
   OCR or PDF extraction required.

No authentication required; open public website.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all adjudications)
python bootstrap.py bootstrap --sample   # ~15 sample documents
python bootstrap.py bootstrap-fast       # Full pull (runner alias)
python bootstrap.py update               # Incremental (recent adjudications)
python bootstrap.py test                 # Connectivity test
```

## License

> ⚠️ **Commercial use restricted.** The ASAI is a private self-regulatory body;
> its adjudications are published publicly on adstandards.ie under copyright with
> no open-data licence. Treat as all-rights-reserved pending clarification.

[© Advertising Standards Authority](https://adstandards.ie/) — adjudications
published publicly for transparency; no open licence stated. Attribution to the
ASAI expected.
