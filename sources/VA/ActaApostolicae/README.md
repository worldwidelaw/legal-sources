# VA/ActaApostolicae — Vatican Apostolic Documents

**Source:** [https://www.vatican.va/](https://www.vatican.va/)
**Data types:** legislation

## Method

vatican.va runs Adobe Experience Manager, so appending `.N.json` to any content
path returns the underlying JCR tree. Sections are listed via
`/content/{pope}/{lang}/{section}/documents.1.json` and bodies read from
`/content/{pope}/{lang}/{section}/documents/{slug}.3.json`.

Two things make a naive single-language crawl truncate the corpus:

- **The slug set differs per language tree.** Paul VI's apostolic letters list
  229 documents under `/en/` but 325 under `/la/`. Slugs are therefore unioned
  across `en, la, it, es, fr, pt, de`.
- **Many acts have no English body.** The English node carries
  `isemptybody: "true"` — this covers essentially all ~640 John Paul II and
  ~356 Paul VI apostolic constitutions erecting or redrawing dioceses, which
  were only ever promulgated in Latin. Each document is walked down the same
  language chain until a version carries text; English wins whenever it holds
  the real act.

Full runs stream to `data/records.jsonl` and checkpoint to
`data/checkpoint.json` every 50 documents, so an interrupted run resumes
without refetching.

## Usage

```bash
python bootstrap.py test              # connectivity + language-fallback check
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap-fast    # full corpus -> data/records.jsonl
```

## License

Open government data — [https://www.vatican.va/](https://www.vatican.va/)
