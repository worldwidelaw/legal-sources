# NZ/HDC — Health and Disability Commissioner Decisions

Formal opinions of New Zealand's Health and Disability Commissioner (Te Toihau
Hauora, Hauatanga) on complaints about health and disability services. The
Commissioner is an independent Crown entity who investigates complaints under
the Health and Disability Commissioner Act 1994 and determines whether a
provider breached the Code of Health and Disability Services Consumers' Rights.

- **Coverage:** 1999 to present, ~1,980 published opinions (165 index pages × 12 tiles)
- **Type:** `case_law`
- **Language:** English
- **Auth:** none

## Access strategy

There is no API, bulk download, or open-data dataset for HDC opinions; the
decision index is the only access path.

1. Walk `/decisions/search-decisions/?page=N`. Each tile carries the decision
   URL, title and publication date, so the date comes from the index rather
   than being inferred from the case reference — a reference like `21HDC02726`
   encodes the **complaint** year, not the decision year.
2. Fetch each decision page and take the full opinion from the in-page
   `c-rte__body-text` block.
3. Fall back to the linked born-digital PDF (`/media/...pdf`) when a page
   carries no body text.

Sample opinions run 6K–140K characters of full text.

Discovery fails loud: 12 consecutive transport failures on the index abort the
run rather than reporting a truncated corpus, and a walk that finds fewer than
85% of the advertised decisions records a coverage gap.

## Usage

```bash
python bootstrap.py test-api             # connectivity + parse check
python bootstrap.py bootstrap --sample   # 15 samples spread across the index
python bootstrap.py bootstrap-fast       # full corpus (fleet entry point)
```

## Notes

- Opinions are anonymised by the Commissioner before publication; individual
  consumers and most providers appear as "the consumer" / "the provider".
- Case references appear in two shapes in URLs (`21hdc02726`,
  `07hdc05409-decision`); the last URL segment is used verbatim as `_id`.

## License

> ⚠️ **Commercial use restricted.** HDC does not apply the NZGOAL Creative
> Commons default — its terms permit personal, educational and research use
> only. Any other use needs prior written permission from the Commissioner.

[HDC website copyright terms](https://www.hdc.org.nz/copyright/) — material
"may be reproduced for personal, educational or research purposes without
formal permission or charge"; other uses require prior written permission.
Attribution must cite the HDC website and the publication date in written form
(the HDC logo may not be used for attribution).
