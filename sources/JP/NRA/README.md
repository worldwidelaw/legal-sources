# JP/NRA — Nuclear Regulation Authority (原子力規制委員会)

Binding legal instruments issued by Japan's **Nuclear Regulation Authority
(NRA)**, the independent nuclear and radiation-safety regulator created after the
2011 Fukushima accident.

## What this collects

The **Related Laws & Ordinances** (関連法令) index lists the NRA's binding
instruments under the Radioisotope Regulation Law and related statutes:

- Official notices / ordinances (告示)
- Enforcement orders and regulations (施行令 / 施行規則)
- Binding regulatory and inspection guides (審査ガイド / 立入検査ガイド)

All are classified as **legislation**. Full text (Japanese) is extracted from the
born-digital PDFs.

## How it works

1. Fetch the static, server-rendered index page
   `https://www.nra.go.jp/activity/ri_kisei/kanrenhourei/index.html`.
2. Extract every inline `/data/{id}.pdf` link and its anchor title (dropping the
   trailing `【PDF：…】` size annotation).
3. Download each PDF and extract text via the shared `common.pdf_extract`
   backend. The promulgation date printed at the top of each ordinance
   (Japanese era format, e.g. `平成三十年十一月二十六日`) is parsed to ISO 8601
   where possible; otherwise `date` is null.

No JavaScript, CAPTCHA, or authentication is required. Tested locally with
`/usr/bin/python3` (which has `requests`, `pdfplumber`, `fitz`).

```bash
python bootstrap.py test               # verify listing + one PDF download
python bootstrap.py bootstrap --sample # fetch sample records
python bootstrap.py bootstrap          # full run (~60 instruments)
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required
(`出典：原子力規制委員会ホームページ`).

NRA content is published under the **Government of Japan Standard Terms of Use
v2.0**, which the NRA's [terms of use](https://www.nra.go.jp/english/termofuse.html)
and [copyright page](https://www.nra.go.jp/nra/site/copyright.html) state is
compatible with CC BY 4.0. Reuse, translation, adaptation and commercial use are
permitted with attribution.
