# CN/CICC — China International Commercial Court (国际商事法庭)

The China International Commercial Court (CICC) is a special division of the
Supreme People's Court of China, established in 2018 to hear cross-border
commercial disputes. It publishes full judgments (判决书 / 裁判文书) and
guiding/typical cases (典型案例) as server-rendered HTML at `cicc.court.gov.cn`.

Distinct from `AE/DIFC-Courts`, `AE/ADGM-Courts`, and `SG/SICC` (other
international commercial courts already in the manifest). Small but high-value
corpus of landmark cross-border commercial judgments.

## Access recipe

The site sits behind the Chinese-government **WZWS** WAF:

1. The first request to any URL 302-redirects and sets a `wzws_cid` cookie.
2. Immediately re-requesting the **same** URL with that cookie returns HTTP 200
   with the real HTML.

`bootstrap.py` performs this two-step transparently. Full decision text lives in
the article `<p>` paragraphs; nav/footer boilerplate is stripped. Chinese
statute names appear in ASCII angle brackets (e.g. `<中华人民共和国民法典>`) — this
is source content, not HTML markup.

## Usage

```bash
python bootstrap.py test               # connectivity test (WZWS two-step)
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full corpus
python bootstrap.py bootstrap-fast     # full corpus (threaded)
```

## License

[Public Domain (Government)](http://cicc.court.gov.cn/) — Chinese court
judgments are official state legal texts in the public domain. Commercial use
permitted; no attribution required.
