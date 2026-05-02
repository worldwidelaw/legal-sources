# NG/SEC — Nigeria Securities and Exchange Commission Circulars

Regulatory circulars, guidelines, and enforcement notices from the Securities
and Exchange Commission of Nigeria. Covers capital market regulation,
investor protection, and market operator requirements.

- **Source:** https://sec.gov.ng/for-investors/keep-track-of-circulars/
- **Data type:** doctrine
- **Language:** English
- **Records:** ~244 circulars (2015–present)
- **Format:** HTML listing page + individual HTML pages, some with PDF attachments

## Strategy

1. Scrape listing page for all circular URLs and dates
2. Fetch each individual page and extract paragraph text
3. If PDF downloads are found, extract text from those as well
4. Combine HTML text and PDF text for full content

## License

[Open Government Data](https://sec.gov.ng/) — publicly published regulatory circulars, attribution required.
