# MX/IFT — Instituto Federal de Telecomunicaciones (IFT) - Resoluciones del Pleno

Plenary resolutions adopted by the IFT (now Comisión Reguladora de Telecomunicaciones), Mexico's telecommunications and broadcasting regulator. Covers competition, spectrum allocation, infrastructure sharing, sanctions, and regulatory decisions.

## Coverage

- **Period:** 2013–2025
- **Volume:** ~6,000+ resolutions
- **Language:** Spanish
- **Types:** case_law, doctrine (regulatory decisions, competition rulings, sanctions)

## Strategy

1. Crawl paginated session listing at `ift.org.mx/conocenos/pleno/sesiones-del-pleno`
2. For each session page, parse HTML to extract resolution numbers and PDF links
3. Download each resolution PDF and extract full text via `pdf_extract`

## License

[Mexican Transparency Law (LGTAIP)](https://www.ift.org.mx/conocenos/transparencia) — official government resolutions published under transparency obligations. Attribution required.
