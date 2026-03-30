# UY/IMPO — Uruguayan Legislation (Parlamento del Uruguay)

## Source
- **Name**: IMPO - Centro de Información Oficial (via Parlamento)
- **URL**: https://parlamento.gub.uy/documentosyleyes/leyes
- **Country**: Uruguay (UY)
- **Type**: Legislation
- **Auth**: None (public access)

## Coverage
~11,000 laws from Law 9500 (1935) to Law 20468 (2026).

## Strategy
1. Enumerate laws via the Parlamento JSON API at `/documentosyleyes/leyes/json` with date-range filtering
2. For each law, fetch its Parlamento page to extract the iframe URL to `infolegislativa.parlamento.gub.uy`
3. Fetch the HTM document from infolegislativa for full text
4. Clean HTML to plain text

## Notes
- IMPO (impo.com.uy) has consolidated/updated text but is partially paywalled
- Parlamento provides original text for all laws, free of charge
- Server reports ISO-8859-1 encoding but content is actually UTF-8
- Rate limit: 2 sec between Parlamento requests
