# BR/SUSEP — Brazilian Insurance Regulator

Normative acts from SUSEP (Superintendência de Seguros Privados),
Brazil's insurance, reinsurance, capitalization, and pension fund regulator.

**URL:** https://www2.susep.gov.br/safe/bnportal/internet/pt-br/

## Coverage

- Circulares SUSEP
- Resoluções CNSP (Conselho Nacional de Seguros Privados)
- Resoluções SUSEP
- Portarias, Despachos, Deliberações
- ~52,000 normative acts total
- Language: Portuguese

## Strategy

1. Search BNWeb REST API (`bnmapi.exe?router=search`) for all norms (paginated, 48/page)
2. For each norm, get the `cod_anexo` of the "Versão Original" PDF
3. Download PDF via `bnmapi.exe?router=upload/{cod_anexo}`
4. Extract text with pdfplumber

## License

[Public Domain (Government Work)](https://www2.susep.gov.br/safe/bnportal/internet/pt-br/) — Brazilian federal regulatory acts are public domain.
