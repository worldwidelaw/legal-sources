# RO/LegislationDatabase - Romanian Legislative Portal

## Overview

Data source for Romanian legislation from the official Portal Legislativ operated by the Ministry of Justice.

- **URL**: https://legislatie.just.ro
- **Data type**: legislation
- **Coverage**: 150,000+ laws from 1989 to present
- **Full text**: Yes (returned directly in API response)
- **License**: Public domain (not protected by copyright under Romanian law)

## API

- **Type**: SOAP (WCF-based)
- **Endpoint**: https://legislatie.just.ro/apiws/FreeWebService.svc/SOAP
- **WSDL**: https://legislatie.just.ro/apiws/FreeWebService.svc?wsdl
- **Documentation**: http://legislatie.just.ro/ServiciulWebLegislatie.htm
- **Authentication**: Token-based (obtained via GetToken call, no registration needed)

### Methods

1. **GetToken** - Obtains an authentication token (expires periodically)
2. **Search** - Searches legislation with optional filters:
   - NumarPagina (page number)
   - RezultatePagina (results per page, max 50)
   - SearchAn (year filter)
   - SearchNumar (document number filter)
   - SearchTitlu (title search)
   - SearchText (full text search)

### Response Fields

Each legislation record (Legi element) contains:
- **DataVigoare**: Date in force (YYYY-MM-DD)
- **Emitent**: Issuing body (Parlamentul, Guvernul, etc.)
- **LinkHtml**: URL to the document on legislatie.just.ro
- **Numar**: Document number
- **Publicatie**: Publication reference (Monitorul Oficial)
- **Text**: Full text of the legislation
- **TipAct**: Type of legal act (LEGE, OG, OUG, HG, etc.)
- **Titlu**: Document title

## Document Types

- **LEGE**: Law (Lege)
- **OG**: Government Ordinance (Ordonanță a Guvernului)
- **OUG**: Emergency Government Ordinance (Ordonanță de Urgență a Guvernului)
- **HG**: Government Decision (Hotărâre a Guvernului)
- **DECRET**: Decree (Decret)
- And various other regulatory acts

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap (150K+ records - use with caution)
python bootstrap.py bootstrap

# Incremental update (recent years)
python bootstrap.py update
```

## Notes

- The portal is interconnected with the European N-Lex gateway
- Database is updated daily
- Full text is returned directly in the API response (no secondary fetch needed)
- Token expires periodically and must be regenerated
- Language: Romanian
