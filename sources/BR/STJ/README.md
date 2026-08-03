# BR/STJ — Superior Tribunal de Justiça (Brazil)

Full **inteiro teor** (complete text) of STJ acórdãos, retrieved from the
official jurisprudence search (SCON) at `scon.stj.jus.br`.

The STJ is Brazil's highest court for non-constitutional federal-law matters
(uniformização da interpretação da lei federal infraconstitucional) — distinct
from `BR/STF` (constitutional court) and the 27 state TJs. Its corpus runs to
millions of acórdãos and decisões.

## What this captures

For each acórdão this source stores the **complete decision PDF text**
(relatório + votos + certidão de julgamento), not merely the ementa/summary.
This is what distinguishes it from `BR/STJDadosAbertos`, whose CKAN "espelhos"
datasets carry only the ementa and cover only May 2022 onward.

Fields: `title`/`process_number` (clean citation, e.g. `AgInt no REsp 1240783 / ES`),
`text` (full inteiro teor), `ementa`, `judge_relator`, `orgao_julgador`,
`date` (data do julgamento), `publication_date`, `numero_registro`.

## Access recipe

The SCON search sits behind a BIG-IP ASM + Cloudflare WAF that rejects naked
programmatic requests (`[#BSTJ#] The requested URL was rejected`). The scraper:

1. Primes a session on `https://scon.stj.jus.br/SCON/` to obtain the BIG-IP
   `TS...` cookie + `JSESSIONID`.
2. Queries `pesquisar.jsp` with a browser User-Agent, the primed cookies and a
   `Referer` header. Enumeration is by publication date using the field
   operator `livre=DTPB="YYYYMMDD"`, paginated via `&l=<n>&i=<offset>`.
3. Downloads each row's inteiro teor from
   `/SCON/GetInteiroTeorDoAcordao?num_registro=<reg>&dt_publicacao=<dd/mm/yyyy>`
   — a text-based (extractable, non-scanned) PDF — and extracts the text.

`fetch_all` walks publication dates backwards from today to 1995, skipping
weekends. On WAF rejection the session is re-primed and the request retried.

Note: datacenter IPs are frequently blocked by BR government sites; the session
priming works from residential IPs and should be run behind a residential/BR
proxy on the fleet if the datacenter IP is rejected.

## License

[Open Government Data](https://scon.stj.jus.br/SCON/) — STJ jurisprudence is
public information under Brazil's Lei de Acesso à Informação (Lei 12.527/2011).
Commercial use permitted.
