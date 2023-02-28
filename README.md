# pgsourcing

*Migrations tool for PostgreSQL + PostGIS + pgRouting*

Ferramenta de 'migração' para PostgreSQL + PostGIS + pgRouting

Permite comparar uma 'fonte' e um 'destino' através da comparação de cada um deles com repositório central que permite registar todos os detalhes sobre os objetos de base de dados dos tipos, pelo menos:

- tabelas
- views
- views materializadas
- procedimentos

As alterações podem ser produzidas gradualmente na fonte, ir sendo progressivamente aplicadas ao repositório central e, posteriormente, aplicadas em massa ao destino.

Em cada momento, podemos sempre comparar a fonte ou o destino com o repositório central.

O repositório e as alterações que reflete podem ser aplicadas em diferentes destinos, consoante as indicações colocadas em "dest" no ficheiro de configuração *conncfg.json*.

O funcionamento da ferramenta baseia-se nas seguintes operações prinicipais:


----------

ASPETOS EM FALTA:

Remover GRANTS em excesso no destino




