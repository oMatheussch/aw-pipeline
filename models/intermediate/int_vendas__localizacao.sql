WITH 
pais as (
    select 
       sk_codigo_pais
       , nome_pais
    from {{ ref("stg_erp__pais")}} pais
)

, provincia as (
    select 
       sk_codigo_provincia_estado
       , sk_codigo_pais
       , sigla_provincia_estado
       , nome_provincia
    from {{ ref("stg_erp__provincia_estadual")}} provincia_estadual
)

, enderecos as (
    select 
       sk_codigo_endereco
       , sk_codigo_provincia_estado
       , descricao_endereco_linha_1
       , descricao_endereco_linha_2
       , nome_cidade
    from {{ ref("stg_erp__enderecos")}} enderecos
)

, join_localizacao as (
    select 
        enderecos.sk_codigo_endereco
        , enderecos.descricao_endereco_linha_1
        , enderecos.descricao_endereco_linha_2
        , enderecos.nome_cidade
        , provincia.sigla_provincia_estado
        , pais.nome_pais
    from enderecos
    left join provincia
        on enderecos.sk_codigo_provincia_estado = provincia.sk_codigo_provincia_estado
    left join pais
        on provincia.sk_codigo_pais = pais.sk_codigo_pais
)

select *
from join_localizacao