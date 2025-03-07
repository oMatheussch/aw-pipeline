WITH clientes as (
    select 
        clientes.sk_codigo_cliente
        , clientes.sk_codigo_pessoa
        , clientes.sk_codigo_loja
        , clientes.sk_codigo_territorio
    from {{ ref("stg_erp__clientes")}} clientes
)

, pessoas as (
    select 
        pessoas.sk_codigo_pessoa
        , pessoas.primeiro_nome
        , pessoas.nome_do_meio
        , pessoas.ultimo_nome
    from {{ ref("stg_erp__pessoas")}} pessoas
)

, join_clientes as (
    select 
        clientes.sk_codigo_cliente
        , pessoas.primeiro_nome
        , pessoas.nome_do_meio
        , pessoas.ultimo_nome
        , concat(
            pessoas.primeiro_nome
            , ' '
            , pessoas.ultimo_nome
        ) as primeiro_ultimo_nome
    from clientes
    left join pessoas
        on clientes.sk_codigo_pessoa = pessoas.sk_codigo_pessoa
)

select *
from join_clientes