WITH clientes as (
    select 
        --PENDENTE TRAZER INFORMAÇÃO DE NOME DE CLIENTES
        sk_codigo_cliente
        , sk_codigo_loja
        , sk_codigo_territorio
    from {{ ref("stg_erp__clientes")}}
)

select *
from clientes