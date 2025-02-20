WITH cartao as (
    select 
        bandeira_cartao
        , sk_codigo_cartao_credito
    from {{ ref("stg_erp__cartao_de_credito")}}
)

select *
from cartao