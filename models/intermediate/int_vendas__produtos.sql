WITH produtos as (
    select 
        nome_produto
        , sk_codigo_produto
    from {{ ref("stg_erp__produtos")}}
)

select *
from produtos