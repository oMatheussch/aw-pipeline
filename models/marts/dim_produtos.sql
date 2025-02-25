WITH dim_produtos as (
    select *
    from {{ ref("int_vendas__produtos")}}
)

select *
from dim_produtos