WITH dim_motivo_venda as (
    select *
    from {{ ref("int_vendas__motivo_venda")}}
)

select *
from dim_motivo_venda