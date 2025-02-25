WITH dim_clientes as (
    select *
    from {{ ref("int_vendas__clientes")}}
)

select *
from dim_clientes