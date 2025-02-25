WITH dim_localizacao as (
    select *
    from {{ ref("int_vendas__localizacao")}}
)

select *
from dim_localizacao