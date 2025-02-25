WITH vendas as (
    select
        *
    from {{ ref("int_vendas__metricas_vendas")}} 
)

select *
from vendas