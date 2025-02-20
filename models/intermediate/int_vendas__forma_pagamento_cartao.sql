WITH cartao as (
    select 
        bandeira_cartao

        , {{dbt_utils.generate_surrogate_key(
            [ 'cartao_de_credito.codigo_cartao_credito' ]
        )}} as sk_codigo_cartao_credito
    from {{ ref("stg_erp__cartao_de_credito")}} cartao_de_credito
)

select 
from  metricas_pedido