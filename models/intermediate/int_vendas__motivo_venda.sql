WITH motivo_pedido_vendas as (
    select 
        descricao_motivo
        , sk_codigo_motivo_venda
    from {{ ref("stg_erp__motivo_pedido_vendas")}}
)

select * 
from motivo_pedido_vendas