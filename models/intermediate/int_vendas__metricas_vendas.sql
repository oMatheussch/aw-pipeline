WITH capa_pedido_venda as (
    select *
    from {{ ref("stg_erp__capa_pedido_venda")}}
)

, itens_pedido_venda as (
    select *
    from {{ ref("stg_erp__itens_pedido_venda")}}
)

, metricas_pedido as (
    select 
        capa_pedido_venda.codigo_pedido_venda
        , capa_pedido_venda.data_pedido
        , capa_pedido_venda.sub_total
        , capa_pedido_venda.total_devido

        , itens_pedido_venda.codigo_item_pedido
        , itens_pedido_venda.quantidade_comprada
        , itens_pedido_venda.preco_unitario
        , itens_pedido_venda.desconto_unitario
        , ((itens_pedido_venda.preco_unitario - itens_pedido_venda.desconto_unitario) 
            * itens_pedido_venda.quantidade_comprada 
        ) as valor_total_unitario

        , itens_pedido_venda.sk_codigo_produto
        , itens_pedido_venda.sk_pedido_venda_produto      
        , capa_pedido_venda.sk_codigo_pedido_venda
        , capa_pedido_venda.sk_codigo_cliente
        , capa_pedido_venda.sk_codigo_vendedor
    from itens_pedido_venda
    left join capa_pedido_venda
        on capa_pedido_venda.codigo_pedido_venda = itens_pedido_venda.codigo_pedido
)

select * 
from  metricas_pedido