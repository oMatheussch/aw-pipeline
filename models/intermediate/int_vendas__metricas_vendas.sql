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

        , {{dbt_utils.generate_surrogate_key(
            [ 'capa_pedido_venda.codigo_pedido_venda' ]
        )}} as sk_codigo_pedido_venda
        , {{dbt_utils.generate_surrogate_key(
            [ 'capa_pedido_venda.codigo_cliente' ]
        )}} as sk_codigo_cliente
        , {{dbt_utils.generate_surrogate_key(
            [ 'capa_pedido_venda.codigo_vendedor' ]
        )}} as sk_codigo_vendedor
        , {{dbt_utils.generate_surrogate_key(
            [ 'itens_pedido_venda.codigo_produto' ]
        )}} as sk_codigo_produto
        , {{dbt_utils.generate_surrogate_key(
            [ 'capa_pedido_venda.codigo_pedido_venda', 'itens_pedido_venda.codigo_produto' ]
        )}} as sk_pedido_venda_produto

        

    from itens_pedido_venda
    left join capa_pedido_venda
        on capa_pedido_venda.codigo_pedido_venda = itens_pedido_venda.codigo_pedido
)

select * 
from  metricas_pedido