WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesorderDetail')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(SALESORDERID as int) as codigo_pedido
            , cast(SALESORDERDETAILID as int) as codigo_item_pedido
            , cast(CARRIERTRACKINGNUMBER as string) as codigo_rastreio
            , cast(ORDERQTY as int) as quantidade_comprada
            , cast(PRODUCTID as int) as codigo_produto
            , cast(SPECIALOFFERID as int) as codigo_oferta_especial
            , cast(UNITPRICE as float) as preco_unitario
            , cast(UNITPRICEDISCOUNT as float) as desconto_unitario
            , cast(MODIFIEDDATE as timestamp) as data_modificacao 

            --, cast(ROWGUID as string)
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_pedido' ]
            )}} as sk_codigo_pedido_venda
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_produto' ]
            )}} as sk_codigo_produto
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_pedido', 'codigo_item_pedido' ]
            )}} as sk_pedido_venda_item
        from data_cleaning_and_transforming
    )

select *
from enrichment_data