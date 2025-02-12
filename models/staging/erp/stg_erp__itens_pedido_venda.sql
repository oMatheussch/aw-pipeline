WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesorderDetail')}}
    )
    
    , enrichment_data AS (
        select
            cast(SALESORDERID as int) as codigo_pedido
            , cast(SALESORDERDETAILID as int) as codigo_item_pedido
            , cast(CARRIERTRACKINGNUMBER as string) as codigo_rastreio
            , cast(ORDERQTY as int) as quantidade_comprada
            , cast(PRODUCTID as int) as codigo_produto
            , cast(SPECIALOFFERID as int) as codigo_oferta_especial
            , cast(UNITPRICE as decimal) as preco_unitario
            , cast(UNITPRICEDISCOUNT as decimal) as desconto_unitario
            , cast(MODIFIEDDATE as timestamp) as data_modificacao 

            --, cast(ROWGUID as string)
        from raw_data
    )

select *
from enrichment_data