WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesOrderHeaderSalesReason')}}
    )
    
    , enrichment_data AS (
        select
            cast(SALESORDERID as int) as codigo_pedido_venda
            , cast(SALESREASONID as int) as codigo_motivo_venda
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
        from raw_data
    )

select *
from enrichment_data