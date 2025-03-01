WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesOrderHeaderSalesReason')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(SALESORDERID as int) as codigo_pedido_venda
            , cast(SALESREASONID as int) as codigo_motivo_venda
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                    [ 'codigo_pedido_venda' ]
                )}} as sk_codigo_pedido_venda
            , {{dbt_utils.generate_surrogate_key(
                    [ 'codigo_motivo_venda' ]
                )}} as sk_codigo_motivo_venda
        from data_cleaning_and_transforming
    )

select *
from enrichment_data