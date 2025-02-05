WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesReason')}}
    )
    
    , enrichment_data AS (
        select
            cast(SALESREASONID as int) codigo_motivo_venda
            , cast(NAME as string) as titulo_motivo
            , cast(REASONTYPE as string) as tipo_motivo
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
        from raw_data
    )

select *
from enrichment_data