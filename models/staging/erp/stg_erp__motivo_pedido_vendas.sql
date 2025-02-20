WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesReason')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(SALESREASONID as int) codigo_motivo_venda
            , cast(NAME as string) as titulo_motivo
            , cast(REASONTYPE as string) as tipo_motivo
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_motivo_venda' ]
            )}} as sk_codigo_motivo_venda
        from data_cleaning_and_transforming
    )

select *
from enrichment_data