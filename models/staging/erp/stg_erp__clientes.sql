WITH 
    raw_data as (
        select *
        from {{ source('erp', 'Customer')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(CUSTOMERID as int) as codigo_cliente
            , cast(STOREID as int) as codigo_loja
            , cast(TERRITORYID as int) as codigo_territorio
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
            --, cast(PERSONID as int) as codigo_pessoa
            --, cast(ROWGUID as int)
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_cliente' ]
            )}} as sk_codigo_cliente
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_loja' ]
            )}} as sk_codigo_loja
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_territorio' ]
            )}} as sk_codigo_territorio
        from data_cleaning_and_transforming
    )

select *
from enrichment_data