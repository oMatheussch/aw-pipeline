WITH 
    raw_data as (
        select *
        from {{ source('erp', 'CountryRegion')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(COUNTRYREGIONCODE as string) as codigo_pais
            , cast(NAME as string) as nome_pais
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_pais' ]
            )}} as sk_codigo_pais
        from data_cleaning_and_transforming
    )

select *
from enrichment_data