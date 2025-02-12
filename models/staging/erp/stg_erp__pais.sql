WITH 
    raw_data as (
        select *
        from {{ source('erp', 'CountryRegion')}}
    )
    
    , enrichment_data AS (
        select
            cast(COUNTRYREGIONCODE as string) as codigo_pais
            , cast(NAME as string) as nome_pais
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
        from raw_data
    )

select *
from enrichment_data