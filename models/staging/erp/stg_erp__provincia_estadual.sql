WITH 
    raw_data as (
        select *
        from {{ source('erp', 'StateProvince')}}
    )
    
    , enrichment_data AS (
        select
            cast(STATEPROVINCEID as int) as codigo_provincia_estado
            , cast(STATEPROVINCECODE as string) as sigla_provincia_estado
            , cast(COUNTRYREGIONCODE as string) as codigo_regiao
            , case
                when cast(ISONLYSTATEPROVINCEFLAG as string) = 'true' then 'Sim'
                else 'Não'
            end as unica_provincia_do_pais
            , cast(NAME as string) as nome_provincia
            , cast(TERRITORYID as string) as codigo_territorio
            , cast(MODIFIEDDATE as string) as data_modificacao 
            --, cast(ROWGUID as string)
        from raw_data
    )

select *
from enrichment_data