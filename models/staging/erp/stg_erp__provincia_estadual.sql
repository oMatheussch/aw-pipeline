WITH 
    raw_data as (
        select *
        from {{ source('erp', 'StateProvince')}}
    )
    
    , data_cleaning_and_transforming AS (
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

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_provincia_estado' ]
            )}} as sk_codigo_provincia_estado
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_regiao' ]
            )}} as sk_codigo_regiao
        from data_cleaning_and_transforming
    )

select *
from enrichment_data