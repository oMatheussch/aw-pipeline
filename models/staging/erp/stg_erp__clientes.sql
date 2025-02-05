WITH 
    raw_data as (
        select *
        from {{ source('erp', 'Customer')}}
    )
    
    , enrichment_data AS (
        select
            cast(CUSTOMERID as int) as codigo_cliente
            , cast(STOREID as int) as codigo_loja
            , cast(TERRITORYID as int) as codigo_territorio
            , cast(MODIFIEDDATE as timestamp) as data_modificacao
            --, cast(PERSONID as int) as codigo_pessoa
            --, cast(ROWGUID as int)
        from raw_data
    )

select *
from enrichment_data