WITH 
    raw_data as (
        select *
        from {{ source('erp', 'Address')}}
    )
    
    , enrichment_data AS (
        select
            cast(ADDRESSID as int) as codigo_endereco
            , cast(ADDRESSLINE1 as string) as descricao_endereco_linha_1
            , cast(ADDRESSLINE2 as string) as descricao_endereco_linha_2
            , cast(CITY as string) as nome_cidade
            , cast(STATEPROVINCEID as int) as codigo_provincia_estado
            , cast(POSTALCODE as string) as  codigo_postal
            , cast(SPATIALLOCATION as string) as localizacao_espacial
            , cast(MODIFIEDDATE as timestamp) as data_modificacao 
            --, cast(ROWGUID as string)
        from raw_data
    )

select *
from enrichment_data