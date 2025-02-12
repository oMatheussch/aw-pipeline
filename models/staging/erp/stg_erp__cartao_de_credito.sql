WITH 
    raw_data as (
        select *
        from {{ source('erp', 'CreditCard')}}
    )
    
    , enrichment_data AS (
        select
            cast(CREDITCARDID as int) as codigo_cartao_credito
            , cast(CARDTYPE as string) as bandeira_cartao
            , cast(CARDNUMBER as int) as numero_cartao
            , cast(EXPMONTH as int) as mes_expiracao
            , cast(EXPYEAR as int) as ano_expiracao
            , cast(MODIFIEDDATE as timestamp) as data_modificacao 
        from raw_data
    )

select *
from enrichment_data