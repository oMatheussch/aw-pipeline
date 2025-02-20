WITH 
    raw_data as (
        select *
        from {{ source('erp', 'CreditCard')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(CREDITCARDID as int) as codigo_cartao_credito
            , cast(CARDTYPE as string) as bandeira_cartao
            , cast(CARDNUMBER as int) as numero_cartao
            , cast(EXPMONTH as int) as mes_expiracao
            , cast(EXPYEAR as int) as ano_expiracao
            , cast(MODIFIEDDATE as timestamp) as data_modificacao 
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_cartao_credito' ]
            )}} as sk_codigo_cartao_credito
        from data_cleaning_and_transforming
    )
select *
from enrichment_data