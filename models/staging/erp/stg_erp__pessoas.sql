WITH 
    raw_data as (
        select *
        from {{ source('erp', 'Person')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
        cast(BUSINESSENTITYID as int) as codigo_pessoa
        , cast(TITLE as string) as sexo
	    , cast(FIRSTNAME as string) as primeiro_nome
	    , cast(MIDDLENAME as string) as nome_do_meio
	    , cast(LASTNAME as string) as ultimo_nome
	    , cast(EMAILPROMOTION as string) as aceita_email_de_promocao
	    , cast(ADDITIONALCONTACTINFO as string) as informacao_conatato_adicional
	    , cast(MODIFIEDDATE as string) as data_modificacao
	    {#  
            , cast(ROWGUID as string)
            , cast(DEMOGRAPHICS as string)
            , cast(SUFFIX as string) as sufixo
            , cast(PERSONTYPE as string) as tipo_pessoa
            , cast(NAMESTYLE as string)
        #}
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_pessoa' ]
            )}} as sk_codigo_pessoa
        from data_cleaning_and_transforming
    )

select *
from enrichment_data