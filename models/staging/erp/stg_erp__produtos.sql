WITH 
    raw_data as (
        select *
        from {{ source('erp', 'Product')}}
    )
    
    , data_cleaning_and_transforming AS (
        select
            cast(PRODUCTID as int) as codigo_produto
	        , cast(PRODUCTMODELID as int) codigo_modelo_produto
            , cast(NAME as string) as nome_produto 
	        , cast(PRODUCTNUMBER as string) numero_produto

	        , cast(COLOR as string) as cor_produto
	        , cast(SAFETYSTOCKLEVEL as int) as estoque_de_seguranca
	        , cast(REORDERPOINT as int) as ponto_reencomenda
	        , cast(STANDARDCOST as decimal) as custo_standard
	        , cast(LISTPRICE as decimal) as preco_lista
	        , cast(SIZE as string) as tamanho
	        , cast(SIZEUNITMEASURECODE as string) as codigo_unidade_de_medida_tamanho
	        , cast(WEIGHTUNITMEASURECODE as string) as codigo_unidade_de_medida_peso
	        , cast(WEIGHT as decimal) as peso_produto
	        , cast(DAYSTOMANUFACTURE as int) as tempo_de_producao
	        , cast(PRODUCTLINE as string) as linha_do_produto
	        , cast(CLASS as string) as classe
	        , cast(STYLE as string) as estilo
	        , cast(PRODUCTSUBCATEGORYID as int) as codigo_subcategoria
	        
	        , cast(SELLSTARTDATE as string) as data_inicial_venda
	        , cast(SELLENDDATE as string) as data_final_venda
	        , cast(DISCONTINUEDDATE as int) as data_descontinuacao
	        , cast(MODIFIEDDATE as string) as data_modificacao
            --, cast(MAKEFLAG as boolean) as indefinido
	        --, cast(FINISHEDGOODSFLAG as boolean) as indefinido2
            --, cast(ROWGUID as string)
        from raw_data
    )

    , enrichment_data AS (
        select 
            *
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_produto' ]
            )}} as sk_codigo_produto
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_modelo_produto' ]
            )}} as sk_codigo_modelo_produto
            , {{dbt_utils.generate_surrogate_key(
                [ 'codigo_subcategoria' ]
            )}} as sk_codigo_subcategoria
        from data_cleaning_and_transforming
    )

select *
from enrichment_data