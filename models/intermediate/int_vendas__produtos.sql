WITH produtos as (
    select *
    from {{ ref("stg_erp__produtos")}}
)

, data_enrichment as (
    select 
        produtos.nome_produto

        , {{dbt_utils.generate_surrogate_key(
            [ 'produtos.codigo_produto' ]
        )}} as sk_codigo_produto
    from produtos
)

select * 
from  data_enrichment