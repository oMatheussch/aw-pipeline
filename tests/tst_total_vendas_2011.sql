/*
Teste para validação de valor bruto vendido em 2011 
Conforme já auditado, o valor bruto vendido é: R$ $12.641.421 
*/

WITH pedidos_valor_bruto_vendido_2011 as (
    select 
        DISTINCT(sk_codigo_pedido_venda)
        , sub_total as sub_total
    from int_vendas__metricas_vendas
    where data_pedido >= '2011-01-01' and  data_pedido <= '2011-12-31'

)

, soma as (
    select
        sum(sub_total) as soma_sub_total
    from pedidos_valor_bruto_vendido_2011
)

select *
from soma
where ROUND(soma_sub_total, 0) != 12641421
