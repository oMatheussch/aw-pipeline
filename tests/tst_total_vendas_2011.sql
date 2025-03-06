/*
Teste para validação de valor bruto vendido em 2011 
Conforme já auditado, o valor bruto vendido é: R$ $12.646.112,16 
*/

WITH valor_bruto_vendido_2011 as (
    select 
        SUM(valor_total_liquido_produto)
    from int_vendas__metricas_vendas
    where data_pedido >= '2011-01-01' and  data_pedido <= '2011-12-31'
)

select *
from valor_bruto_vendido_2011
