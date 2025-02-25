WITH dim_forma_pagamento_cartao as (
    select *
    from {{ ref("int_vendas__forma_pagamento_cartao")}}
)

select *
from dim_forma_pagamento_cartao