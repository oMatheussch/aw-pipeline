WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesOrderHeader')}}
    )

select
    cast(SALESORDERID as int) as codigo_pedido_venda
    --, cast(REVISIONNUMBER as int) as codigo_revisao_pedido_venda
    , cast(ORDERDATE as date) as data_pedido
    , cast(DUEDATE as date) as data_prevista_envio
    , cast(SHIPDATE as date) as data_entrega
    , cast(STATUS as int) as codigo_status_pedido /*ADICIONAR DIRETAMENTE OS ASTATUS NESSA LINHA*/ 
    , case 
        when cast(ONLINEORDERFLAG as string) = 'true' then 'Sim'  
        else 'Não'
    end as pedido_online

    /*PENDENTE FINALIZAR*/
    , cast(PURCHASEORDERNUMBER as int)
    , cast(ACCOUNTNUMBER as string)
    , cast(CUSTOMERID as string)
    , cast(SALESPERSONID as string)
    , cast(TERRITORYID as string)
    , cast(BILLTOADDRESSID as string)
    , cast(SHIPTOADDRESSID as string)
    , cast(SHIPMETHODID as string)
    , cast(CREDITCARDID as string)
    , cast(CREDITCARDAPPROVALCODE as string)
    , cast(CURRENCYRATEID as string)
    , cast(SUBTOTAL as string)
    , cast(TAXAMT as string)
    , cast(FREIGHT as string)
    , cast(TOTALDUE as string)
    , cast(COMMENT as string)
    , cast(ROWGUID as string)
    , cast(MODIFIEDDATE as string)
from raw_data

