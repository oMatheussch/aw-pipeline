WITH 
    raw_data as (
        select *
        from {{ source('erp', 'SalesOrderHeader')}}
    )

    , enrichment_data AS (
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
            , cast(PURCHASEORDERNUMBER as string) as codigo_pedido_de_compra
            , cast(ACCOUNTNUMBER as string) as numero_da_conta
            , cast(CUSTOMERID as int) codigo_cliente
            , cast(SALESPERSONID as int) as codigo_vendedor
            , cast(TERRITORYID as int) as codigo_territorio
            , cast(BILLTOADDRESSID as int) as codigo_endereco_de_faturamento
            , cast(SHIPTOADDRESSID as int) as codigo_endereco_de_entrega
            , cast(SHIPMETHODID as int) as codigo_metodo_envio
            , cast(CREDITCARDID as int) as codigo_cartao_de_credito
            , cast(CREDITCARDAPPROVALCODE as string) as codigo_aprovacao_cartao_credito
            , cast(CURRENCYRATEID as int) as codigo_taca_de_moeda
            , cast(SUBTOTAL as decimal) as sub_total
            , cast(TAXAMT as decimal) as imposto
            , cast(FREIGHT as decimal) as frete
            , cast(TOTALDUE as decimal) as total_devido
            , cast(COMMENT as string) as comentario
            --, cast(ROWGUID as string)
            , cast(MODIFIEDDATE as timestamp) data_modificacao
        from raw_data
    )

select *
from enrichment_data