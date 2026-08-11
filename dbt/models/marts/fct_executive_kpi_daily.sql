{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where record_status in ('NORMAL', 'LATE')
),

kpi as (
    select
        date_trunc('day', event_time)      as kpi_date,
        region,
        count(transaction_id)              as total_transactions,
        sum(amount)                        as total_revenue,
        avg(amount)                        as avg_transaction_value,
        sum(case when transaction_type = 'DEBIT' then amount else 0 end)  as total_debits,
        sum(case when transaction_type = 'CREDIT' then amount else 0 end) as total_credits
    from transactions
    group by date_trunc('day', event_time), region
)

select * from kpi