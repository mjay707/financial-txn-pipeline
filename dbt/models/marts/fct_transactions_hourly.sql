{{ config(
    materialized='incremental',
    unique_key='transaction_hour'
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where record_status in ('NORMAL', 'LATE')
),

hourly as (
    select
        date_trunc('hour', event_time)     as transaction_hour,
        count(transaction_id)              as total_transactions,
        sum(amount)                        as total_amount,
        avg(amount)                        as avg_amount,
        sum(case when transaction_type = 'DEBIT' then amount else 0 end)  as total_debits,
        sum(case when transaction_type = 'CREDIT' then amount else 0 end) as total_credits,
        count(case when record_status = 'LATE' then 1 end)                as late_records
    from transactions
    group by date_trunc('hour', event_time)
)

select * from hourly

{% if is_incremental() %}
    where transaction_hour > (select max(transaction_hour) from {{ this }})
{% endif %}