{{ config(
    materialized='incremental',
    unique_key='transaction_date'
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where record_status in ('NORMAL', 'LATE')
),

daily as (
    select
        date_trunc('day', event_time)      as transaction_date,
        customer_id,
        count(transaction_id)              as total_transactions,
        sum(amount)                        as total_amount,
        avg(amount)                        as avg_amount,
        max(amount)                        as max_amount,
        count(case when latency_seconds > 1800 then 1 end) as late_count
    from transactions
    group by date_trunc('day', event_time), customer_id
)

select * from daily

{% if is_incremental() %}
    where transaction_date > (select max(transaction_date) from {{ this }})
{% endif %}