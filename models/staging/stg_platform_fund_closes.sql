-- models/staging/stg_platform_fund_closes.sql
--
-- One row per fund close. Grain: close_id. 153 rows.
-- A fund has multiple closes (sequential tranches of committed capital).
--
-- DATA QUALITY: scheduled_close_date is stored as an Excel serial number.
-- Converted to DATE type here. Same Excel origin issue as other tables.
--
-- DATA QUALITY: close_status includes 'cancelled' which is not documented
-- in the data dictionary. Preserved as-is; filtered in marts where relevant.

with source as (
    select * from {{ source('platform', 'platform_fund_closes') }}
),

cleaned as (
    select
        close_id,
        fund_id,
        trim(fund_name)                                                         as fund_name,
        partner_id,
        close_number::integer                                                   as close_number,

        -- Convert Excel serial to DATE
        dateadd('day', scheduled_close_date::integer, '1899-12-30'::date)       as scheduled_close_date,

        lower(trim(close_status))                                               as close_status,  -- upcoming | completed | cancelled
        total_committed_aum::bigint                                             as total_committed_aum_gbp

    from source
)

select * from cleaned
