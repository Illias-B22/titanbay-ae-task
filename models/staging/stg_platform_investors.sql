-- models/staging/stg_platform_investors.sql
--
-- One row per investor. Grain: investor_id. 1,253 rows.
--
-- DATA QUALITY: created_at is stored as an Excel serial number.
-- Converted using same formula as freshdesk_tickets.
--
-- relationship_manager_id is nullable (~41% null).
-- Null = self-directed investor (manages own platform activity).
-- Non-null = RM handles the platform activity on their behalf.
--
-- email is lowercased to match against freshdesk requester_email.

with source as (
    select * from {{ source('platform', 'platform_investors') }}
),

cleaned as (
    select
        investor_id,
        user_id,
        lower(trim(email))                                                  as email,
        trim(full_name)                                                     as full_name,
        entity_id,
        trim(country)                                                       as country,
        dateadd('day', created_at::integer, '1899-12-30'::date)::timestamp  as created_at,
        relationship_manager_id   -- nullable; null = self-directed

    from source
)

select * from cleaned
