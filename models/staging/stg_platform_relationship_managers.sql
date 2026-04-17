-- models/staging/stg_platform_relationship_managers.sql
--
-- One row per relationship manager. Grain: rm_id. 42 rows.
--
-- email is lowercased to match against freshdesk requester_email.
-- This is the resolution path for RM-raised tickets.

with source as (
    select * from {{ source('platform', 'platform_relationship_managers') }}
),

cleaned as (
    select
        rm_id,
        partner_id,
        trim(name)              as rm_name,
        lower(trim(email))      as email    -- join key to freshdesk requester_email

    from source
)

select * from cleaned
