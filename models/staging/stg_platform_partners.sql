-- models/staging/stg_platform_partners.sql
--
-- One row per partner organisation. Grain: partner_id. 15 rows.
--
-- Exposes partner_name_normalised (lowercase) as a join key against
-- the partner_label_map seed and against freshdesk partner_label_raw.
-- This table is the source of truth for canonical partner names.

with source as (
    select * from {{ source('platform', 'platform_partners') }}
),

cleaned as (
    select
        partner_id,
        trim(partner_name)            as partner_name,
        lower(trim(partner_name))     as partner_name_normalised,
        lower(trim(partner_type))     as partner_type   -- wealth_manager | fund_manager | family_office

    from source
)

select * from cleaned
