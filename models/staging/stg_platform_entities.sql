-- models/staging/stg_platform_entities.sql
--
-- One row per investing entity. Grain: entity_id. 772 rows.
-- Entities are the legal structures (individual, corporate, trust, pension_fund)
-- through which investors hold their commitments.
-- Each entity belongs to exactly one partner.

with source as (
    select * from {{ source('platform', 'platform_entities') }}
),

cleaned as (
    select
        entity_id,
        trim(entity_name)           as entity_name,
        partner_id,
        lower(trim(entity_type))    as entity_type,   -- individual | corporate | trust | pension_fund
        lower(trim(kyc_status))     as kyc_status      -- approved | pending | expired | rejected

    from source
)

select * from cleaned
