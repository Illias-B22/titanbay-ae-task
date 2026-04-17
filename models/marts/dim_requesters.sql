-- models/marts/dim_requesters.sql
--
-- One row per unique requester email seen in Freshdesk tickets.
-- Grain: requester_email.
--
-- PURPOSE: Resolve every Freshdesk requester to their platform identity.
-- This is the core entity resolution model. All ticket enrichment flows through here.
--
-- RESOLUTION LOGIC (in priority order):
--   1. Match email → platform_investors  → requester_type = 'investor'
--   2. Match email → platform_rms        → requester_type = 'relationship_manager'
--   3. @titanbay.com / .co.uk domain     → requester_type = 'internal'
--   4. Personal email domain             → requester_type = 'unknown_personal'
--   5. Other unmatched business domain   → requester_type = 'unknown_business'
--
-- KNOWN RESOLUTION RATES (profiled from actual data):
--   investors matched:      181 / 331 unique emails (~55%)
--   RMs matched:             40 / 331 (~12%)
--   Titanbay internal:       44 / 331 (~13%)
--   Unknown personal email:  29 / 331 (~9%)
--   Unknown business email:  37 / 331 (~11%)
--
-- ASSUMPTION: Investor match takes precedence over RM match (enforced by
-- conditional left join). Edge case — same email in both tables — should not
-- occur given different domains, but this makes the logic deterministic.
--
-- NOTE ON RM TICKETS: RM-raised tickets are linked to partner level only.
-- We do not infer which investor the RM was acting for — that would be guesswork.
--
-- NOTE ON freshdesk_emails CTE: We use QUALIFY with ROW_NUMBER to get one row
-- per requester_email with the most recent requester_name. SELECT DISTINCT with
-- a window function is not reliable in Snowflake — QUALIFY is the correct pattern.

with investors as (
    select
        i.email,
        i.investor_id,
        i.full_name,
        i.entity_id,
        i.relationship_manager_id,
        e.partner_id,
        e.entity_type,
        e.kyc_status
    from {{ ref('stg_platform_investors') }} i
    left join {{ ref('stg_platform_entities') }} e
        on i.entity_id = e.entity_id
),

rms as (
    select
        email,
        rm_id,
        rm_name,
        partner_id
    from {{ ref('stg_platform_relationship_managers') }}
),

partners as (
    select partner_id, partner_name, partner_type
    from {{ ref('stg_platform_partners') }}
),

-- One row per unique requester email, using the most recent requester_name
-- QUALIFY + ROW_NUMBER is the correct Snowflake pattern for this deduplication
freshdesk_emails as (
    select
        requester_email,
        requester_name
    from {{ ref('stg_freshdesk_tickets') }}
    qualify row_number() over (
        partition by requester_email
        order by created_at desc
    ) = 1
),

resolved as (
    select
        fe.requester_email,
        fe.requester_name,

        case
            when i.email is not null
                then 'investor'
            when r.email is not null
                then 'relationship_manager'
            when fe.requester_email like '%@titanbay.com'
              or fe.requester_email like '%@titanbay.co.uk'
                then 'internal'
            when fe.requester_email like '%@gmail.com'
              or fe.requester_email like '%@outlook.com'
              or fe.requester_email like '%@icloud.com'
              or fe.requester_email like '%@yahoo.co.uk'
              or fe.requester_email like '%@hotmail.com'
                then 'unknown_personal'
            else 'unknown_business'
        end                                                     as requester_type,

        i.investor_id,
        i.full_name                                             as investor_name,
        i.entity_id,
        i.entity_type,
        i.kyc_status,
        i.relationship_manager_id,

        r.rm_id,
        r.rm_name,

        -- Partner: from entity (investors) or directly (RMs); null for unknowns
        coalesce(i.partner_id, r.partner_id)                    as partner_id,
        p.partner_name,
        p.partner_type

    from freshdesk_emails fe
    left join investors i
        on fe.requester_email = i.email
    left join rms r
        on fe.requester_email = r.email
        and i.email is null     -- only join RM if investor match not found
    left join partners p
        on p.partner_id = coalesce(i.partner_id, r.partner_id)
)

select * from resolved
