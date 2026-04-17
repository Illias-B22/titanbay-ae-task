-- models/marts/fct_tickets.sql
--
-- One row per support ticket. Grain: ticket_id.
-- This is the primary analytics model for IS support analysis.
--
-- Enriches each ticket with:
--   - Resolved requester identity and type (via dim_requesters)
--   - Canonical partner name and ID (from platform, with fallback to partner_label seed)
--   - Resolution time in hours
--   - Calendar fields for time-series analysis
--
-- INTERNAL TICKETS EXCLUDED: Tickets from @titanbay.com/co.uk domains are
-- filtered out. They represent QA/internal activity and would distort IS metrics.
--
-- PARTNER RESOLUTION STRATEGY (two-step with documented fallback):
--   Step 1: Use partner_id resolved via email match in dim_requesters (most reliable)
--   Step 2: If step 1 null, look up partner_label_raw against partner_label_map seed
--   If both null: partner fields remain null (ticket is genuinely unattributable)
--
-- GRAIN NOTE: Tags are kept as a comma-separated string. Do NOT explode tags here
-- as that would change the grain and corrupt ticket counts. Tag-level analysis
-- should use a separate fct_ticket_tags model (see suggestions in README).

with tickets as (
    select * from {{ ref('stg_freshdesk_tickets') }}
    where is_internal = false   -- exclude Titanbay QA/internal tickets
),

requesters as (
    select * from {{ ref('dim_requesters') }}
),

label_map as (
    select * from {{ ref('partner_label_map') }}
),

partners as (
    select partner_id, partner_name, partner_type
    from {{ ref('stg_platform_partners') }}
),

enriched as (
    select
        -- Core ticket fields
        t.ticket_id,
        t.requester_email,
        t.subject,
        t.status,
        t.priority,
        t.created_at,
        t.resolved_at,
        t.resolution_time_hours,
        t.is_resolved,
        t.tags,
        t.partner_label_raw,

        -- Requester identity (from entity resolution)
        r.requester_type,
        r.investor_id,
        r.investor_name,
        r.rm_id,
        r.rm_name,
        r.entity_id,
        r.entity_type,
        r.kyc_status                                            as investor_kyc_status,
        r.relationship_manager_id                               as investor_rm_id,

        -- Partner resolution (step 1: from email match; step 2: from label seed)
        coalesce(r.partner_id, lm.partner_id)                   as partner_id,
        coalesce(r.partner_name, lm.canonical_partner_name)     as partner_name,
        coalesce(r.partner_type, p2.partner_type)               as partner_type,

        -- Flag whether partner was resolved via email (reliable) or label (fallback)
        case
            when r.partner_id is not null then 'email_match'
            when lm.partner_id is not null then 'label_fallback'
            else 'unresolved'
        end                                                     as partner_resolution_source,

        -- Calendar fields for time-series analysis
        date_trunc('month', t.created_at)                       as ticket_month,
        date_trunc('week', t.created_at)                        as ticket_week,
        dayofweek(t.created_at)                                 as ticket_day_of_week,
        extract('hour' from t.created_at)                       as ticket_hour_of_day

    from tickets t
    left join requesters r
        on t.requester_email = r.requester_email
    left join label_map lm
        on t.partner_label_raw = lm.partner_label_normalised
        and r.partner_id is null   -- only use label fallback if email match gave no partner
    left join partners p2
        on p2.partner_id = lm.partner_id
        and r.partner_id is null
)

select * from enriched
