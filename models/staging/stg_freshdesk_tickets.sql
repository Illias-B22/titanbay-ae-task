-- models/staging/stg_freshdesk_tickets.sql
--
-- One row per support ticket. Grain: ticket_id.
--
-- Key responsibilities:
--   1. Convert Excel serial dates to proper timestamps (both created_at and resolved_at)
--   2. Lowercase + trim requester_email as the join key for entity resolution
--   3. Normalise partner_label to lowercase for seed-based lookup downstream
--   4. Derive resolution_time_hours and is_resolved for analyst convenience
--   5. Flag internal Titanbay tickets for exclusion in marts
--
-- DATA QUALITY: created_at and resolved_at are Excel serial numbers, not timestamps.
-- These were exported from Freshdesk via Excel rather than direct DB extract.
-- Conversion formula: base date 1899-12-30 + N days.
-- Snowflake syntax used throughout. For BigQuery: DATE_ADD(DATE '1899-12-30', INTERVAL value DAY)
--
-- DATA QUALITY: partner_label has 70+ variants for 15 partners — mixed case,
-- abbreviations, truncations. Normalised to lowercase here; resolved to partner_id
-- via seed table in dim_requesters / fct_tickets.

with source as (
    select * from {{ source('freshdesk', 'freshdesk_tickets') }}
),

cleaned as (
    select
        ticket_id::integer                                                      as ticket_id,
        lower(trim(requester_email))                                            as requester_email,
        trim(requester_name)                                                    as requester_name,
        trim(subject)                                                           as subject,
        lower(trim(status))                                                     as status,
        lower(trim(priority))                                                   as priority,

        -- Convert Excel serial to timestamp
        dateadd('day', created_at::integer, '1899-12-30'::date)::timestamp      as created_at,

        case
            when resolved_at is not null and trim(resolved_at::string) != ''
            then dateadd('day', resolved_at::integer, '1899-12-30'::date)::timestamp
            else null
        end                                                                     as resolved_at,

        trim(tags)                                                              as tags,

        -- Normalise for seed join: lowercase + trim; preserve null
        case
            when partner_label is not null and trim(partner_label) != ''
            then lower(trim(partner_label))
            else null
        end                                                                     as partner_label_raw,

        -- Resolution time in hours (null if ticket not yet resolved)
        case
            when resolved_at is not null and trim(resolved_at::string) != ''
            then datediff(
                'hour',
                dateadd('day', created_at::integer, '1899-12-30'::date)::timestamp,
                dateadd('day', resolved_at::integer, '1899-12-30'::date)::timestamp
            )
            else null
        end                                                                     as resolution_time_hours,

        case
            when lower(trim(status)) in ('resolved', 'closed') then true
            else false
        end                                                                     as is_resolved,

        -- Flag internal Titanbay tickets — these are QA/internal and should be
        -- excluded from all IS-facing analysis
        case
            when lower(trim(requester_email)) like '%@titanbay.com'
              or lower(trim(requester_email)) like '%@titanbay.co.uk'
            then true
            else false
        end                                                                     as is_internal

    from source
)

select * from cleaned
