-- models/marts/fct_ticket_volume_by_close.sql
--
-- One row per fund close. Grain: close_id.
--
-- PURPOSE: Addresses the Head of IS's second question — "anticipate when our team
-- is under more pressure than usual so we can plan resourcing in advance."
--
-- HYPOTHESIS: Ticket volume spikes in the weeks leading up to a fund close, as
-- investors rush to complete KYC, sign documents, and finalise commitments.
-- By surfacing ticket counts in windows before each close, IS can look at the
-- calendar of upcoming closes and anticipate pressure points.
--
-- DESIGN DECISIONS:
--   - We count tickets from the SAME PARTNER as the close (most relevant signal)
--   - Window: 30 days before the close date (configurable via variable if needed)
--   - We also provide an all-partner count for absolute volume context
--   - Cancelled closes are excluded — they generate no investor activity
--   - We count only external tickets (is_internal already filtered in fct_tickets)
--
-- GRAIN NOTE: This model is aggregated to close_id level. Joining it back to
-- fct_tickets would fan out rows — analysts should use this as a standalone
-- forecasting reference, not joined to ticket-level data.

with closes as (
    select *
    from {{ ref('stg_platform_fund_closes') }}
    where close_status != 'cancelled'
),

tickets as (
    -- Use fct_tickets (already excludes internal) for consistency
    select
        ticket_id,
        partner_id,
        created_at,
        requester_type,
        is_resolved,
        priority
    from {{ ref('fct_tickets') }}
),

partners as (
    select partner_id, partner_name
    from {{ ref('stg_platform_partners') }}
),

close_ticket_counts as (
    select
        c.close_id,
        c.fund_id,
        c.fund_name,
        c.partner_id,
        p.partner_name,
        c.close_number,
        c.scheduled_close_date,
        c.close_status,
        c.total_committed_aum_gbp,

        -- Days until close from today (useful for upcoming closes)
        datediff('day', current_date(), c.scheduled_close_date)     as days_until_close,

        -- Ticket counts in the 30-day window before this close, same partner
        count(
            case
                when t.partner_id = c.partner_id
                 and t.created_at >= dateadd('day', -30, c.scheduled_close_date::timestamp)
                 and t.created_at < c.scheduled_close_date::timestamp
                then t.ticket_id
            end
        )                                                           as tickets_30d_before_close_same_partner,

        -- Ticket counts across ALL partners in same window (absolute volume)
        count(
            case
                when t.created_at >= dateadd('day', -30, c.scheduled_close_date::timestamp)
                 and t.created_at < c.scheduled_close_date::timestamp
                then t.ticket_id
            end
        )                                                           as tickets_30d_before_close_all_partners,

        -- Breakdown by requester type (same partner, same window)
        count(
            case
                when t.partner_id = c.partner_id
                 and t.requester_type = 'investor'
                 and t.created_at >= dateadd('day', -30, c.scheduled_close_date::timestamp)
                 and t.created_at < c.scheduled_close_date::timestamp
                then t.ticket_id
            end
        )                                                           as investor_tickets_30d,

        count(
            case
                when t.partner_id = c.partner_id
                 and t.requester_type = 'relationship_manager'
                 and t.created_at >= dateadd('day', -30, c.scheduled_close_date::timestamp)
                 and t.created_at < c.scheduled_close_date::timestamp
                then t.ticket_id
            end
        )                                                           as rm_tickets_30d,

        -- High/urgent tickets only — strongest signal for resourcing pressure
        count(
            case
                when t.partner_id = c.partner_id
                 and t.priority in ('high', 'urgent')
                 and t.created_at >= dateadd('day', -30, c.scheduled_close_date::timestamp)
                 and t.created_at < c.scheduled_close_date::timestamp
                then t.ticket_id
            end
        )                                                           as high_priority_tickets_30d

    from closes c
    cross join tickets t   -- cross join then filter — avoids fan-out on the closes grain
    left join partners p on p.partner_id = c.partner_id
    group by
        c.close_id, c.fund_id, c.fund_name, c.partner_id, p.partner_name,
        c.close_number, c.scheduled_close_date, c.close_status,
        c.total_committed_aum_gbp
)

select * from close_ticket_counts
order by scheduled_close_date asc
