# Titanbay IS Support — Analytics Engineering Take-Home

## The Business Problem

The IS team currently handles tickets as they come in with no real view of who's raising them, what patterns exist or when things are likely to get busy. 
The Head of IS wants two things: 
- A way to understand which investors are struggling most and what with
- Visibility on the calendar to resource the team in advance rather than constantly reacting.

Both of those are solvable with the data available they just need it modelled properly so an analyst isn't manually stitching together Freshdesk exports and platform data every time someone asks a question.

---

## What an Analyst Can Now Do

The current process of answering the question "which investors raise the most tickets" involves manually exporting Freshdesk, the platform database and reconciling them by hand.

After this model the queries analysts can run are:

```sql
-- Which investors raise the most tickets?
select investor_name, partner_name, count(*) as ticket_count
from marts.fct_tickets
where requester_type = 'investor'
group by 1, 2
order by 3 desc;

-- What are they raising tickets about?
select tags, count(*) as ticket_count
from marts.fct_tickets
where requester_type = 'investor'
group by 1
order by 2 desc;

-- How long is resolution taking by partner?
select partner_name, round(avg(resolution_time_hours), 1) as avg_resolution_hrs
from marts.fct_tickets
where is_resolved = true
group by 1
order by 2 desc;

-- Which upcoming closes should IS be staffing up for?
select fund_name, partner_name, scheduled_close_date,
       days_until_close,
       tickets_30d_before_close_same_partner,
       high_priority_tickets_30d
from marts.fct_ticket_volume_by_close
where close_status = 'upcoming'
  and days_until_close between 0 and 60
order by scheduled_close_date;
```

---

## Source Tables: What I Used and Why

| `freshdesk_tickets` | Core — every ticket | Dates are Excel serials not actual timestamps; partner_label is a mess |
| `platform_investors` | Core — resolves investor emails | Clean; email is unique |
| `platform_relationship_managers` | Core — resolves RM emails | Clean; 42 RMs across 15 partners |
| `platform_partners` | Source of truth for partner names/IDs | Clean; 15 rows |
| `platform_entities` | Bridge only — links investors to partners | Not surfaced directly in final models |
| `platform_fund_closes` | Forecasting model — close schedule | Also has Excel serial dates; has an undocumented 'cancelled' status |

**What to trust:** 
Email matching against platform tables is the primary way we link tickets to people as the platform emails are structured and consistent. `partner_label` in Freshdesk is a last resort as it's manually typed by IS staff and has over 70 variants for 15 partners. I don't use `requester_name` as a join key at all as it's free text and names aren't unique.

---

## Modelling Approach

```
sources (Freshdesk + platform warehouse)
    ↓
staging/       — one model per source table
    stg_freshdesk_tickets
    stg_platform_partners
    stg_platform_relationship_managers
    stg_platform_investors
    stg_platform_entities
    stg_platform_fund_closes

seeds/         — lookup table I built from profiling the actual data
    partner_label_map.csv   — maps 70+ partner_label variants to the right partner_id

marts/         — what analysts actually query
    dim_requesters                  — who raised each ticket?
    fct_tickets                     — one row per ticket
    fct_ticket_volume_by_close      — ticket pressure forecast by close date
```

Staging models consist of type casting, lowercasing and fixing the date issue described below. All the actual logic lives in the marts layer where it's easy to follow and test.

---

## Grain

I was careful about grain throughout particularly in two places where it's easy to get it wrong.

| Model | Grain | Decision |
|---|---|---|
| Staging models | Same as source | No joins or aggregation |
| `dim_requesters` | One row per unique `requester_email` | Used `QUALIFY ROW_NUMBER()` to deduplicate |
| `fct_tickets` | One row per `ticket_id` | Tags kept flat to preserve grain (see below) |
| `fct_ticket_volume_by_close` | One row per `close_id` | Aggregated with conditional COUNT not a JOIN |

**Tags:** 
The `tags` field looks like `"kyc,payment,documents"`. If we exploded that into one row per tag every ticket count would be multiplied by the number of tags on that ticket so decided to keep it flat in `fct_tickets` 
tag-level analysis needs its own model where the grain change is explicit.

**The close/ticket join:** 
`fct_ticket_volume_by_close` uses a cross join with filters inside the COUNT rather than a regular join. A regular join would fan out the closes grain. One close matching many tickets means many rows and this would break grain. To keep it at one row per close we used conditional COUNT.

---

## The Linkage Problem

This is the core issue of the task. There's no shared foreign key between Freshdesk and the platform, the only bridge is `requester_email`.

### Two types of requester

Tickets are raised by two different kinds of people and putting them together would produce misleading numbers.

As investors directly accessed the platform this meant a ticket from an investor means that specific person had a problem.

Relationship Managers raised tickets on behalf of clients. One RM raising 10 tickets could be 10 different investors' problems or the same investor's issue that kept occuring.

We fix the above by joining `dim_requesters` and `requester_email` against the investor table first then the RM table. 

The classification logic in order:

1. Matches `platform_investors.email` → `investor`
2. Matches `platform_relationship_managers.email` → `relationship_manager`
3. `@titanbay.com` or `@titanbay.co.uk` → `internal`
4. Personal email → `unknown_personal`
5. Everything else → `unknown_business`

Doing some analysis on the actual data on the 331 unique Freshdesk emails(we excluded Internal tickets):

| Type | Count | % |
|---|---|---|
| Investor matched | 181 | 55% |
| RM matched | 40 | 12% |
| Titanbay internal | 44 | 13% |
| Unknown personal | 29 | 9% |
| Unknown business domain | 37 | 11% |


### The 37 unmatched business emails

Things like `abigail.taylor@west-invest.co.uk` had no match in the platform. Also, domains like `@west-invest.co.uk` doesn't reliably map to any of Titanbay's 15 partners. They were left as `unknown_business` with null partner fields so the IS team can see and investigate them.

### Partner resolution in `fct_tickets`

For matched investors: `investor → entity → partner_id`.
For matched RMs: direct from the RM table.
For unknowns: fall back to `partner_label` via the seed lookup.

The `partner_resolution_source` column flags which method was used: `email_match`, `label_fallback`, or `unresolved`.

---

## Data Quality Issues

### 1. Excel serial dates
`created_at` and `resolved_at` in `freshdesk_tickets`, `created_at` in `platform_investors` and `scheduled_close_date` in `platform_fund_closes` are all stored as numbers like `45941.0` rather than actual dates.

Fix: `dateadd('day', value::integer, '1899-12-30'::date)` in the staging models. A long-term action would be to pull these directly from the Freshdesk API and the platform DB.

### 2. partner_label has 70+ variants for 15 partners
"CLEARWATER DIRECT", "Clearwater D", "clearwater direct", "Clearwater" are all the same partner. About 44% of tickets have no label at all.

Fix: lowercased in staging and then resolved with the seed lookup we built by going through every unique value in the actual data. Where both email match and label lookup fail the partner stays null and is flagged as `unresolved`. Long-term: this field should be a dropdown that is auto-filled from the requester's platform profile.

### 3. Internal QA tickets mixed in with live data
About 44 Titanbay-domain emails appear in the ticket data. Subjects like "Internal QA test" distorts things.

Fix: flagged as `is_internal = true` in staging part and filtered out in `fct_tickets`. Long-term: internal testing should be in a separate Freshdesk queue.

### 4. 'cancelled' close status not in the data dictionary
`platform_fund_closes` has a `close_status = 'cancelled'` that isn't documented. Cancelled closes don't generate investor activity so they're excluded from the forecasting model.

### 5. Tags stored as a comma-separated string
This breaks the grain so was kept flat in `fct_tickets`.

---

## Assumptions

1. If the same email appears in both the investor and RM tables then the investor obvioulsy takes priority.

2. There's no reliable way to get  which investor an RM was raising a ticket for from the data. The IS team needs to add an "acting on behalf of" field to Freshdesk.

3. The `partner_label_map` seed is based on what's in this dataset. New variants will appear and the seed will need to be maintainined.

4. All SQL is written in Snowflake dialect. BigQuery equivalents are noted in the staging model comments where the syntax differs.

5. The 37 unmatched business-domain emails are left unresolved rather than trying to be guessed as a wrong guess is worse than an acknowledged null.

---

## `fct_tickets` Column Reference

| Column | Description |
|---|---|
| `ticket_id` | Freshdesk ticket ID |
| `requester_email` | Email of the person who raised the ticket |
| `subject` | Ticket subject |
| `status` | open / pending / resolved / closed |
| `priority` | low / medium / high / urgent |
| `created_at` | When the ticket was raised |
| `resolved_at` | When resolved — null if still open |
| `resolution_time_hours` | Hours from creation to resolution, null if unresolved |
| `is_resolved` | true if status is resolved or closed |
| `tags` | Comma-separated tags — kept flat |
| `partner_label_raw` | Original Freshdesk label, lowercased — reference only |
| `requester_type` | investor / relationship_manager / unknown_personal / unknown_business |
| `investor_id` | Platform investor ID (null if not an investor) |
| `investor_name` | Investor full name from platform |
| `rm_id` | RM platform ID (null if not an RM) |
| `rm_name` | RM full name from platform |
| `entity_id` | Investing entity ID (investor tickets only) |
| `entity_type` | individual / corporate / trust / pension_fund |
| `investor_kyc_status` | approved / pending / expired / rejected |
| `investor_rm_id` | The investor's assigned RM — different from who raised the ticket |
| `partner_id` | Canonical partner UUID |
| `partner_name` | Canonical partner name |
| `partner_type` | wealth_manager / fund_manager / family_office |
| `partner_resolution_source` | email_match / label_fallback / unresolved |
| `ticket_month` | Month the ticket was raised |
| `ticket_week` | Week the ticket was raised |
| `ticket_day_of_week` | 0–6 |
| `ticket_hour_of_day` | 0–23 |

---

## How I Used AI

I used AI in this or the initial data analysis, profiling the source tables, and building out the models. It helped me move faster on things like spotting the Excel serial date issue across multiple tables, working through the entity resolution logic.
The data issues themselves — the serial dates, the partner_label variants, the internal QA tickets and the undocumented cancelled status actually came from profiling the data rather than just reading the brief.
---

## Reflection

The root problem is that Freshdesk and the platform have no shared key as email is the only bridge and it breaks when someone uses a different address or raises a ticket before their account exists. The real fix is for the platform's support flow to pass the user's `investor_id` into Freshdesk as a custom field when the ticket is created and for RM tickets to capture which investor they're acting for.

---

## What I'd Build Next

- **`fct_ticket_tags`** — expand tags into one row per ticket-tag pair so you can actually analyse what investors are struggling with by topic
- **`fct_investor_support_summary`** — one row per investor with total tickets, most common tag, avg resolution time and last ticket date. This gives IS team visbility on an investor's full history.
- **Incremental materialisation on `fct_tickets`** — currently full refresh but should be incremental on `created_at` as volume grows
- **Source freshness test** — no current alert if the Freshdesk export goes stale
