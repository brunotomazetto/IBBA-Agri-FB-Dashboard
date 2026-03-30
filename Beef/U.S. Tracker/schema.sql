-- ═══════════════════════════════════════════════════════════════════════
-- U.S. Beef Packer Margin Tracker — Supabase Schema
-- Run once in the Supabase SQL editor for your project.
-- ═══════════════════════════════════════════════════════════════════════

-- ── 1. WEEKLY MARKET DATA ────────────────────────────────────────────────────
create table if not exists beef_weekly (
    week_ending  date        primary key,
    slaughter    numeric,           -- commercial FI slaughter, head/week
    ct150_steer  numeric,           -- 5-area steer avg price, $/cwt
    ct150_heifer numeric,
    ct150_mixed  numeric,
    ct150_all    numeric,
    ks_steer     numeric,           -- Kansas steer avg price, $/cwt
    ks_heifer    numeric,
    ks_avg       numeric,           -- (ks_steer + ks_heifer) / 2
    ne_steer     numeric,           -- Nebraska steer avg price, $/cwt
    ne_heifer    numeric,
    ne_avg       numeric,
    choice       numeric,           -- Choice 600-900 cutout, $/cwt
    select_      numeric,           -- Select 600-900 cutout, $/cwt
    drop_credit  numeric,           -- by-product drop credit, $/head
    henry_hub    numeric,           -- Henry Hub spot price, $/MMBtu
    updated_at   timestamptz default now()
);

-- ── 2. QUARTERLY SUMMARY (market averages + company financials) ──────────────
create table if not exists beef_quarterly (
    quarter          text primary key,   -- "1Q18", "2Q18", …
    quarter_start    date,               -- first day of the quarter (for sorting)

    -- Market variables (quarterly averages of the weekly series)
    slaughter        numeric,
    ct150_steer      numeric,
    ct150_heifer     numeric,
    ct150_mixed      numeric,
    ct150_all        numeric,
    ks_steer         numeric,
    ks_heifer        numeric,
    ks_avg           numeric,
    ne_steer         numeric,
    ne_heifer        numeric,
    ne_avg           numeric,
    choice           numeric,
    select_          numeric,
    drop_credit      numeric,
    henry_hub        numeric,

    -- MBRF / National Beef  (BRL mm, IFRS)
    mbrf_revenue     numeric,
    mbrf_gp          numeric,
    mbrf_gm          numeric,    -- gross margin %  (e.g. 0.075)
    mbrf_ebitda      numeric,
    mbrf_ebitda_mgn  numeric,

    -- JBS North America  (USD mm, US GAAP)
    jbs_revenue      numeric,
    jbs_gp           numeric,
    jbs_gm           numeric,
    jbs_ebit         numeric,
    jbs_ebit_mgn     numeric,
    jbs_ebitda       numeric,
    jbs_ebitda_mgn   numeric,

    -- Tyson Beef  (USD mm, US GAAP)
    tyson_sales      numeric,
    tyson_adj_op_inc numeric,
    tyson_adj_op_mgn numeric,   -- adj. operating margin %

    updated_at  timestamptz default now()
);

-- ── 3. ROW-LEVEL SECURITY ────────────────────────────────────────────────────
-- Tables are readable by the anon key (same as the rest of the portal).
-- Writes require the service-role key (used only by the Python loader/updater).

alter table beef_weekly    enable row level security;
alter table beef_quarterly enable row level security;

-- Drop existing policies if re-running this script
drop policy if exists "anon read weekly"    on beef_weekly;
drop policy if exists "anon read quarterly" on beef_quarterly;

create policy "anon read weekly"
    on beef_weekly for select using (true);

create policy "anon read quarterly"
    on beef_quarterly for select using (true);

-- ── 4. INDEXES ────────────────────────────────────────────────────────────────
create index if not exists beef_weekly_week_ending_idx
    on beef_weekly (week_ending desc);

create index if not exists beef_quarterly_quarter_start_idx
    on beef_quarterly (quarter_start desc);

-- ── 5. DASHBOARD REGISTRY ROW ────────────────────────────────────────────────
-- Insert (or update) the module-5 row in the dashboards table.
-- Adjust title / description / tags as needed.
insert into dashboards (id, sector, subsector, title, description, source, tags,
                        display_order, visible_to_all, coming_soon)
values (
    5,
    'fnb',
    'beef',
    'U.S. Beef Packer Margin Tracker',
    'Quarterly spread model vs. reported gross margins for MBRF / National Beef, JBS North America, and Tyson Beef. Includes market price series (CT150, cutout, KS/NE cattle), drop credit, and energy cost.',
    'USDA AMS · EIA · Company Reports',
    array['Beef','Margins','U.S.','MBRF','JBS','Tyson'],
    1,
    true,
    false
)
on conflict (id) do update
    set sector        = excluded.sector,
        subsector     = excluded.subsector,
        title         = excluded.title,
        description   = excluded.description,
        source        = excluded.source,
        tags          = excluded.tags,
        visible_to_all = excluded.visible_to_all,
        coming_soon   = excluded.coming_soon;
