{{
  config(
    materialized = 'table',
    description = 'Draft fact table — one row per drafted player with their draft position, drafting team, pre-draft background, and physical measurements from the combine. Great for scouting and draft analysis demos.'
  )
}}

with draft_history as (
    select * from {{ ref('stg_nba__draft_history') }}
),

draft_stats as (
    select * from {{ ref('stg_nba__draft_stats') }}
),

player_info as (
    select
        player_id,
        full_name,
        position,
        country,
        college,
        is_active,
        seasons_experience,
        greatest_75_flag

    from {{ ref('int_nba__players_enriched') }}
)

select
    -- primary key
    draft_history.player_id,

    -- player name
    draft_history.player_name,

    -- draft position
    draft_history.draft_year,
    draft_history.draft_round,
    draft_history.pick_in_round,
    draft_history.overall_pick,
    draft_history.draft_type,

    -- drafting team
    draft_history.team_id,
    draft_history.team_city,
    draft_history.team_name,
    draft_history.team_abbreviation,

    -- pre-draft background
    draft_history.organization            as pre_draft_school,
    draft_history.organization_type       as pre_draft_organization_type,

    -- current career info
    player_info.position,
    player_info.country,
    player_info.college,
    player_info.is_active,
    player_info.seasons_experience,
    player_info.greatest_75_flag,

    -- combine measurements
    draft_stats.height_w_shoes_ft_in      as combine_height,
    draft_stats.weight                    as combine_weight,
    draft_stats.wingspan_ft_in            as combine_wingspan,
    draft_stats.standing_reach_ft_in      as combine_standing_reach,
    draft_stats.body_fat_pct             as combine_body_fat_pct,

    -- combine athleticism
    draft_stats.max_vertical_leap         as combine_max_vertical,
    draft_stats.standing_vertical_leap    as combine_standing_vertical,
    draft_stats.lane_agility_time         as combine_lane_agility_time,
    draft_stats.three_quarter_sprint      as combine_sprint_time,
    draft_stats.bench_press               as combine_bench_press_reps

from draft_history
left join draft_stats
    on draft_history.player_id = draft_stats.player_id
left join player_info
    on draft_history.player_id = player_info.player_id
