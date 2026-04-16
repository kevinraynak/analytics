{{
  config(
    materialized = 'table',
    description = 'Draft fact table — one row per drafted player with their draft position, drafting team, pre-draft background, and physical measurements from the combine. Great for scouting and draft analysis demos.'
  )
}}

with draft_history as (
    select * from {{ ref('stg_nba__draft_history') }}
),

draft_history_deduped as (
    select *
    from draft_history
    qualify row_number() over (
        partition by player_id
        order by draft_year desc, overall_pick asc
    ) = 1
),

draft_stats as (
    select * from {{ ref('stg_nba__draft_stats') }}
),

draft_stats_deduped as (
    select *
    from draft_stats
    qualify row_number() over (
        partition by player_id
        order by draft_combine_year desc
    ) = 1
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
    draft_history_deduped.player_id,

    -- player name
    draft_history_deduped.player_name,

    -- draft position
    draft_history_deduped.draft_year,
    draft_history_deduped.draft_round,
    draft_history_deduped.pick_in_round,
    draft_history_deduped.overall_pick,
    draft_history_deduped.draft_type,

    -- drafting team
    draft_history_deduped.team_id,
    draft_history_deduped.team_city,
    draft_history_deduped.team_name,
    draft_history_deduped.team_abbreviation,

    -- pre-draft background
    draft_history_deduped.organization             as pre_draft_school,
    draft_history_deduped.organization_type        as pre_draft_organization_type,

    -- current career info
    player_info.position,
    player_info.country,
    player_info.college,
    player_info.is_active,
    player_info.seasons_experience,
    player_info.greatest_75_flag,

    -- combine measurements
    draft_stats_deduped.height_w_shoes_ft_in       as combine_height,
    draft_stats_deduped.weight                     as combine_weight,
    draft_stats_deduped.wingspan_ft_in             as combine_wingspan,
    draft_stats_deduped.standing_reach_ft_in       as combine_standing_reach,
    draft_stats_deduped.body_fat_pct               as combine_body_fat_pct,

    -- combine athleticism
    draft_stats_deduped.max_vertical_leap          as combine_max_vertical,
    draft_stats_deduped.standing_vertical_leap     as combine_standing_vertical,
    draft_stats_deduped.lane_agility_time          as combine_lane_agility_time,
    draft_stats_deduped.three_quarter_sprint       as combine_sprint_time,
    draft_stats_deduped.bench_press                as combine_bench_press_reps

from draft_history_deduped
left join draft_stats_deduped
    on draft_history_deduped.player_id = draft_stats_deduped.player_id
left join player_info
    on draft_history_deduped.player_id = player_info.player_id
