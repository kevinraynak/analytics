{{
  config(
    materialized = 'view',
    description = 'Enriched player profile joining biographical info, active roster status, draft history, and draft combine measurements.'
  )
}}

with player_info as (
    select * from {{ ref('stg_nba__player_info') }}
),

player as (
    select * from {{ ref('stg_nba__player') }}
),

draft_history as (
    select * from {{ ref('stg_nba__draft_history') }}
),

draft_stats as (
    select * from {{ ref('stg_nba__draft_stats') }}
)

select
    -- primary key
    player_info.player_id,

    -- name
    player_info.full_name,
    player_info.first_name,
    player_info.last_name,

    -- roster status (use player table for is_active flag)
    player.is_active,
    player_info.roster_status,
    player_info.team_id,
    player_info.team_name,
    player_info.team_abbreviation,
    player_info.team_city,

    -- biography
    player_info.birthdate,
    player_info.country,
    player_info.college,
    player_info.last_affiliation,

    -- physical attributes
    player_info.height,
    player_info.weight,

    -- career details
    player_info.position,
    player_info.jersey,
    player_info.seasons_experience,
    player_info.from_year,
    player_info.to_year,

    -- draft info from history
    draft_history.draft_year,
    draft_history.draft_round,
    draft_history.pick_in_round,
    draft_history.overall_pick,
    draft_history.draft_type,
    draft_history.organization               as pre_draft_organization,
    draft_history.organization_type          as pre_draft_organization_type,

    -- draft combine measurements
    draft_stats.height_w_shoes_ft_in        as combine_height,
    draft_stats.weight                      as combine_weight,
    draft_stats.wingspan_ft_in              as combine_wingspan,
    draft_stats.standing_reach_ft_in        as combine_standing_reach,
    draft_stats.body_fat_pct               as combine_body_fat_pct,

    -- draft combine athleticism
    draft_stats.max_vertical_leap           as combine_max_vertical,
    draft_stats.standing_vertical_leap      as combine_standing_vertical,
    draft_stats.lane_agility_time           as combine_lane_agility_time,
    draft_stats.three_quarter_sprint        as combine_sprint_time,
    draft_stats.bench_press                 as combine_bench_press_reps,

    -- flags
    player_info.greatest_75_flag,
    player_info.nba_flag,
    player_info.dleague_flag,
    player_info.games_played_flag,
    player_info.games_played_current_season_flag

from player_info
left join player
    on player_info.player_id = player.player_id
left join draft_history
    on player_info.player_id = draft_history.player_id
left join draft_stats
    on player_info.player_id = draft_stats.player_id
