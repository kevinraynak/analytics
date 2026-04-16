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

player_deduped as (
    select *
    from player
    qualify row_number() over (
        partition by player_id
        order by is_active desc, full_name
    ) = 1
),

draft_history as (
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
)

select
    -- primary key
    player_info.player_id,

    -- name
    player_info.full_name,
    player_info.first_name,
    player_info.last_name,

    -- roster status (use player table for is_active flag)
    player_deduped.is_active,
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
    draft_history_deduped.draft_year,
    draft_history_deduped.draft_round,
    draft_history_deduped.pick_in_round,
    draft_history_deduped.overall_pick,
    draft_history_deduped.draft_type,
    draft_history_deduped.organization               as pre_draft_organization,
    draft_history_deduped.organization_type          as pre_draft_organization_type,

    -- draft combine measurements
    draft_stats_deduped.height_w_shoes_ft_in         as combine_height,
    draft_stats_deduped.weight                       as combine_weight,
    draft_stats_deduped.wingspan_ft_in               as combine_wingspan,
    draft_stats_deduped.standing_reach_ft_in         as combine_standing_reach,
    draft_stats_deduped.body_fat_pct                 as combine_body_fat_pct,

    -- draft combine athleticism
    draft_stats_deduped.max_vertical_leap            as combine_max_vertical,
    draft_stats_deduped.standing_vertical_leap       as combine_standing_vertical,
    draft_stats_deduped.lane_agility_time            as combine_lane_agility_time,
    draft_stats_deduped.three_quarter_sprint         as combine_sprint_time,
    draft_stats_deduped.bench_press                  as combine_bench_press_reps,

    -- flags
    player_info.greatest_75_flag,
    player_info.nba_flag,
    player_info.dleague_flag,
    player_info.games_played_flag,
    player_info.games_played_current_season_flag

from player_info
left join player_deduped
    on player_info.player_id = player_deduped.player_id
left join draft_history_deduped
    on player_info.player_id = draft_history_deduped.player_id
left join draft_stats_deduped
    on player_info.player_id = draft_stats_deduped.player_id
