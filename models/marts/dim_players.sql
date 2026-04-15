{{
  config(
    materialized = 'table',
    description = 'Player dimension table — one row per NBA player with biographical info, draft details, and combine measurements. Use this as the primary player lookup.'
  )
}}

with players as (
    select * from {{ ref('int_nba__players_enriched') }}
)

select
    -- surrogate-friendly primary key
    player_id,

    -- identity
    full_name,
    first_name,
    last_name,

    -- roster status
    is_active,
    roster_status,
    team_id,
    team_name,
    team_abbreviation,
    team_city,

    -- biography
    birthdate,
    country,
    college,
    last_affiliation,

    -- physical
    height,
    weight,
    position,
    jersey,

    -- career
    seasons_experience,
    from_year,
    to_year,

    -- draft
    draft_year,
    draft_round,
    pick_in_round,
    overall_pick,
    draft_type,
    pre_draft_organization,
    pre_draft_organization_type,

    -- combine measurements
    combine_height,
    combine_weight,
    combine_wingspan,
    combine_standing_reach,
    combine_body_fat_pct,
    combine_max_vertical,
    combine_standing_vertical,
    combine_lane_agility_time,
    combine_sprint_time,
    combine_bench_press_reps,

    -- flags
    greatest_75_flag,
    nba_flag,
    dleague_flag,
    games_played_flag,
    games_played_current_season_flag

from players
