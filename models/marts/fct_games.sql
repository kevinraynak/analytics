{{
  config(
    materialized = 'table',
    description = 'Core game fact table — one row per game with both team box scores side-by-side, quarter scores, advanced stats, and derived metrics. The primary fact table for game-level analysis.'
  )
}}

with games as (
    select * from {{ ref('int_nba__games_enriched') }}
)

select
    -- grain: one row per game
    game_id,
    game_date,
    season_id,
    season,
    season_type,
    national_tv_network,
    attendance,
    went_to_overtime,

    -- home team
    team_id_home,
    team_name_home,
    team_abbreviation_home,
    wl_home,
    pts_home,
    plus_minus_home,
    fgm_home,
    fga_home,
    fg_pct_home,
    fg3m_home,
    fg3a_home,
    fg3_pct_home,
    ftm_home,
    fta_home,
    ft_pct_home,
    oreb_home,
    dreb_home,
    reb_home,
    ast_home,
    stl_home,
    blk_home,
    tov_home,
    pf_home,

    -- home team advanced
    pts_paint_home,
    pts_2nd_chance_home,
    pts_fast_break_home,
    pts_off_turnovers_home,
    largest_lead_home,
    team_turnovers_home,
    total_turnovers_home,

    -- away team
    team_id_away,
    team_name_away,
    team_abbreviation_away,
    wl_away,
    pts_away,
    plus_minus_away,
    fgm_away,
    fga_away,
    fg_pct_away,
    fg3m_away,
    fg3a_away,
    fg3_pct_away,
    ftm_away,
    fta_away,
    ft_pct_away,
    oreb_away,
    dreb_away,
    reb_away,
    ast_away,
    stl_away,
    blk_away,
    tov_away,
    pf_away,

    -- away team advanced
    pts_paint_away,
    pts_2nd_chance_away,
    pts_fast_break_away,
    pts_off_turnovers_away,
    largest_lead_away,
    team_turnovers_away,
    total_turnovers_away,

    -- quarter scores (home)
    pts_qtr1_home,
    pts_qtr2_home,
    pts_qtr3_home,
    pts_qtr4_home,
    pts_ot1_home,
    pts_ot2_home,
    pts_ot3_home,
    pts_ot4_home,

    -- quarter scores (away)
    pts_qtr1_away,
    pts_qtr2_away,
    pts_qtr3_away,
    pts_qtr4_away,
    pts_ot1_away,
    pts_ot2_away,
    pts_ot3_away,
    pts_ot4_away,

    -- game-level metrics
    lead_changes,
    times_tied,

    -- derived
    winning_team_id,
    winning_team_name,
    point_differential,

    -- total points scored in game
    pts_home + pts_away         as total_pts_scored

from games
