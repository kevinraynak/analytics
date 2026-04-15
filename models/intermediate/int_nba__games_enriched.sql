{{
  config(
    materialized = 'view',
    description = 'Enriched game-level model combining box score stats, game metadata, quarter scores, and advanced stats.'
  )
}}

with game as (
    select * from {{ ref('stg_nba__game') }}
),

game_info as (
    select * from {{ ref('stg_nba__game_info') }}
),

game_summary as (
    select * from {{ ref('stg_nba__game_summary') }}
),

line_score as (
    select * from {{ ref('stg_nba__line_score') }}
),

other_stats as (
    select * from {{ ref('stg_nba__other_stats') }}
)

select
    -- game identifiers
    game.game_id,
    game.game_date,
    game.season_id,
    game.season_type,
    game_summary.season,
    game_summary.gamecode,
    game_summary.game_status_text,
    game_summary.national_tv_network,

    -- attendance & logistics
    game_info.attendance,
    game_info.game_time,

    -- home team identifiers
    game.team_id_home,
    game.team_name_home,
    game.team_abbreviation_home,
    game.matchup_home,

    -- home team outcome
    game.wl_home,
    game.pts_home,
    game.plus_minus_home,

    -- home team shooting
    game.fgm_home,
    game.fga_home,
    game.fg_pct_home,
    game.fg3m_home,
    game.fg3a_home,
    game.fg3_pct_home,
    game.ftm_home,
    game.fta_home,
    game.ft_pct_home,

    -- home team other stats
    game.oreb_home,
    game.dreb_home,
    game.reb_home,
    game.ast_home,
    game.stl_home,
    game.blk_home,
    game.tov_home,
    game.pf_home,

    -- away team identifiers
    game.team_id_away,
    game.team_name_away,
    game.team_abbreviation_away,
    game.matchup_away,

    -- away team outcome
    game.wl_away,
    game.pts_away,
    game.plus_minus_away,

    -- away team shooting
    game.fgm_away,
    game.fga_away,
    game.fg_pct_away,
    game.fg3m_away,
    game.fg3a_away,
    game.fg3_pct_away,
    game.ftm_away,
    game.fta_away,
    game.ft_pct_away,

    -- away team other stats
    game.oreb_away,
    game.dreb_away,
    game.reb_away,
    game.ast_away,
    game.stl_away,
    game.blk_away,
    game.tov_away,
    game.pf_away,

    -- quarter scores (home)
    line_score.pts_qtr1_home,
    line_score.pts_qtr2_home,
    line_score.pts_qtr3_home,
    line_score.pts_qtr4_home,
    line_score.pts_ot1_home,
    line_score.pts_ot2_home,
    line_score.pts_ot3_home,
    line_score.pts_ot4_home,

    -- quarter scores (away)
    line_score.pts_qtr1_away,
    line_score.pts_qtr2_away,
    line_score.pts_qtr3_away,
    line_score.pts_qtr4_away,
    line_score.pts_ot1_away,
    line_score.pts_ot2_away,
    line_score.pts_ot3_away,
    line_score.pts_ot4_away,

    -- advanced game stats (home)
    other_stats.pts_paint_home,
    other_stats.pts_2nd_chance_home,
    other_stats.pts_fast_break_home,
    other_stats.largest_lead_home,
    other_stats.team_turnovers_home,
    other_stats.total_turnovers_home,
    other_stats.team_rebounds_home,
    other_stats.pts_off_turnovers_home,

    -- advanced game stats (away)
    other_stats.pts_paint_away,
    other_stats.pts_2nd_chance_away,
    other_stats.pts_fast_break_away,
    other_stats.largest_lead_away,
    other_stats.team_turnovers_away,
    other_stats.total_turnovers_away,
    other_stats.team_rebounds_away,
    other_stats.pts_off_turnovers_away,

    -- game-level momentum stats
    other_stats.lead_changes,
    other_stats.times_tied,

    -- derived fields
    case
        when game.pts_home > game.pts_away then game.team_id_home
        else game.team_id_away
    end                         as winning_team_id,

    case
        when game.pts_home > game.pts_away then game.team_name_home
        else game.team_name_away
    end                         as winning_team_name,

    abs(game.pts_home - game.pts_away) as point_differential,

    case
        when (coalesce(line_score.pts_ot1_home, 0) + coalesce(line_score.pts_ot1_away, 0)) > 0
        then true
        else false
    end                         as went_to_overtime

from game
left join game_info
    on game.game_id = game_info.game_id
left join game_summary
    on game.game_id = game_summary.game_id
left join line_score
    on game.game_id = line_score.game_id
left join other_stats
    on game.game_id = other_stats.game_id
