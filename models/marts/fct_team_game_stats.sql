{{
  config(
    materialized = 'table',
    description = 'Team-game fact table — one row per team per game. Ideal for team performance analysis, season standings, and rolling stats. Built from the unpivoted intermediate model.'
  )
}}

with team_game_stats as (
    select * from {{ ref('int_nba__team_game_stats') }}
),

game_info as (
    select
        game_id,
        attendance,
        national_tv_network,
        went_to_overtime,
        season,
        gamecode

    from {{ ref('int_nba__games_enriched') }}
)

select
    -- grain: one row per team per game
    team_game_stats.game_id,
    team_game_stats.team_id,
    team_game_stats.game_date,
    team_game_stats.season_id,
    game_info.season,
    team_game_stats.season_type,
    team_game_stats.home_away,

    -- team
    team_game_stats.team_name,
    team_game_stats.team_abbreviation,
    team_game_stats.matchup,

    -- opponent
    team_game_stats.opponent_team_id,
    team_game_stats.opponent_team_name,
    team_game_stats.opponent_team_abbreviation,

    -- outcome
    team_game_stats.win_loss,
    team_game_stats.is_win,
    team_game_stats.pts,
    team_game_stats.opponent_pts,
    team_game_stats.point_differential,
    team_game_stats.plus_minus,

    -- shooting
    team_game_stats.fgm,
    team_game_stats.fga,
    team_game_stats.fg_pct,
    team_game_stats.fg3m,
    team_game_stats.fg3a,
    team_game_stats.fg3_pct,
    team_game_stats.ftm,
    team_game_stats.fta,
    team_game_stats.ft_pct,

    -- other stats
    team_game_stats.oreb,
    team_game_stats.dreb,
    team_game_stats.reb,
    team_game_stats.ast,
    team_game_stats.stl,
    team_game_stats.blk,
    team_game_stats.tov,
    team_game_stats.pf,

    -- game context
    game_info.attendance,
    game_info.national_tv_network,
    game_info.went_to_overtime,
    game_info.gamecode

from team_game_stats
left join game_info
    on team_game_stats.game_id = game_info.game_id
