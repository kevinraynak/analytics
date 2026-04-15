{{
  config(
    materialized = 'table',
    description = 'Team dimension table — one row per NBA franchise with current team info, arena details, and leadership. Use this as the primary team lookup.'
  )
}}

with teams as (
    select * from {{ ref('int_nba__teams_enriched') }}
),

-- Count games played per team for enrichment
team_game_counts as (
    select
        team_id,
        count(distinct game_id)                             as total_games_played,
        sum(is_win)                                         as total_wins,
        count(distinct game_id) - sum(is_win)               as total_losses,
        round(sum(is_win)::float / nullif(count(distinct game_id), 0), 3) as all_time_win_pct,
        min(game_date)                                      as first_game_date,
        max(game_date)                                      as last_game_date

    from {{ ref('int_nba__team_game_stats') }}
    group by team_id
)

select
    -- primary key
    teams.team_id,

    -- identity
    teams.team_name,
    teams.team_abbreviation,
    teams.team_nickname,
    teams.team_city,
    teams.team_state,
    teams.year_founded,

    -- arena
    teams.arena,
    teams.arena_capacity,

    -- leadership
    teams.owner,
    teams.general_manager,
    teams.head_coach,

    -- affiliates & social
    teams.g_league_affiliate,
    teams.facebook,
    teams.instagram,
    teams.twitter,

    -- all-time record (from game data)
    team_game_counts.total_games_played,
    team_game_counts.total_wins,
    team_game_counts.total_losses,
    team_game_counts.all_time_win_pct,
    team_game_counts.first_game_date,
    team_game_counts.last_game_date

from teams
left join team_game_counts
    on teams.team_id = team_game_counts.team_id
