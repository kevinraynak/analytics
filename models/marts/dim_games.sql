{{
  config(
    materialized = 'table',
    description = 'Game dimension table — one row per NBA game with full context: season, matchup, TV info, and attendance. Use this as the game lookup for joining to fact tables.'
  )
}}

with games as (
    select * from {{ ref('int_nba__games_enriched') }}
),

officials as (
    select
        game_id,
        listagg(full_name, ', ') within group (order by jersey_number) as officials_crew
    from {{ ref('stg_nba__officials') }}
    group by game_id
)

select
    -- primary key
    games.game_id,

    -- time
    games.game_date,
    extract(year from games.game_date)      as game_year,
    extract(month from games.game_date)     as game_month,
    extract(dayofweek from games.game_date) as game_day_of_week,
    games.season_id,
    games.season,
    games.season_type,
    games.game_time,

    -- matchup
    games.matchup_home                      as matchup,
    games.team_id_home,
    games.team_name_home,
    games.team_abbreviation_home,
    games.team_id_away,
    games.team_name_away,
    games.team_abbreviation_away,

    -- winner
    games.winning_team_id,
    games.winning_team_name,

    -- game context
    games.game_status_text,
    games.gamecode,
    games.attendance,
    games.national_tv_network,
    games.went_to_overtime,
    games.point_differential,

    -- officials
    officials.officials_crew

from games
left join officials
    on games.game_id = officials.game_id
