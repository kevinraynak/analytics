{{
  config(
    materialized = 'table',
    description = 'One row per player per regular season where the player averaged at least 25 points, 5 rebounds, and 5 assists per game. Includes each player\'s total count of qualifying seasons in the dataset.'
  )
}}

with play_by_play as (
    select * from {{ ref('stg_nba__play_by_play') }}
),

games as (
    select
        game_id,
        season,
        season_type
    from {{ ref('int_nba__games_enriched') }}
),

players as (
    select
        player_id,
        full_name as player_name
    from {{ ref('dim_players') }}
),

player_game_stats as (
    select
        play_by_play.game_id,
        games.season,
        play_by_play.player1_id as player_id,
        sum(
            case
                when play_by_play.eventmsgtype = 1
                    and (
                        upper(coalesce(play_by_play.home_description, '')) like '%3PT%'
                        or upper(coalesce(play_by_play.visitor_description, '')) like '%3PT%'
                        or upper(coalesce(play_by_play.neutral_description, '')) like '%3PT%'
                    ) then 3
                when play_by_play.eventmsgtype = 1 then 2
                when play_by_play.eventmsgtype = 3 then 1
                else 0
            end
        ) as points,
        sum(case when play_by_play.eventmsgtype = 4 then 1 else 0 end) as rebounds,
        0 as assists

    from play_by_play
    inner join games
        on play_by_play.game_id = games.game_id

    where games.season_type = 'Regular Season'
      and play_by_play.player1_id is not null
      and play_by_play.player1_id != 0
      and play_by_play.eventmsgtype in (1, 3, 4)

    group by 1, 2, 3

    union all

    select
        play_by_play.game_id,
        games.season,
        play_by_play.player2_id as player_id,
        0 as points,
        0 as rebounds,
        sum(case when play_by_play.eventmsgtype = 1 then 1 else 0 end) as assists

    from play_by_play
    inner join games
        on play_by_play.game_id = games.game_id

    where games.season_type = 'Regular Season'
      and play_by_play.player2_id is not null
      and play_by_play.player2_id != 0
      and play_by_play.eventmsgtype = 1

    group by 1, 2, 3
),

player_season_stats as (
    select
        player_game_stats.player_id,
        players.player_name,
        player_game_stats.season,
        count(distinct player_game_stats.game_id) as games_played,
        sum(player_game_stats.points) as total_points,
        sum(player_game_stats.assists) as total_assists,
        sum(player_game_stats.rebounds) as total_rebounds,
        round(sum(player_game_stats.points) * 1.0 / nullif(count(distinct player_game_stats.game_id), 0), 2) as points_per_game,
        round(sum(player_game_stats.assists) * 1.0 / nullif(count(distinct player_game_stats.game_id), 0), 2) as assists_per_game,
        round(sum(player_game_stats.rebounds) * 1.0 / nullif(count(distinct player_game_stats.game_id), 0), 2) as rebounds_per_game

    from player_game_stats
    left join players
        on player_game_stats.player_id = players.player_id

    group by 1, 2, 3
),

qualified_seasons as (
    select
        *,
        count(*) over (partition by player_id) as times_25_5_5_seasons
    from player_season_stats
    where points_per_game >= 25
      and assists_per_game >= 5
      and rebounds_per_game >= 5
)

select
    player_id,
    player_name,
    season,
    games_played,
    total_points,
    total_assists,
    total_rebounds,
    points_per_game,
    assists_per_game,
    rebounds_per_game,
    times_25_5_5_seasons
from qualified_seasons
order by times_25_5_5_seasons desc, player_name, season desc
