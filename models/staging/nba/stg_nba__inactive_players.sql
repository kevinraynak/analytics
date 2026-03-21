select
    GAME_ID,
    PLAYER_ID,
    FIRST_NAME,
    LAST_NAME,
    JERSEY_NUM,
    TEAM_ID,
    TEAM_CITY,
    TEAM_NAME,
    TEAM_ABBREVIATION,

from {{ source('dbt_kraynak', 'inactive_players')}}