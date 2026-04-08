SELECT
    GAME_ID,
    OFFICIAL_ID,
    FIRST_NAME,
    LAST_NAME,
    JERSEY_NUM

from {{ source('dbt_kraynak', 'officials')}}