select
    GAME_ID,
    GAME_DATE,
    ATTENDANCE,
    GAME_TIME

from {{ source('dbt_kraynak', 'game_info')}}