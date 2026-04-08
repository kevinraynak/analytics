SELECT
    ID AS PLAYER_ID,
    FULL_NAME,
    FIRST_NAME,
    LAST_NAME,
    IS_ACTIVE

from {{ source('dbt_kraynak', 'player')}}