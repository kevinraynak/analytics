SELECT
    ID AS TEAM_ID,
    FULL_NAME,
    ABBREVIATION,
    NICKNAME,
    CITY,
    STATE,
    YEAR_FOUNDED

from {{ source('dbt_kraynak', 'team')}}