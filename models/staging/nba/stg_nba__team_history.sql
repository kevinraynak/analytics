SELECT
    TEAM_ID,
    CITY,
    NICKNAME,
    YEAR_FOUNDED,
    YEAR_ACTIVE_TILL

from {{ source('dbt_kraynak', 'team_history')}}