select
    PERSON_ID,
    PLAYER_NAME,
    SEASON,
    ROUND_NUMBER,
    ROUND_PICK,
    OVERALL_PICK,
    DRAFT_TYPE,
    TEAM_ID,
    TEAM_CITY,
    TEAM_NAME,
    TEAM_ABBREVIATION,
    ORGANIZATION,
    ORGANIZATION_TYPE,
    PLAYER_PROFILE_FLAG

from {{ source('dbt_kraynak', 'draft_history')}}