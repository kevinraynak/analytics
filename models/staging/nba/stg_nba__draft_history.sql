with source as (
    select * from {{ source('nba', 'draft_history') }}
),

renamed as (
    select
        person_id           as player_id,
        player_name,
        season              as draft_year,
        round_number        as draft_round,
        round_pick          as pick_in_round,
        overall_pick,
        draft_type,
        team_id,
        team_city,
        team_name,
        team_abbreviation,
        organization,
        organization_type,
        player_profile_flag

    from source
)

select * from renamed
