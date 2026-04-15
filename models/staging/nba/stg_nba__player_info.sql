with source as (
    select * from {{ source('nba', 'common_player_info') }}
),

renamed as (
    select
        -- identifiers
        person_id               as player_id,
        team_id,

        -- name
        first_name,
        last_name,
        display_first_last      as full_name,

        -- biography
        birthdate,
        country,
        school                  as college,
        last_affiliation,

        -- physical attributes
        height,
        weight,

        -- career info
        season_exp              as seasons_experience,
        jersey,
        position,
        rosterstatus            as roster_status,
        from_year,
        to_year,

        -- team info
        team_name,
        team_abbreviation,
        team_code,
        team_city,

        -- draft info
        draft_year,
        draft_round,
        draft_number,

        -- flags
        games_played_current_season_flag,
        dleague_flag,
        nba_flag,
        games_played_flag,
        greatest_75_flag

    from source
)

select * from renamed
