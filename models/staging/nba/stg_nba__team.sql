with source as (
    select * from {{ source('nba', 'team') }}
),

renamed as (
    select
        id              as team_id,
        full_name       as team_name,
        abbreviation    as team_abbreviation,
        nickname        as team_nickname,
        city            as team_city,
        state           as team_state,
        year_founded

    from source
)

select * from renamed
