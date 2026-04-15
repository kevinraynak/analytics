with source as (
    select * from {{ source('nba', 'inactive_players') }}
),

renamed as (
    select
        game_id,
        player_id,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        jersey_num                      as jersey_number,
        team_id,
        team_city,
        team_name,
        team_abbreviation

    from source
)

select * from renamed
