with source as (
    select * from {{ source('nba', 'player') }}
),

renamed as (
    select
        id          as player_id,
        full_name,
        first_name,
        last_name,
        is_active

    from source
)

select * from renamed
