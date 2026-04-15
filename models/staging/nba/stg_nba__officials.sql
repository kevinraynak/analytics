with source as (
    select * from {{ source('nba', 'officials') }}
),

renamed as (
    select
        official_id,
        game_id,
        first_name,
        last_name,
        first_name || ' ' || last_name   as full_name,
        jersey_num                       as jersey_number

    from source
)

select * from renamed
