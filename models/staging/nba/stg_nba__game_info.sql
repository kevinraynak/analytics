with source as (
    select * from {{ source('nba', 'game_info') }}
),

renamed as (
    select
        game_id,
        game_date,
        attendance,
        game_time

    from source
)

select distinct * from renamed

