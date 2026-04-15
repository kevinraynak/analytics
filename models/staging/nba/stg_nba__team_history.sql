with source as (
    select * from {{ source('nba', 'team_history') }}
),

renamed as (
    select
        team_id,
        city,
        nickname,
        year_founded,
        year_active_till

    from source
)

select * from renamed
