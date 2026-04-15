with source as (
    select * from {{ source('nba', 'team_details') }}
),

renamed as (
    select
        team_id,
        abbreviation        as team_abbreviation,
        nickname            as team_nickname,
        yearfounded         as year_founded,
        city                as team_city,
        arena,
        arenacapacity       as arena_capacity,
        replace(owner, char(160), ' ') as owner,
        generalmanager      as general_manager,
        headcoach           as head_coach,
        dleagueaffiliation  as g_league_affiliate,
        facebook,
        instagram,
        twitter

    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by team_id
        order by owner desc
    ) = 1
)

select * from deduped

