with source as (
    select * from {{ source('nba', 'game_summary') }}
),

renamed as (
    select
        game_id,
        game_date_est,
        game_sequence,
        game_status_id,
        game_status_text,
        gamecode,
        home_team_id,
        visitor_team_id,
        season,
        live_period,
        live_pc_time,
        natl_tv_broadcaster_abbreviation  as national_tv_network,
        live_period_time_bcast,
        wh_status

    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by game_id
        order by coalesce(wh_status, 0) desc, coalesce(game_status_id, 0) desc, coalesce(live_period, 0) desc
    ) = 1
)

select * from deduped

