with source as (
    select * from {{ source('nba', 'play_by_play') }}
),

renamed as (
    select
        game_id,
        eventnum,
        eventmsgtype,
        eventmsgactiontype,
        period,
        wctimestring            as wall_clock_time,
        pctimestring            as game_clock_time,
        homedescription         as home_description,
        neutraldescription      as neutral_description,
        visitordescription      as visitor_description,
        score,
        scoremargin             as score_margin,

        -- player 1 (primary actor)
        person1type             as player1_type,
        player1_id,
        player1_name,
        player1_team_id,
        player1_team_city,
        player1_team_nickname,
        player1_team_abbreviation,

        -- player 2 (secondary actor)
        person2type             as player2_type,
        player2_id,
        player2_name,
        player2_team_id,
        player2_team_city,
        player2_team_nickname,
        player2_team_abbreviation,

        -- player 3 (tertiary actor)
        person3type             as player3_type,
        player3_id,
        player3_name,
        player3_team_id,
        player3_team_city,
        player3_team_nickname,
        player3_team_abbreviation,

        video_available_flag

    from source
)

select * from renamed
