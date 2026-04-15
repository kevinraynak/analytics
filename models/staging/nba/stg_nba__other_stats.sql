with source as (
    select * from {{ source('nba', 'other_stats') }}
),

renamed as (
    select
        game_id,
        league_id,

        -- home team
        team_id_home,
        team_abbreviation_home,
        team_city_home,
        pts_paint_home,
        pts_2nd_chance_home,
        pts_fb_home             as pts_fast_break_home,
        largest_lead_home,
        team_turnovers_home,
        total_turnovers_home,
        team_rebounds_home,
        pts_off_to_home         as pts_off_turnovers_home,

        -- away team
        team_id_away,
        team_abbreviation_away,
        team_city_away,
        pts_paint_away,
        pts_2nd_chance_away,
        pts_fb_away             as pts_fast_break_away,
        largest_lead_away,
        team_turnovers_away,
        total_turnovers_away,
        team_rebounds_away,
        pts_off_to_away         as pts_off_turnovers_away,

        -- game-level
        lead_changes,
        times_tied

    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by game_id
        order by coalesce(league_id, 0) desc
    ) = 1
)

select * from deduped

