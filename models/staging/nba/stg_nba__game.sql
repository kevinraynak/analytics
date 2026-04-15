with source as (
    select * from {{ source('nba', 'game') }}
),

renamed as (
    select
        -- identifiers
        game_id,
        season_id,
        game_date,
        replace(season_type, 'All Star', 'All-Star') as season_type,

        -- home team identifiers
        team_id_home,
        team_abbreviation_home,
        team_name_home,
        matchup_home,

        -- home team outcome
        wl_home,
        pts_home,
        plus_minus_home,

        -- home team shooting
        fgm_home,
        fga_home,
        fg_pct_home,
        fg3m_home,
        fg3a_home,
        fg3_pct_home,
        ftm_home,
        fta_home,
        ft_pct_home,

        -- home team other stats
        oreb_home,
        dreb_home,
        reb_home,
        ast_home,
        stl_home,
        blk_home,
        tov_home,
        pf_home,
        min           as minutes_played,
        video_available_home,

        -- away team identifiers
        team_id_away,
        team_abbreviation_away,
        team_name_away,
        matchup_away,

        -- away team outcome
        wl_away,
        pts_away,
        plus_minus_away,

        -- away team shooting
        fgm_away,
        fga_away,
        fg_pct_away,
        fg3m_away,
        fg3a_away,
        fg3_pct_away,
        ftm_away,
        fta_away,
        ft_pct_away,

        -- away team other stats
        oreb_away,
        dreb_away,
        reb_away,
        ast_away,
        stl_away,
        blk_away,
        tov_away,
        pf_away,
        video_available_away

    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by game_id
        order by season_type desc
    ) = 1
)

select * from deduped

