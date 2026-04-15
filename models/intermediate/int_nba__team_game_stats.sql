{{
  config(
    materialized = 'view',
    description = 'Unpivots game box scores from a home/away wide format into one row per team per game. This makes team performance analysis significantly easier.'
  )
}}

-- Home team perspective
with home_team as (
    select
        game_id,
        game_date,
        season_id,
        season_type,
        'home'                  as home_away,
        team_id_home            as team_id,
        team_name_home          as team_name,
        team_abbreviation_home  as team_abbreviation,
        matchup_home            as matchup,
        wl_home                 as win_loss,
        pts_home                as pts,
        fgm_home                as fgm,
        fga_home                as fga,
        fg_pct_home             as fg_pct,
        fg3m_home               as fg3m,
        fg3a_home               as fg3a,
        fg3_pct_home            as fg3_pct,
        ftm_home                as ftm,
        fta_home                as fta,
        ft_pct_home             as ft_pct,
        oreb_home               as oreb,
        dreb_home               as dreb,
        reb_home                as reb,
        ast_home                as ast,
        stl_home                as stl,
        blk_home                as blk,
        tov_home                as tov,
        pf_home                 as pf,
        plus_minus_home         as plus_minus,
        team_id_away            as opponent_team_id,
        team_name_away          as opponent_team_name,
        team_abbreviation_away  as opponent_team_abbreviation,
        pts_away                as opponent_pts

    from {{ ref('stg_nba__game') }}
),

-- Away team perspective
away_team as (
    select
        game_id,
        game_date,
        season_id,
        season_type,
        'away'                  as home_away,
        team_id_away            as team_id,
        team_name_away          as team_name,
        team_abbreviation_away  as team_abbreviation,
        matchup_away            as matchup,
        wl_away                 as win_loss,
        pts_away                as pts,
        fgm_away                as fgm,
        fga_away                as fga,
        fg_pct_away             as fg_pct,
        fg3m_away               as fg3m,
        fg3a_away               as fg3a,
        fg3_pct_away            as fg3_pct,
        ftm_away                as ftm,
        fta_away                as fta,
        ft_pct_away             as ft_pct,
        oreb_away               as oreb,
        dreb_away               as dreb,
        reb_away                as reb,
        ast_away                as ast,
        stl_away                as stl,
        blk_away                as blk,
        tov_away                as tov,
        pf_away                 as pf,
        plus_minus_away         as plus_minus,
        team_id_home            as opponent_team_id,
        team_name_home          as opponent_team_name,
        team_abbreviation_home  as opponent_team_abbreviation,
        pts_home                as opponent_pts

    from {{ ref('stg_nba__game') }}
),

unioned as (
    select * from home_team
    union all
    select * from away_team
)

select
    game_id,
    team_id,
    game_date,
    season_id,
    season_type,
    home_away,
    team_name,
    team_abbreviation,
    matchup,
    opponent_team_id,
    opponent_team_name,
    opponent_team_abbreviation,

    -- outcome
    win_loss,
    case when win_loss = 'W' then 1 else 0 end  as is_win,
    pts,
    opponent_pts,
    pts - opponent_pts                           as point_differential,
    plus_minus,

    -- shooting
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,

    -- rebounds & other stats
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    tov,
    pf

from unioned
