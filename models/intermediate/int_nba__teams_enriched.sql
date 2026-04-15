{{
  config(
    materialized = 'view',
    description = 'Enriched team dimension combining the master team list with detailed team info (arena, ownership, coaching staff, social media).'
  )
}}

with team as (
    select * from {{ ref('stg_nba__team') }}
),

team_details as (
    select * from {{ ref('stg_nba__team_details') }}
)

select
    -- primary key
    team.team_id,

    -- team identity
    team.team_name,
    team.team_abbreviation,
    team.team_nickname,
    team.team_city,
    team.team_state,
    team.year_founded,

    -- arena info
    team_details.arena,
    team_details.arena_capacity,

    -- leadership
    team_details.owner,
    team_details.general_manager,
    team_details.head_coach,

    -- affiliates
    team_details.g_league_affiliate,

    -- social media
    team_details.facebook,
    team_details.instagram,
    team_details.twitter

from team
left join team_details
    on team.team_id = team_details.team_id
