-- NOTE: This model is a stub. The `team_info_commons` source table surfaces
-- a blend of team metadata that largely overlaps with `team` and `team_details`.
-- Populate this model once the source table is confirmed in Snowflake.
-- For now, downstream models rely on stg_nba__team and stg_nba__team_details.

{{
  config(
    enabled = false
  )
}}

with source as (
    select * from {{ source('nba', 'team_info_commons') }}
),

renamed as (
    select
        team_id,
        team_city,
        team_name,
        team_abbreviation,
        team_conference,
        team_division,
        team_code,
        team_slug,
        w                   as wins,
        l                   as losses,
        pct                 as win_pct,
        conf_rank,
        div_rank,
        min_year,
        max_year

    from source
)

select * from renamed
