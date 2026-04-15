# dbt NBA Analytics

A production-grade dbt project built on top of the NBA API dataset loaded into Snowflake. Designed as a client-facing demo showcasing dbt best practices end-to-end — from raw source data through to analytics-ready mart models.

## Project Overview

This project transforms raw NBA data (games, players, teams, draft history) into clean, documented, and tested analytics models organized into three layers.

## Data Sources

All raw data lives in the `dbt_kraynak` schema of the `analytics` Snowflake database. Source tables include:

- `game` — box score stats for every game (home & away)
- `game_info` — attendance and game time
- `game_summary` — matchup metadata and TV network
- `line_score` — quarter-by-quarter scoring
- `other_stats` — paint points, fast break pts, lead changes
- `play_by_play` — event-level play log
- `officials` — referee assignments
- `inactive_players` — did-not-dress player lists
- `player` — master player list
- `common_player_info` — biographical and career info
- `team` — master team list
- `team_details` — arena, ownership, coaching staff
- `team_history` — franchise relocation history
- `draft_history` — all-time draft picks
- `draft_combine_stats` — physical measurements and athleticism tests

## Model Architecture

```
sources (Snowflake)
    └── staging/nba/          (views — light cleaning, column renaming, lowercase)
            └── intermediate/nba/   (views — joins, unpivoting, enrichment)
                    └── marts/core/        (tables — analytics-ready dimensions & facts)
```

### Staging (`stg_nba__*`)

One model per source table. Each model:
- Renames columns to snake_case
- Applies meaningful aliases (e.g., `min` → `minutes_played`, `wl_home` → preserved for clarity)
- Adds derived columns where obvious (e.g., `full_name = first_name || ' ' || last_name`)
- References sources via `{{ source('nba', 'table_name') }}`

### Intermediate (`int_nba__*`)

Business logic and joins live here:

| Model | Description |
|---|---|
| `int_nba__games_enriched` | Joins game box scores with metadata, quarter scores, and advanced stats. Adds derived fields (winner, OT flag, point differential). |
| `int_nba__team_game_stats` | Unpivots home/away game format into one row per team per game. |
| `int_nba__players_enriched` | Joins player info with draft history, combine stats, and active roster status. |
| `int_nba__teams_enriched` | Joins team master list with arena and leadership details. |

### Marts (`dim_*` and `fct_*`)

Analytics-ready, documented, and tested tables:

| Model | Grain | Description |
|---|---|---|
| `dim_players` | 1 row per player | Full player profile with draft details and combine measurements |
| `dim_teams` | 1 row per team | Team info with arena, leadership, and all-time record |
| `dim_games` | 1 row per game | Game context, TV info, attendance, officials |
| `fct_games` | 1 row per game | Complete box score fact table with advanced stats |
| `fct_team_game_stats` | 1 row per team per game | Team performance, easily queryable without home/away pivoting |
| `fct_draft` | 1 row per drafted player | Draft position, combine measurements, career outcome |

## dbt Best Practices Applied

- **Naming conventions**: `stg_<source>__<entity>`, `int_<domain>__<description>`, `dim_<entity>`, `fct_<event>`
- **Source freshness**: All sources defined in `src_nba.yml` with descriptions and tests
- **Documentation**: Every model and key column has a description in YAML
- **Testing**: `unique`, `not_null`, and `accepted_values` tests on primary keys and flag columns
- **Materializations**: Staging/Intermediate as views; Marts as tables
- **CTE pattern**: All models use `with source as (...)` and `with renamed as (...)` for readability
- **Schema separation**: Each layer writes to its own schema (`staging`, `intermediate`, `marts`)

## Running the Project

```bash
# Run all models
dbt run

# Run only marts
dbt run --select marts

# Run staging + everything downstream
dbt run --select staging+

# Run tests
dbt test

# Build (run + test) a specific model and all its dependencies
dbt build --select +fct_team_game_stats
```
