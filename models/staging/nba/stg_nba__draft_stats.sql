with source as (
    select * from {{ source('nba', 'draft_combine_stats') }}
),

renamed as (
    select
        -- identifiers
        player_id,
        season              as draft_combine_year,
        first_name,
        last_name,
        player_name,
        position,

        -- physical measurements
        height_wo_shoes,
        height_wo_shoes_ft_in,
        height_w_shoes,
        height_w_shoes_ft_in,
        weight,
        wingspan,
        wingspan_ft_in,
        standing_reach,
        standing_reach_ft_in,
        body_fat_pct,
        hand_length,
        hand_width,

        -- athleticism tests
        standing_vertical_leap,
        max_vertical_leap,
        lane_agility_time,
        modified_lane_agility_time,
        three_quarter_sprint,
        bench_press,

        -- shooting — spot-up 15 ft
        spot_fifteen_corner_left,
        spot_fifteen_break_left,
        spot_fifteen_top_key,
        spot_fifteen_break_right,
        spot_fifteen_corner_right,

        -- shooting — spot-up college 3pt
        spot_college_corner_left,
        spot_college_break_left,
        spot_college_top_key,
        spot_college_break_right,
        spot_college_corner_right,

        -- shooting — spot-up NBA 3pt
        spot_nba_corner_left,
        spot_nba_break_left,
        spot_nba_top_key,
        spot_nba_break_right,
        spot_nba_corner_right,

        -- shooting — off-dribble 15 ft
        off_drib_fifteen_break_left,
        off_drib_fifteen_top_key,
        off_drib_fifteen_break_right,

        -- shooting — off-dribble college 3pt
        off_drib_college_break_left,
        off_drib_college_top_key,
        off_drib_college_break_right,

        -- on-move shooting
        on_move_fifteen,
        on_move_college

    from source
)

select * from renamed
