WITH players as (
    select * from dbt_staging.stg_player
)
, spine AS (
    select 
        e.name,
        e.id as event_id,
        p.id as player_id
    from {{ ref('stg_events') }} e 
    cross join {{ ref('stg_player') }} p
)
, player_goals_per_event AS (
    select 
        player_id, 
        event, 
        sum(player_value) as goals_scored
    from {{ ref('stg_fixture_statistics') }} 
    where statistic = 'goals_scored'
    group by player_id, event
)

, most_goals_scored_of_any_player as (
    select max(stg_player.goals_scored) as most_goals_in_season from dbt_staging.stg_player
)

select 
       {{ dbt_utils.generate_surrogate_key(['s.event_id']) }} as event_key_hash,
       {{ dbt_utils.generate_surrogate_key(['s.player_id']) }} as player_key_hash,
       --s.name, -- name of gameweek
       s.event_id, -- gameweek id
       s.player_id, -- player id 
       coalesce(pg.goals_scored, 0) as goals_scored,
       sum(goals_scored) over(partition by s.player_id) as total_goals_scored_in_season,
       (select most_goals_in_season from most_goals_scored_of_any_player) as most_goals_in_season,
       total_goals_scored_in_season = most_goals_in_season as is_player_with_most_goals_in_season
from spine s 
left join player_goals_per_event pg 
on s.player_id = pg.player_id 
and s.event_id = pg.event