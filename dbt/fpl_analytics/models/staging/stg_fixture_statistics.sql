WITH spine as (
    SELECT 
        code as code,
        event as event,
        finished as is_finished,
        finished_provisional as is_finished_provisional,
        id as id,
        kickoff_time as kickoff_time,
        minutes as minutes,
        provisional_start_time as provisional_start_time,
        started::boolean as is_started,
        team_a::varchar as away_team_id,
        team_a_score::float as away_team_score,
        team_h::varchar as home_team_id,
        team_h_score::float as home_team_score,
        stats as stats ,
    
        stats.value:identifier::varchar as identifier,
        stats.value:a as away_array,
        stats.value:h as home_array
    FROM fpl.raw.fixtures,
        LATERAL FLATTEN(INPUT => stats) stats
) 

, away_team_statistics AS (
    select 
        code,
        provisional_start_time,
        minutes,
        kickoff_time,
        is_started,
        is_finished,
        is_finished_provisional,
        id,
        away_team_id,
        away_team_score,
        home_team_id,
        home_team_score,
        identifier as statistic,
        --
        event,
        away_team_id as team_id,
        away.value:element::varchar as player_id,
        away.value:value::float as player_value,
        'Away' as home_or_away_team
    from spine,
    LATERAL FLATTEN(INPUT => away_array) away
)

, home_team_statistics AS (
    select 
        code,
        provisional_start_time,
        minutes,
        kickoff_time,
        is_started,
        is_finished,
        is_finished_provisional,
        id,
        away_team_id,
        away_team_score,
        home_team_id,
        home_team_score,
        identifier as statistic,
        --
        event,
        home_team_id as team_id,
        home.value:element::varchar as player_id,
        home.value:value::float as player_value,
        'Home' as home_or_away_team
    from spine,
    LATERAL FLATTEN(INPUT => home_array) home
)
, all_statistics as (
    select * from 
        home_team_statistics 
    union 
    select * from
        away_team_statistics
)

select * from all_statistics

/* WITH spine as (
    SELECT 
        "code" as code,
        "event" as event,
        "finished" as is_finished,
        "finished_provisional" as is_finished_provisional,
        "id" as id,
        "kickoff_time" as kickoff_time,
        "minutes" as minutes,
        "provisional_start_time" as provisional_start_time,
        "started"::boolean as is_started,
        "team_a"::varchar as away_team_id,
        "team_a_score"::float as away_team_score,
        "team_h"::varchar as home_team_id,
        "team_h_score"::float as home_team_score,
        "stats" as stats ,
    
        stats.value:"identifier"::varchar as identifier,
        stats.value:"a" as away_array,
        stats.value:"h" as home_array
    FROM fpl.raw.fixtures,
        LATERAL FLATTEN(INPUT => "stats") stats
) 

, away_team_statistics AS (
    select 
        code,
        provisional_start_time,
        minutes,
        kickoff_time,
        is_started,
        is_finished,
        is_finished_provisional,
        id,
        away_team_id,
        away_team_score,
        home_team_id,
        home_team_score,
        identifier as statistic,
        --
        event,
        away_team_id as team_id,
        away.value:element::varchar as player_id,
        away.value:value::float as player_value,
        'Away' as home_or_away_team
    from spine,
    LATERAL FLATTEN(INPUT => away_array) away
)

, home_team_statistics AS (
    select 
        code,
        provisional_start_time,
        minutes,
        kickoff_time,
        is_started,
        is_finished,
        is_finished_provisional,
        id,
        away_team_id,
        away_team_score,
        home_team_id,
        home_team_score,
        identifier as statistic,
        --
        event,
        home_team_id as team_id,
        home.value:element::varchar as player_id,
        home.value:value::float as player_value,
        'Home' as home_or_away_team
    from spine,
    LATERAL FLATTEN(INPUT => home_array) home
)
, all_statistics as (
    select * from 
        home_team_statistics 
    union 
    select * from
        away_team_statistics
)

select * from all_statistics */
