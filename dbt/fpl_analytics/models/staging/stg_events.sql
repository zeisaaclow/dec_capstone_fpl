-- grain: only gameweek per chip_play (3 at most) so upper bound for rows is 38 * 3
SELECT 
    id as id,
    name as name,
    average_entry_score as average_entry_score,
    finished as finished,
    data_checked as data_checked,
    highest_scoring_entry as highest_scoring_entry,
    deadline_time as deadline_time,
    deadline_time_epoch as deadline_time_epoch,
    deadline_time_game_offset as deadline_time_game_offset,
    highest_score as highest_score,
    is_previous as is_previous,
    is_current as is_current,
    is_next as is_next,
    cup_leagues_created as cup_leagues_created,
    h2h_ko_matches_created as h2h_ko_matches_created,
    most_selected as most_selected,
    most_transferred_in as most_transferred_in,
    top_element as top_element,
    transfers_made as transfers_made,
    most_captained as most_captained,
    most_vice_captained as most_vice_captained
FROM {{source('raw', 'events')}} 

-- grain: only gameweek per chip_play (3 at most) so upper bound for rows is 38 * 3
/* SELECT 
    "id" as id,
    "name" as name,
    "average_entry_score" as average_entry_score,
    "finished" as finished,
    "data_checked" as data_checked,
    "highest_scoring_entry" as highest_scoring_entry,
    "deadline_time" as deadline_time,
    "deadline_time_epoch" as deadline_time_epoch,
    "deadline_time_game_offset" as deadline_time_game_offset,
    "highest_score" as highest_score,
    "is_previous" as is_previous,
    "is_current" as is_current,
    "is_next" as is_next,
    "cup_leagues_created" as cup_leagues_created,
    "h2h_ko_matches_created" as h2h_ko_matches_created,
    "most_selected" as most_selected,
    "most_transferred_in" as most_transferred_in,
    "top_element" as top_element,
    "transfers_made" as transfers_made,
    "most_captained" as most_captained,
    "most_vice_captained" as most_vice_captained
FROM {{source('raw', 'events')}} */