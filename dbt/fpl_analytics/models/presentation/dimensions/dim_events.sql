select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as event_key_hash,
    id, 
    name,
    finished,
    data_checked,
    most_selected,
    most_transferred_in,
    top_element as top_player,
    most_captained as most_captained_player,
    most_vice_captained as most_vice_captained_player
from 
{{ref('stg_events')}}