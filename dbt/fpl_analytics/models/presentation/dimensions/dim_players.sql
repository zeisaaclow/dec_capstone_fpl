select 

    {{ dbt_utils.generate_surrogate_key(['id']) }} as player_key_hash,
    {{ dbt_utils.generate_surrogate_key(['team']) }} as team_key_hash,
    id,
    code,
    first_name,
    second_name,
    web_name,
    team,
    team_code,
    in_dreamteam
from
{{ref('stg_player')}}