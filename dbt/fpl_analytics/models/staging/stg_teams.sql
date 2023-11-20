select 
	id as id,
	code as code,
	name as name,
	short_name as short_name,
	strength as strength,
	strength_overall_home as strength_overall_home,
	strength_overall_away as strength_overall_away,
	strength_defence_home as strength_defence_home,
	strength_defence_away as strength_defence_away,
	strength_attack_home as strength_attack_home,
	strength_attack_away as strength_attack_away,
	unavailable as unavailable,
	draw as draw,
	played as played,
	pulse_id as pulse_id,
	win as win,
	loss as loss,
	points as points,
	position as position
from 
{{source('raw', 'teams')}}

/* select 
	"id" as id,
	"code" as code,
	"name" as name,
	"short_name" as short_name,
	"strength" as strength,
	"strength_overall_home" as strength_overall_home,
	"strength_overall_away" as strength_overall_away,
	"strength_defence_home" as strength_defence_home,
	"strength_defence_away" as strength_defence_away,
	"strength_attack_home" as strength_attack_home,
	"strength_attack_away" as strength_attack_away,
	"unavailable" as unavailable,
	"draw" as draw,
	"played" as played,
	"pulse_id" as pulse_id,
	"win" as win,
	"loss" as loss,
	"points" as points,
	"position" as position
from 
{{source('raw', 'teams')}} */