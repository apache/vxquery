let $collection := "/path/to/data"
return
	for $r in collection($collection)/ghcnd_observation
	where $r/station_id = "AG000060390"
	return $r/name