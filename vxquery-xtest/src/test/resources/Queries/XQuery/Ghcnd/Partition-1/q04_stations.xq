(: XQuery Join Query :)
(: Find all the weather readings for Washington state for a specific day    :)
(: 2002-2-2.                                                                  :)
let $station_collection := "ghcnd"
for $s in collection($station_collection)/stationCollection/station

where (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "STATE 1"))
return $s
