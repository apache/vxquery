(: XQuery Join Query :)
(: Find all the weather readings for Washington state for a specific day    :)
(: 2002-2-2.                                                                  :)
let $station_collection := "ghcnd_half_1|ghcnd_half_2"
for $s in collection($station_collection)/stationCollection/station

let $sensor_collection := "ghcnd_half_1|ghcnd_half_2"
for $r in collection($sensor_collection)/dataCollection/data
    
where $s/id eq $r/station 
    and (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "STATE 1"))
    and xs:dateTime(fn:data($r/date)) eq xs:dateTime("2002-02-02T00:00:00.000")
return $r
