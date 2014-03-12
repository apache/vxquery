(: XQuery Join Query :)
(: Find all the weather readings for King county for a specific day    :)
(: 1976/7/4.                                                                  :)
let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($station_collection)/stationCollection/station

let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($sensor_collection)/dataCollection/data
    
let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
where $s/id eq $r/station 
    and (some $x in $s/locationLabels satisfies ($x/type eq "CNTY" and fn:contains(fn:upper-case(fn:data($x/displayName)), "KING")))
    and $date eq xs:date("1976-07-04")
return $r