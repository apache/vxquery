(: XQuery Join Query :)
(: Find all the weather readings for King county for a specific day    :)
(: 1976/7/4.                                                                  :)
let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($station_collection)/stationCollection/station

let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($sensor_collection)/dataCollection/data
    
where $s/id eq $r/station 
    and (some $x in $s/locationLabels satisfies ($x/type eq "CNTY" and fn:contains(fn:upper-case(fn:data($x/displayName)), "KING")))
    and xs:dateTime(fn:data($r/date)) eq xs:dateTime("1976-07-04T00:00:00.000")
return $r