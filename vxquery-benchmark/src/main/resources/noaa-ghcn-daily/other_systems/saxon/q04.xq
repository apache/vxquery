(: XQuery Join Query :)
(: Find all the weather readings for King county for a specific day    :)
(: 1976/7/4.                                                                  :)
let $sensor_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
for $r in collection($sensor_collection)/root/dataCollection/data

let $station_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/stations/?select=*.xml;recurse=yes"
for $s in collection($station_collection)/root/stationCollection/station

where $s/id eq $r/station 
    and (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "WASHINGTON"))
    and xs:dateTime(fn:data($r/date)) eq xs:dateTime("1976-07-04T00:00:00.000")
return $r
