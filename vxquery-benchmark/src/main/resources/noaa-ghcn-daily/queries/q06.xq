(: XQuery Join Query :)
(: Find the highest recorded temperature (TMAX) for each station for each     :)
(: day over the year 2000.                                                    :)
let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($station_collection)/stationCollection/station

let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($sensor_collection)/dataCollection/data

where $s/id eq $r/station
    and $r/dataType eq "TMAX" 
    and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2000
return ($s/displayName, $r/date, $r/value)