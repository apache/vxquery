(: XQuery Join Query :)
(: Find the highest recorded temperature (TMAX) for each station on           :)
(: 2000-01-01.                                                                :)
let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($station_collection)/stationCollection/station

let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($sensor_collection)/dataCollection/data

where $s/id eq $r/station
    and $r/dataType eq "TMAX" 
    and xs:dateTime(fn:data($r/date)) eq xs:dateTime("2000-01-01T00:00:00.000")
return ($s/displayName, $r/value)