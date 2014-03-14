(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) for each station 2001.         :)
let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($station_collection)/stationCollection/stations

return fn:min(
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where $s/id eq $r/station
        and $r/dataType eq "TMIN" 
        and fn:year-from-date($date) eq 2001
    return $r/value
) div 10