(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) in the state of Oregon for     :)
(: 2001.                                                                      :)
fn:min(
    let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($station_collection)/stationCollection/station
    
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    
    where $s/id eq $r/station
        and (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "OREGON"))
        and $r/dataType eq "TMIN" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2001
    return $r/value
) div 10