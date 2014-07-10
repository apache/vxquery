(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) in the state of Oregon for     :)
(: 2001.                                                                      :)
fn:min(
    let $sensor_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($sensor_collection)/root/dataCollection/data
        
    let $station_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/stations/?select=*.xml;recurse=yes"
    for $s in collection($station_collection)/root/stationCollection/station
    
    where $s/id eq $r/station
        and (some $x in $s/locationLabels satisfies ($x/type eq "CNTRY" and $x/id eq "FIPS:US"))
        and $r/dataType eq "TMIN" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2001
    return $r/value
) div 10
