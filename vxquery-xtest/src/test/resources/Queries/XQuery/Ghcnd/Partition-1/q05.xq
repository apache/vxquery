(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) in the United States for     :)
(: 2001.                                                                      :)
fn:min(
    let $station_collection := "ghcnd"
    for $s in collection($station_collection)/stationCollection/station
    
    let $sensor_collection := "ghcnd"
    for $r in collection($sensor_collection)/dataCollection/data
    
    where $s/id eq $r/station
        and (some $x in $s/locationLabels satisfies ($x/type eq "CNTRY" and $x/id eq "FIPS:US"))
        and $r/dataType eq "TMIN" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2001
    return $r/value
) div 10
