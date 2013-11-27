(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) in the state of Oregon for 2001. :)
fn:min(
    let $collection1 := "/tmp/test/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($collection1)/stationCollection/station
    
    let $collection2 := "/tmp/test/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($collection2)/dataCollection/data
    
    let $date := xs:date($r/date)
    where some $x in $s/locationLabels satisfies ($x/type eq "state" and $x/value eq "Oregon") 
        and $r/dataType eq "TMIN" 
        and fn:year-from-date($date) eq 2001
    return $r
)