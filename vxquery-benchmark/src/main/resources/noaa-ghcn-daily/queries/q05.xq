(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) in the state of Oregon for     :)
(: 2001.                                                                      :)
fn:min(
    let $collection1 := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($collection1)/stationCollection/station
    
    let $collection2 := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($collection2)/dataCollection/data
    
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where $s/id eq $r/station
        and (some $x in $s/locationLabels satisfies ($x/type eq "ST" and $x/displayName eq "Oregon"))
        and $r/dataType eq "TMIN" 
        and fn:year-from-date($date) eq 2001
    return $r/value
) div 10