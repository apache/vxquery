(: XQuery Join Aggregate Query :)
(: Count all sensor readings for TMIN in 2001.                                :)
count(
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where $r/dataType eq "TMIN" 
        and fn:year-from-date($date) eq 2001
    return $r/value
)