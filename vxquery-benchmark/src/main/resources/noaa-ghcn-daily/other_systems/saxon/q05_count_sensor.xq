(: XQuery Join Aggregate Query :)
(: Count all sensor readings for TMIN in 2001.                                :)
count(
    let $sensor_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($sensor_collection)/root/dataCollection/data
    
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where $r/dataType eq "TMIN" 
        and fn:year-from-date($date) eq 2001
    return $r/value
)