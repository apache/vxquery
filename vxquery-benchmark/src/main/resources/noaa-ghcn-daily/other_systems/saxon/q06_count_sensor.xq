count(
    let $sensor_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($sensor_collection)/root/dataCollection/data
    
    where $r/dataType eq "TMAX" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2000
    return $r
)