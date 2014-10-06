(: XQuery Join Query :)
(: Count all the weather sensor readings on 1976-07-04.                       :)
count(
    let $sensor_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($sensor_collection)/root/dataCollection/data
        
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where $date eq xs:date("1976-07-04")
    return $r
)