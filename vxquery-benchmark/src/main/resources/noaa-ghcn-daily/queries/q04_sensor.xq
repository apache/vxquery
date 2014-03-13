(: XQuery Join Query :)
(: Count all the weather sensor readings on 1976-07-04.                       :)
count(
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
        
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where $date eq xs:date("1976-07-04")
    return $r
)