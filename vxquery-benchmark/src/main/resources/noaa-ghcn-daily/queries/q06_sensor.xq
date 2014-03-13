(: XQuery Join Aggregate Query :)
(: Count all sensor readings after 2000.                                      :)
count(
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
    where fn:year-from-date($date) gt 2000
    return $r
)