(: XQuery Join Query :)
(: Count all the weather sensor readings available.                           :)
count(
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    return $r
)