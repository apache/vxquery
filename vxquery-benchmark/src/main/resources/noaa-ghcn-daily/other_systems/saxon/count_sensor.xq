(: XQuery Join Query :)
(: Count all the weather sensor readings available.                           :)
count(
    let $sensor_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($sensor_collection)/root/dataCollection/data
    return $r
)