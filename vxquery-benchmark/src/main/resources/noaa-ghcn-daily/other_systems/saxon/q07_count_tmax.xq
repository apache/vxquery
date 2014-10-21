(: XQuery Join Query :)
(: Find the all the records for TMAX.                                         :)
count(
    let $sensor_collection_max := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r_max in collection($sensor_collection_max)/root/dataCollection/data
    
    where $r_max/dataType eq "TMAX"
    return $r_max
)