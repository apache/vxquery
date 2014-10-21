(: XQuery Join Query :)
(: Find the all the records for TMIN.                                         :)
count(
    let $sensor_collection_min := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r_min in collection($sensor_collection_min)/root/dataCollection/data
    
    where $r_min/dataType eq "TMIN"
    return $r_min
)