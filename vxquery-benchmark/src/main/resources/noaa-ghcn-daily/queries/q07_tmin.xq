(: XQuery Join Query :)
(: Find the all the records for TMIN.                                         :)
count(
    let $sensor_collection_min := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r_min in collection($sensor_collection_min)/dataCollection/data
    
    where $r_min/dataType eq "TMIN"
    return $r_min
)