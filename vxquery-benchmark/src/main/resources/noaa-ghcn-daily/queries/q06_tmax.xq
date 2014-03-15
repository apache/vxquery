(: XQuery Join Query :)
(: Find the all the records for TMAX.                                         :)
count(
    let $sensor_collection_max := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r_max in collection($sensor_collection_max)/dataCollection/data
    
    where $r_max/dataType eq "TMAX"
    return $r_max
)