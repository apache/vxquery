(: XQuery Join Query :)
(: Count all the stations.                                         :)
count(
    let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($station_collection)/stationCollection/station
    return $s
)