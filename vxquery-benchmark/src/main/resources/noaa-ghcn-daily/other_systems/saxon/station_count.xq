(: XQuery Join Query :)
(: Count all the weather stations available.                                  :)
count(
    let $station_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/stations/?select=*.xml;recurse=yes"
    for $s in collection($station_collection)/root/stationCollection/station
    return $s
)