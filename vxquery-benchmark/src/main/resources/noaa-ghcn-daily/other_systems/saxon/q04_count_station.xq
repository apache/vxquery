(: XQuery Join Query :)
(: Count all the weather stations for King county.                            :)
count(
    let $station_collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/stations/?select=*.xml;recurse=yes"
    for $s in collection($station_collection)/root/stationCollection/station
    where (some $x in $s/locationLabels satisfies ($x/type eq "CNTY" and fn:contains(fn:upper-case(fn:data($x/displayName)), "KING")))
    return $s
)