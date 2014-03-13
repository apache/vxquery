(: XQuery Join Query :)
(: Count all the weather stations for King county.                            :)
count(
    let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($station_collection)/stationCollection/station
    where (some $x in $s/locationLabels satisfies ($x/type eq "CNTY" and fn:contains(fn:upper-case(fn:data($x/displayName)), "KING")))
    return $s
)