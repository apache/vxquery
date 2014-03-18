(: XQuery Join Query :)
(: Count all the weather stations for Washington state.                       :)
count(
    let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($station_collection)/stationCollection/station
    where (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "WASHINGTON"))
    return $s
)