(: XQuery Join Aggregate Query :)
(: Count all stations in the state of Oregon.                                 :)
count(
    let $station_collection := "/tmp/1.0_partition_ghcnd_all_xml/stations"
    for $s in collection($station_collection)/stationCollection/station
    where (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "OREGON"))
    return $s
)