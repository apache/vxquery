(: XQuery Join Query :)
(: Get each stations highest temperature :)
let $collection1 := "/tmp/test/1.0_partition_ghcnd_gsn_xml_gz/sensors"
for $r in collection($collection1)/dataCollection/data
where $r/dataType eq "TMAX"

let $collection2 := "/tmp/test/1.0_partition_ghcnd_gsn_xml_gz/stations"
for $s in collection($collection2)/dataCollection/data

where data($r/station_id) eq data($s/station_id)
return ($s/name, fn:max($r/value))