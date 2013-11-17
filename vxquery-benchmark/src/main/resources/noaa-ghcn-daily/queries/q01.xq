(: XQuery Filter Query :)
(: Select all the sensor readings for 1951 :)
let $collection := "/tmp/test/1.0_partition_ghcnd_gsn_xml_gz/sensors"
for $r in collection($collection)/dataCollection/data
where fn:starts-with($r/date, "1951")
return $r