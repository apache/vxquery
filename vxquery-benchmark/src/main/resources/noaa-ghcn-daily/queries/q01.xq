(: XQuery Filter Query :)
(: Find all reading for hurricane force wind warning or extreme wind warning. The warnings occur when the wind speed (AWND) exceeds 110 mph (49.1744 meters per second). :)
let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($collection)/dataCollection/data
where $r/dataType eq "AWND" and $r/value > 491.744
return $r