(: XQuery Join Query :)
(: Find all the weather readings for Los Angeles county for a specific day 1976/7/4. :)
let $collection1 := "/tmp/test/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($collection1)/stationCollection/station

let $collection2 := "/tmp/test/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($collection2)/dataCollection/data

where some $x in $s/locationLabels satisfies ($x/type eq "county" and $x/value eq "Los Angeles") 
    and xs:date($r/date) eq xs:date("1976/7/4")
return $r