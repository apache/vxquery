(: XQuery Join Aggregate Query :)
(: Self join with all stations.                                               :)
let $sensor_collection1 := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r1 in collection($sensor_collection1)/dataCollection/data

let $sensor_collection2 := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r2 in collection($sensor_collection2)/dataCollection/data

let $date1 := xs:date(fn:substring(xs:string(fn:data($r1/date)), 0, 11))
let $date2 := xs:date(fn:substring(xs:string(fn:data($r2/date)), 0, 11))
where $r1/station eq $r2/station
    and fn:year-from-date($date1) gt 2000
    and fn:year-from-date($date2) gt 2000
return ($r1/value, $r2/value) 