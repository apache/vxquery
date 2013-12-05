(: XQuery Join Query :)
(: Find all the weather readings for Los Angeles county for a specific day    :)
(: 1976/7/4.                                                                  :)
let $collection1 := "/tmp/1.0_partition_ghcnd_all_xml/stations"
for $s in collection($collection1)/stationCollection/station

let $collection2 := "/tmp/1.0_partition_ghcnd_all_xml/sensors/"
for $r in collection($collection2)/dataCollection/data
    
let $date := xs:date(fn:substring(xs:string(fn:data($r/date)), 0, 11))
where some $x in $s/locationLabels satisfies ($x/type eq "CNTY" and $x/displayName eq "Los Angeles County, CA") 
    and $date eq xs:date("1976-07-04")
return $r