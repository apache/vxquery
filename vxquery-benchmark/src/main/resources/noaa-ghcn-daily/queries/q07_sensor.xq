(: XQuery Join Query :)
(: Count max temperature (TMAX) readings for 2000-01-01.                          :)
count(
    let $sensor_collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($sensor_collection)/dataCollection/data
    
    where $r/dataType eq "TMAX" 
    	and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2000
    return $r
)