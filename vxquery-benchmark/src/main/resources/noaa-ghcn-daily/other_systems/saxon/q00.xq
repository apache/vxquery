(: 
XQuery Filter Query
-------------------
See historical data for Key West International Airport, FL (USW00012836)
station by selecting  the weather readings for December 25 over the last 
10 years. 
:)
let $collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
for $r in collection($collection)/root/dataCollection/data
let $datetime := xs:dateTime(fn:data($r/date))
where $r/station eq "GHCND:USW00012836" 
    and fn:year-from-dateTime($datetime) ge 2003
    and fn:month-from-dateTime($datetime) eq 12 
    and fn:day-from-dateTime($datetime) eq 25
return $r