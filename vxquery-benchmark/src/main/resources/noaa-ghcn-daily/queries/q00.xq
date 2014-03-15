(: XQuery Filter Query :)
(: See historical data for Riverside, CA (ASN00008113) station by selecting   :)
(: the weather readings for December 25 over the last 10 years.               :)
let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($collection)/dataCollection/data
let $datetime := xs:dateTime(fn:data($r/date))
where $r/station eq "GHCND:ASN00008113" 
    and fn:year-from-dateTime($datetime) ge 2003
    and fn:month-from-dateTime($datetime) eq 12 
    and fn:day-from-dateTime($datetime) eq 25
return $r