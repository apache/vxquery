(: XQuery Filter Query :)
(: See historical data for Riverside, CA (AS000000003) station by selecting   :)
(: the weather readings for December 25 over the last 10 years.               :)
let $collection := "ghcnd"
for $r in collection($collection)/dataCollection/data
let $datetime := xs:dateTime(fn:data($r/date))
where $r/station eq "GHCND:AS000000003" 
    and fn:year-from-dateTime($datetime) ge 2000
    and fn:month-from-dateTime($datetime) eq 3 
    and fn:day-from-dateTime($datetime) eq 3
return $r
