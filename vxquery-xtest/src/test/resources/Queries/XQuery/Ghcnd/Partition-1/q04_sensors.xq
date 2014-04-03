(: XQuery Join Query :)
(: Find all the weather readings for Washington state for a specific day    :)
(: 2002-2-2.                                                                  :)
let $sensor_collection := "ghcnd"
for $r in collection($sensor_collection)/dataCollection/data
    
where xs:dateTime(fn:data($r/date)) eq xs:dateTime("2002-02-02T00:00:00.000")
return $r
