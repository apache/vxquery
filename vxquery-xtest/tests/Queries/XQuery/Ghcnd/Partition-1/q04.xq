(: XQuery Join Query :)
(: Find all the weather readings for Washington state for a specific day    :)
(: 2002-2-2.                                                                  :)
let $station_collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/"
for $s in collection($station_collection)/stationCollection/station

let $sensor_collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/"
for $r in collection($sensor_collection)/dataCollection/data
    
where $s/id eq $r/station 
    and (some $x in $s/locationLabels satisfies ($x/type eq "ST" and fn:upper-case(fn:data($x/displayName)) eq "STATE 1"))
    and xs:dateTime(fn:data($r/date)) eq xs:dateTime("2002-02-02T00:00:00.000")
return $r