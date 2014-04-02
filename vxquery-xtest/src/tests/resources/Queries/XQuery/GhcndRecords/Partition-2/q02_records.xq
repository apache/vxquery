(: XQuery Aggregate Query :)
(: Find the annual precipitation (PRCP) for a Seattle using the airport       :)
(: station (US000000002) for 2002.                                            :)
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/"
    for $r in collection($collection)/dataCollection/data
    where $r/station eq "GHCND:US000000002" 
        and $r/dataType eq "PRCP" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2002
    return $r/value
