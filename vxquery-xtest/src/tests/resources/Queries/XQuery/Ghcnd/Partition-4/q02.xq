(: XQuery Aggregate Query :)
(: Find the annual precipitation (PRCP) for a Seattle using the airport       :)
(: station (US000000002) for 2002.                                            :)
fn:sum(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_2/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_3/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_4/"
    for $r in collection($collection)/dataCollection/data
    where $r/station eq "GHCND:US000000002" 
        and $r/dataType eq "PRCP" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2002
    return $r/value
) div 10