(: XQuery Aggregate Query :)
(: Find the total precipitation.                                            :)
fn:sum(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_2/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_3/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_4/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "PRCP" 
    return $r/value
)