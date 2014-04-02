(: XQuery Aggregate Query :)
(: Find the highest max temperature.                                            :)
fn:max(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_2/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_3/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_4/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX" 
    return $r/value
)