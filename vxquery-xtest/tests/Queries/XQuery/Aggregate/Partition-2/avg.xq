(: XQuery Aggregate Query :)
(: Find the average minimum temperature.                                            :)
fn:avg(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMIN" 
    return $r/value
)