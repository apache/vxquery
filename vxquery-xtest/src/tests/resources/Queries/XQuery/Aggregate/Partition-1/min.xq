(: XQuery Aggregate Query :)
(: Find the lowest min temperature.                                            :)
fn:min(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMIN" 
    return $r/value
)