(: XQuery Aggregate Query :)
(: Find the number of wind sensor readings.                                            :)
fn:count(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "AWND" 
    return $r/value
)