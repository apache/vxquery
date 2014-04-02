(: XQuery Aggregate Query :)
(: Find the total precipitation.                                            :)
fn:sum(
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "PRCP" 
    return $r/value
)