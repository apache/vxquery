(: XQuery Aggregate Query :)
(: Find the highest recorded temperature (TMAX) in Celsius.                   :)
    let $collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX"
    return $r/value
