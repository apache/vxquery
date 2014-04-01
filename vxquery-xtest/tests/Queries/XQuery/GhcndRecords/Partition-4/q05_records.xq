(: XQuery Join Aggregate Query :)
(: Find the lowest recorded temperature (TMIN) in the United States for     :)
(: 2001.                                                                      :)
    let $station_collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_2/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_3/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_4/"
    for $s in collection($station_collection)/stationCollection/station
    
    let $sensor_collection := "/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_1/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_1/quarter_2/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_3/|/Users/prestoncarman/Documents/smartsvn/vxquery_git_master/vxquery-xtest/tests/TestSources/ghcnd/half_2/quarter_4/"
    for $r in collection($sensor_collection)/dataCollection/data
    
    where $s/id eq $r/station
        and (some $x in $s/locationLabels satisfies ($x/type eq "CNTRY" and $x/id eq "FIPS:US"))
        and $r/dataType eq "TMIN" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2001
    return $r/value
