(: XQuery Aggregate Query :)
(: Find the total precipitation recorded :)
let $collection := "/tmp/test/1.0_partition_ghcnd_gsn_xml_gz/sensors"
return fn:sum(
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "PRCP"
    return $r/value
)