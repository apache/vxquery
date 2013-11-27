(: XQuery Aggregate Query :)
(: Find the highest recorded temperature (TMAX). :)
fn:max(
    let $collection := "/tmp/test/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX"
    return $r/value
)