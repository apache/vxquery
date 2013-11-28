(: XQuery Aggregate Query :)
(: Find the highest recorded temperature (TMAX) in Celsius. :)
fn:max(
    let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX"
    return $r/value
) div 10