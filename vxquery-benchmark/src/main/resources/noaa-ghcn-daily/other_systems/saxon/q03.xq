(: XQuery Aggregate Query :)
(: Find the highest recorded temperature (TMAX) in Celsius.                   :)
fn:max(
    let $collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($collection)/root/dataCollection/data
    where $r/dataType eq "TMAX"
    return $r/value
) div 10
