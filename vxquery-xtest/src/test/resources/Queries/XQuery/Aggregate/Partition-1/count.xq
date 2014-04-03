(: XQuery Aggregate Query :)
(: Find the number of wind sensor readings.                                            :)
fn:count(
    let $collection := "ghcnd"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "AWND" 
    return $r/value
)