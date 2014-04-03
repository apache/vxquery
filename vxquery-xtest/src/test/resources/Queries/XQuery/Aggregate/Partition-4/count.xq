(: XQuery Aggregate Query :)
(: Find the number of wind sensor readings.                                            :)
fn:count(
    let $collection := "ghcnd_quarter_1|ghcnd_quarter_2|ghcnd_quarter_3|ghcnd_quarter_4"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "AWND" 
    return $r/value
)
