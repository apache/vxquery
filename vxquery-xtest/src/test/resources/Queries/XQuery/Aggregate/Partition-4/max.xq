(: XQuery Aggregate Query :)
(: Find the highest max temperature.                                            :)
fn:max(
    let $collection := "ghcnd_quarter_1|ghcnd_quarter_2|ghcnd_quarter_3|ghcnd_quarter_4"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX" 
    return $r/value
)
