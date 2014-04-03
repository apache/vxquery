(: XQuery Aggregate Query :)
(: Find the highest max temperature.                                            :)
fn:max(
    let $collection := "ghcnd_half_1|ghcnd_half_2"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX" 
    return $r/value
)
