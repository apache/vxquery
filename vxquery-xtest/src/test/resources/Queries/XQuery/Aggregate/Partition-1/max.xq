(: XQuery Aggregate Query :)
(: Find the highest max temperature.                                            :)
fn:max(
    let $collection := "ghcnd"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMAX" 
    return $r/value
)