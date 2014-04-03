(: XQuery Aggregate Query :)
(: Find the average minimum temperature.                                            :)
fn:avg(
    let $collection := "ghcnd"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMIN" 
    return $r/value
)