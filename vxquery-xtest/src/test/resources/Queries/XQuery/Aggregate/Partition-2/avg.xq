(: XQuery Aggregate Query :)
(: Find the average minimum temperature.                                            :)
fn:avg(
    let $collection := "ghcnd_half_1|ghcnd_half_2"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "TMIN" 
    return $r/value
)
