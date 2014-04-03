(: XQuery Aggregate Query :)
(: Find the total precipitation.                                            :)
fn:sum(
    let $collection := "ghcnd_half_1|ghcnd_half_2"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "PRCP" 
    return $r/value
)
