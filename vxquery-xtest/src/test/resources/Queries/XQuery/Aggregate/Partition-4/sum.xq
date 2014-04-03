(: XQuery Aggregate Query :)
(: Find the total precipitation.                                            :)
fn:sum(
    let $collection := "ghcnd_quarter_1|ghcnd_quarter_2|ghcnd_quarter_3|ghcnd_quarter_4"
    for $r in collection($collection)/dataCollection/data
    where $r/dataType eq "PRCP" 
    return $r/value
)
