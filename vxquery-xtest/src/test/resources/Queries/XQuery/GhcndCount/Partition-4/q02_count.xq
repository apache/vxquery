(: XQuery Aggregate Query :)
(: Find the annual precipitation (PRCP) for a Seattle using the airport       :)
(: station (US000000002) for 2002.                                            :)
fn:count(
    let $collection := "ghcnd_quarter_1|ghcnd_quarter_2|ghcnd_quarter_3|ghcnd_quarter_4"
    for $r in collection($collection)/dataCollection/data
    where $r/station eq "GHCND:US000000002" 
        and $r/dataType eq "PRCP" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 2002
    return $r/value
)
