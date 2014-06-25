(:
XQuery Aggregate Query
----------------------
Find the annual precipitation (PRCP) for a Syracuse, NY using the airport
weather station (USW00014771) report for 1999.                                     
:)
fn:sum(
    let $collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
    for $r in collection($collection)/root/dataCollection/data
    where $r/station eq "GHCND:USW00014771" 
        and $r/dataType eq "PRCP" 
        and fn:year-from-dateTime(xs:dateTime(fn:data($r/date))) eq 1999
    return $r/value
) div 10
