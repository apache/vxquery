(: XQuery Aggregate Query :)
(: Find the annual precipitation for a Seattle using the airport station (USW00024233) for 1999. :)
fn:sum(
    let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    for $r in collection($collection)/dataCollection/data
    let $date := xs:date(fn:substring($r/date, 0, 11))
    where $r/station eq "GHCND:USW00024233" and $r/dataType eq "PRCP" and fn:year-from-date($date) eq 1999
    return $r/value
) div 10