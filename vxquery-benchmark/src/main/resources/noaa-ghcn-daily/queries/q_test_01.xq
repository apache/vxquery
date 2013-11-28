(: XQuery Test Query :)
(: Find a specific record GHCND:BB000078954 on 1944-02-29 for TMIN. :)
let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
for $r in collection($collection)/dataCollection/data
where data($r/station) eq "GHCND:ASN00008113" and data($r/date) eq "1944-02-29T00:00:00.000" and data($r/dataType) eq "PRCP"
return $r