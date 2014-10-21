(: XQuery Join Aggregate Query :)
(: Self join with all sensor readings after the year 2000.                    :)
fn:avg(
let $sensor_collection_min := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
for $r_min in collection($sensor_collection_min)/root/dataCollection/data

let $sensor_collection_max := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
for $r_max in collection($sensor_collection_max)/root/dataCollection/data

where $r_min/station eq $r_max/station
    and $r_min/date eq $r_max/date
    and $r_min/dataType eq "TMIN"
    and $r_max/dataType eq "TMAX"
return ($r_max/value - $r_min/value)
) div 10