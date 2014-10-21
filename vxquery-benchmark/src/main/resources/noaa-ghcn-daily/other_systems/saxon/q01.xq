(: XQuery Filter Query :)
(: Find all reading for hurricane force wind warning or extreme wind warning. :)
(: The warnings occur when the wind speed (AWND) exceeds 110 mph (49.1744     :)
(: meters per second). (Wind value is in tenth of a meter per second)         :)
let $collection := "../../../../../../../weather_data/dataset-tiny-local/data_links/local_speed_up/d0_p1_i0/sensors/?select=*.xml;recurse=yes"
for $r in fn:collection($collection)/root/dataCollection/data
where $r/dataType eq "AWND" and xs:decimal($r/value) gt 491.744
return $r
