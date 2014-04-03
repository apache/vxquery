(: XQuery Filter Query :)
(: Find all reading for hurricane force wind warning or extreme wind warning. :)
(: The warnings occur when the wind speed (AWND) exceeds 110 mph (49.1744     :)
(: meters per second). (Wind value is in tenth of a meter per second)         :)
let $collection := "ghcnd_quarter_1|ghcnd_quarter_2|ghcnd_quarter_3|ghcnd_quarter_4"
for $r in collection($collection)/dataCollection/data
where $r/dataType eq "AWND" and xs:decimal(fn:data($r/value)) gt 491.744
return $r
