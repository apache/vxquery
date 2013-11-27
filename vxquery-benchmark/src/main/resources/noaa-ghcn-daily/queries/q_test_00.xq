(: XQuery Test Query :)
for $r in (1 to 5)
for $s in (3 to 7)
where $r eq $s
return $r