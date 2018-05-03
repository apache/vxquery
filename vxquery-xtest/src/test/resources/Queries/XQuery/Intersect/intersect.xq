let $a := doc("src/test/resources/TestSources/xml/catalog.xml")/catalog/book[price<50]
let $b := doc("src/test/resources/TestSources/xml/catalog.xml")/catalog/book[price<40]
return $a intersect $b
