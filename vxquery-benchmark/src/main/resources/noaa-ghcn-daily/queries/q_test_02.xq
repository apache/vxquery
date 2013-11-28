(: XQuery Test Query :)
(: Count all the XML documents. :)
count( 
    let $collection := "/tmp/1.0_partition_ghcnd_all_xml/sensors"
    return collection($collection) 
)
