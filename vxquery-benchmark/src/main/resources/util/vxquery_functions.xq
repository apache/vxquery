(: XQuery Function List :)
(: VXQuery function list in csv with arguments and return types :)
let $list := "../../../../../vxquery-core/src/main/java/org/apache/vxquery/functions/builtin-functions.xml"
let $r :=
    for $f in fn:doc($list)/functions/function
        let $pl := 
            for $p in $f/param
            return $p/@type
        return fn:string-join(($f/@name, fn:string-join($pl, ' '), $f/return/@type), ',')
return fn:string-join($r , '|')