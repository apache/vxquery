<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# VXQuery REST Server

VXQuery REST Server allows users to submit queries and receive results either synchronously or
asynchronously through the exposed REST API. Along with the statement to be executed, few other parameters can be given as
well. Complete REST API specification can be found at [REST API Specification](specification.html).

## Installation

No additional steps needed to be taken to get the REST Server up and running. That is, setting up a VXQuery cluster will
automatically start the REST Server on port `8080`. Please see [VXQuery Cluster Setup](../user_cluster_installation.html)
to understand how a VXQuery cluster is setup.

## Getting Started

Suppose we want to execute a very simple XQuery like:

```
for $x in (1, 2.0, 3) return $x
```

### Async (Default Mode) Example

If we want to send this, following will be the plain HTTP request.

```
GET http://127.0.1.1:39003/vxquery/query?statement=for+%24x+in+%281%2C+2.0%2C+3%29+return+%24x HTTP/1.1
```

Note the query parameter `statement=for+%24x+in+%281%2C+2.0%2C+3%29+return+%24x` in which the above mentioned statement
has been encoded. If we send this request using **cURL**, it will look like follows.

#### Accept: application/json

```
curl -i -H "Accept: application/json" -X GET "http://localhost:39003/vxquery/query?statement=for+%24x+in+%281%2C+2.0%2C+3%29+return+%24x"
```

and the response is,

```
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept
content-type: application/json
content-length: 320

{
  "status": "success",
  "requestId": "b0cbe06f-3454-4422-ba23-59150e1c1400",
  "statement": "for $x in (1, 2.0, 3) return $x",
  "abstractSyntaxTree": null,
  "translatedExpressionTree": null,
  "optimizedExpressionTree": null,
  "runtimePlan": null,
  "metrics": {
    "compileTime": 0,
    "elapsedTime": 0
  },
  "resultId": 6,
  "resultUrl": "/vxquery/query/result/6"
}
```

#### Accept: application/xml

```
curl -i -H "Accept: application/xml" -X GET "http://localhost:39003/vxquery/query?statement=for+%24x+in+%281%2C+2.0%2C+3%29+return+%24x"
```

and the response is,

```
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept
content-type: application/xml
content-length: 403

<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<asyncQueryResponse>
    <requestId>d0c2c0ef-2e46-4153-9d4b-1ef4593184e7</requestId>
    <metrics>
        <compileTime>0</compileTime>
        <elapsedTime>0</elapsedTime>
    </metrics>
    <statement>for $x in (1, 2.0, 3) return $x</statement>
    <resultId>8</resultId>
    <resultUrl>/vxquery/query/result/8</resultUrl>
</asyncQueryResponse>
```

#### Result Fetching

Since we have used the default mode (**async**), we only got the **resultId**. Now we have to send another request asking
for the actual results. Send a cURL request to `/vxquery/query/result/8` to fetch results for result ID 8.

##### Accept: application/json

```
curl -i -H "Accept: application/json" -X GET "http://localhost:39003/vxquery/query/result/8"
```

and the response is,

```
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept
content-type: application/json
content-length: 137

{
  "status": "success",
  "requestId": "d0c2c0ef-2e46-4153-9d4b-1ef4593184e7",
  "results": "1\n2\n3\n",
  "metrics": {
    "compileTime": 0,
    "elapsedTime": 0
  }
}
```

Note the *results* in the JSON content in the response.

##### Accept: application/xml

```
curl -i -H "Accept: application/xml" -X GET "http://localhost:39003/vxquery/query/result/8"
```

and the response is,

```
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept
content-type: application/xml
content-length: 298

<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<queryResultResponse>
    <requestId>d0c2c0ef-2e46-4153-9d4b-1ef4593184e7</requestId>
    <metrics>
        <compileTime>0</compileTime>
        <elapsedTime>0</elapsedTime>
    </metrics>
    <results>1
2
3
</results>
</queryResultResponse>
```

Note the *<results></results>* in the XML content in the response.

### Sync (Synchronous Mode) Example

Similarly to what we did under async requests, we can send the query requests here as well, but with the added query parameter
`mode=sync` which is to indicate that the response should be a synchronous one. That is, we wait for the query to be 
executed and the response to arrive.

```
curl -i -H "Accept: application/xml" -X GET \
"http://localhost:39003/vxquery/query?statement=for+%24x+in+%281%2C+2.0%2C+3%29+return+%24x&mode=sync"
```

and the response now contains **results** instead of the **resultId** we received previously.

```
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept
content-type: application/xml
content-length: 353

<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<syncQueryResponse>
    <requestId>93b67f50-4f14-4304-a9b2-f75b4a736df3</requestId>
    <metrics>
        <compileTime>0</compileTime>
        <elapsedTime>0</elapsedTime>
    </metrics>
    <statement>for $x in (1, 2.0, 3) return $x</statement>
    <results>1
2
3
</results>
</syncQueryResponse>
```

Similarly with `accept:application/json`,

```
curl -i -H "Accept: application/json" -X GET \
"http://localhost:39003/vxquery/query?statement=for+%24x+in+%281%2C+2.0%2C+3%29+return+%24x&mode=sync"
```

and the response is,

```
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept
content-type: application/json
content-length: 291

{
  "status": "success",
  "requestId": "8010a699-a6f2-423c-91e1-8ac17cd5c5cd",
  "statement": "for $x in (1, 2.0, 3) return $x",
  "abstractSyntaxTree": null,
  "translatedExpressionTree": null,
  "optimizedExpressionTree": null,
  "runtimePlan": null,
  "metrics": {
    "compileTime": 0,
    "elapsedTime": 0
  },
  "results": "1\n2\n3\n"
}
```
