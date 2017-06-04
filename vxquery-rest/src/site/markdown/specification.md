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
# REST API Specification

Swagger configuration of the REST API can be found 
[here](https://cwiki.apache.org/confluence/display/VXQUERY/SwaggerIO+Configuration). 

**NOTE:** This REST API supports both **content types**, `application/json` and `application/xml`. Based on the `accept` header 
of your query request, REST API will return the results wither in *json* form or *XML* form. Returned content type 
defaults to `application/json` if no `accept` header is present.

Base Path **${host}/vxquery**

## Query Request

Request of this type should be submitted for a given *query* to be executed. Depending on the value of the parameter
`mode`, a **synchronous** response or an **asynchronous** response will be returned

`*` required

| Path | Method |Parameters | Type | Description |
| ------ | ------ | ------ | ----- |----- |
| /query | GET  |statement*     | string    | Statement to be executed |
|       |       |mode           | string    | `sync` or `async`. **async** will return an asynchronous response **(default: async)** |
|       |       |compileOnly    | boolean   | If `true`, statement will be compiled, but won't be executed (default: false) |
|       |       |optimization   | integer   | Optimization level (0 - 2,147,483,647). (Default: Full optimization) |
|       |       |frameSize      | integer   | Frame size in bytes (default: 65536) |
|       |       |repeatExecutions|integer   | Number of times to repeat execution (default: 1) |
|       |       |metrics        | boolean   | If `true`, returns metrics (compile and execution time) with the response (default: false) |
|       |       |showAbstractSyntaxTree         | boolean | Shows abstract syntax tree if `true` (default: false) |
|       |       |showTranslatedExpressionTree   | boolean | Shows translated expression tree if `true` (default: false) |
|       |       |showOptimizedExpressionTree    | boolean | Shows optimized expression tree if `true` (default: false) |
|       |       |showRuntimePlan| boolean   | Shows runtime plan if set to `true` (default: false) |

### Synchronous Query Response

Received only when `mode` is set to `sync` in the query request above. 

| Attribute | Type | Description |
| ------ | ------ | ------ |
|statement* |string         | Statement submitted to be executed |
|status*	|string         | `success` to indicate that the query execution was successful | 
|requestId*	|string         | A unique ID assigned to the request sent earlier |
|abstractSyntaxTree	        |string | Abstract Syntax Tree if requested in the query request. Else `null` |
|translatedExpressionTree    |string | Translated Expression Tree if requested in the query request. Else `null` |
|optimizedExpressionTree	    |string | Optimized Expression Tree if requested in the query request. Else `null` |
|runtimePlan    |string     | Runtime plan if requested in the query request. Else `null` |
|metrics	    |metrics    | Metrics (`compileTime` and `elapsedTime`) if requested in the query request |
|results*       |string     | Results of the query/statement submitted for execution |

### Asynchronous Query Response

Received only when `mode` is set to `async` (which is the default value) in the query request above. 

| Attribute | Type | Description |
| ------ | ------ | ------ |
|statement* |string         | Statement submitted to be executed |
|status*	|string         | `success` to indicate that the query execution was successful | 
|requestId*	|string         | A unique ID assigned to the request sent earlier |
|abstractSyntaxTree	        |string | Abstract Syntax Tree if requested in the query request. Else `null` |
|translatedExpressionTree    |string | Translated Expression Tree if requested in the query request. Else `null` |
|optimizedExpressionTree	    |string | Optimized Expression Tree if requested in the query request. Else `null` |
|runtimePlan    |string     | Runtime plan if requested in the query request. Else `null` |
|metrics	    |metrics    | Metrics (`compileTime` and `elapsedTime`) if requested in the query request |
|resultId*  |string     | Result ID of the query submitted for execution. This ID is required later for result fetching |
|resultUrl* |string     | URL from which the results of the submitted query can be retrieved |

### Result fetching (After an Asynchronous Query Response)

The **resultId** received in the asynchronous query response needs to be submitted as a 
**path parameter** (`/query/result/${resultId}`) to the REST API in order to retrieve the corresponding results.

| Path | Method |Parameters | Type | Description |
| ------ | ------ | ------ | ----- |----- |
| /query/result/${resultId}    | GET | metrics | boolean   | If `true`, returns metrics (compile and execution time) with the response (default: false) |

***

## Error Response

In any of the above scenarios, if an error occurred while processing, REST API will return an *Error Response* as 
specified below.

| Attribute | Type | Description |
| ------ | ------ | ------ |
|status*	|string         | `fatal` to indicate that the query execution was failed at some point | 
|requestId*	|string         | A unique ID assigned to the request sent |
|error*     |Error          | An error object which include an error code with an error message. {code: xxx, message: "error message"} |
