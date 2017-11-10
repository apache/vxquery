/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.vxquery.rest;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;

import java.net.URI;

import javax.ws.rs.HttpMethod;

import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.AsyncQueryResponse;
import org.apache.vxquery.rest.response.ErrorResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.rest.service.Status;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests the success responses received for XQueries submitted. i.e
 * we are submitting correct queries which are expected to return a predictable
 * result. All the parameters that are expected to be sent with query requests
 * are subjected to test in this test class
 *
 * @author Erandi Ganepola
 */
public class SuccessAsyncResponseTest extends AbstractRestServerTest {

    @Test
    public void testSimpleQuery001() throws Exception {
        QueryRequest request = new QueryRequest("1+1");
        request.setShowAbstractSyntaxTree(true);
        request.setShowOptimizedExpressionTree(true);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(true);
        request.setShowMetrics(false);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSimpleQuery002() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowAbstractSyntaxTree(true);
        request.setShowOptimizedExpressionTree(true);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(true);
        request.setShowMetrics(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSimpleQuery003() throws Exception {
        QueryRequest request = new QueryRequest("1+2+3");
        request.setShowAbstractSyntaxTree(false);
        request.setShowOptimizedExpressionTree(false);
        request.setShowRuntimePlan(false);
        request.setShowTranslatedExpressionTree(false);
        request.setShowMetrics(false);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSimpleQuery004() throws Exception {
        QueryRequest request = new QueryRequest("fn:true()");
        request.setShowAbstractSyntaxTree(false);
        request.setShowOptimizedExpressionTree(false);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(false);
        request.setShowMetrics(false);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterNone() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterMetrics() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowMetrics(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterAST() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowAbstractSyntaxTree(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterOptimization() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setOptimization(10000);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterFrameSize() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setFrameSize((int) Math.pow(2, 12));

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterCompileOnly() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setCompileOnly(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterOET() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowOptimizedExpressionTree(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterTET() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowTranslatedExpressionTree(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSingleParameterRP() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowRuntimePlan(true);

        runTest(null, request);
        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    private void runTest(String contentType, QueryRequest request) throws Exception {
        runTest(contentType, request, HttpMethod.GET);
        runTest(contentType, request, HttpMethod.POST);
    }

    private void runTest(String contentType, QueryRequest request, String httpMethod) throws Exception {
        URI queryEndpointUri = RestUtils.buildQueryURI(request, restIpAddress, restPort);
        System.out.println("<<<<<"+restIpAddress+":"+String.valueOf(restPort));
        //Thread.sleep(1000);
        /*
         * ========== Query Response Testing ==========
         */
        // Testing the accuracy of VXQueryService class
        AsyncQueryResponse expectedAsyncQueryResponse = (AsyncQueryResponse) vxQueryService.execute(request);

        Assert.assertEquals(Status.SUCCESS.toString(), expectedAsyncQueryResponse.getStatus());
        Assert.assertEquals(request.getStatement(), expectedAsyncQueryResponse.getStatement());
        checkResults(expectedAsyncQueryResponse, request.isCompileOnly());
        checkMetrics(expectedAsyncQueryResponse, request.isShowMetrics());
        if (request.isShowMetrics()) {
            Assert.assertTrue(expectedAsyncQueryResponse.getMetrics().getCompileTime() > 0);
        } else {
            Assert.assertTrue(expectedAsyncQueryResponse.getMetrics().getCompileTime() == 0);
        }
        if (request.isShowAbstractSyntaxTree()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getAbstractSyntaxTree());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getAbstractSyntaxTree());
        }
        if (request.isShowTranslatedExpressionTree()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getTranslatedExpressionTree());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getTranslatedExpressionTree());
        }
        if (request.isShowOptimizedExpressionTree()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getOptimizedExpressionTree());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getOptimizedExpressionTree());
        }
        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getRuntimePlan());
        }

        // Testing the accuracy of REST server and servlets
        AsyncQueryResponse actualAsyncQueryResponse =
                getQuerySuccessResponse(queryEndpointUri, contentType, AsyncQueryResponse.class, httpMethod);

        Assert.assertNotNull(actualAsyncQueryResponse.getRequestId());
        Assert.assertEquals(request.getStatement(), actualAsyncQueryResponse.getStatement());
        Assert.assertEquals(Status.SUCCESS.toString(), actualAsyncQueryResponse.getStatus());
        checkResults(actualAsyncQueryResponse, request.isCompileOnly());
        checkMetrics(actualAsyncQueryResponse, request.isShowMetrics());
        // Cannot check this because Runtime plan include some object IDs which differ
        // Assert.assertEquals(expectedAsyncQueryResponse.getRuntimePlan(),
        // actualAsyncQueryResponse.getRuntimePlan());
        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(actualAsyncQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(actualAsyncQueryResponse.getRuntimePlan());
        }
        Assert.assertEquals(normalize(expectedAsyncQueryResponse.getOptimizedExpressionTree()),
                normalize(actualAsyncQueryResponse.getOptimizedExpressionTree()));
        Assert.assertEquals(normalize(expectedAsyncQueryResponse.getTranslatedExpressionTree()),
                normalize(actualAsyncQueryResponse.getTranslatedExpressionTree()));
        Assert.assertEquals(normalize(expectedAsyncQueryResponse.getAbstractSyntaxTree()),
                normalize(actualAsyncQueryResponse.getAbstractSyntaxTree()));

        /*
         * ========== Query Result Response Testing ========
         */
        QueryResultRequest resultRequest = new QueryResultRequest(actualAsyncQueryResponse.getResultId());
        resultRequest.setShowMetrics(true);

        if (request.isCompileOnly()) {
            APIResponse resultResponse = (APIResponse) vxQueryService.getResult(resultRequest);
            Assert.assertTrue(resultResponse instanceof ErrorResponse);
        } else {
            APIResponse resultResponse =vxQueryService.getResult(resultRequest);
            
            APIResponse resultResponse1= (APIResponse) resultResponse;
//            QueryResultResponse expectedResultResponse = (QueryResultResponse) vxQueryService.getResult(resultRequest);
            QueryResultResponse expectedResultResponse = (QueryResultResponse) resultResponse1;
            Assert.assertEquals(expectedResultResponse.getStatus(), Status.SUCCESS.toString());
            Assert.assertNotNull(expectedResultResponse.getResults());

            QueryResultResponse actualResultResponse = getQueryResultResponse(resultRequest, contentType, httpMethod);
            Assert.assertEquals(actualResultResponse.getStatus(), Status.SUCCESS.toString());
            Assert.assertNotNull(actualResultResponse.getResults());
            Assert.assertNotNull(actualResultResponse.getRequestId());
            Assert.assertEquals(normalize(expectedResultResponse.getResults()),
                    normalize(actualResultResponse.getResults()));
            if (resultRequest.isShowMetrics()) {
                Assert.assertTrue(actualResultResponse.getMetrics().getElapsedTime() > 0);
            } else {
                Assert.assertTrue(actualResultResponse.getMetrics().getElapsedTime() == 0);
            }

        }
    }
}
