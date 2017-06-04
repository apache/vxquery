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

import static org.apache.vxquery.app.util.RestUtils.buildQueryResultURI;
import static org.apache.vxquery.app.util.RestUtils.buildQueryURI;
import static org.apache.vxquery.rest.Constants.ErrorCodes.INVALID_INPUT;
import static org.apache.vxquery.rest.Constants.ErrorCodes.NOT_FOUND;
import static org.apache.vxquery.rest.Constants.ErrorCodes.PROBLEM_WITH_QUERY;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.HttpMethod;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.ErrorResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests error codes of the possible error responses that can be received for
 * erroneous queries.
 *
 * @author Erandi Ganepola
 */
public class ErrorResponseTest extends AbstractRestServerTest {

    @Test
    public void testInvalidInput01() throws Exception {
        QueryRequest request = new QueryRequest("   ");
        runTest(buildQueryURI(request, restIpAddress, restPort), null, INVALID_INPUT);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, INVALID_INPUT);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, INVALID_INPUT);
    }

    @Test
    public void testInvalidInput02() throws Exception {
        QueryRequest request = new QueryRequest("");
        runTest(buildQueryURI(request, restIpAddress, restPort), null, 405);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, 405);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, 405);
    }

    @Test
    public void testInvalidQuery01() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1,2,3) return $y");
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testInvalidQuery02() throws Exception {
        QueryRequest request = new QueryRequest("for x in (1,2,3) return $x");
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testInvalidQuery03() throws Exception {
        QueryRequest request = new QueryRequest("insert nodes <book></book> into doc(\"abcd.xml\")/books");
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testInvalidQuery04() throws Exception {
        QueryRequest request = new QueryRequest("delete nodes /a/b//node()");
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testInvalidResultId() throws Exception {
        QueryResultRequest request = new QueryResultRequest(1000);
        runTest(buildQueryResultURI(request, restIpAddress, restPort), null, NOT_FOUND);
        runTest(buildQueryResultURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, NOT_FOUND);
        runTest(buildQueryResultURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, NOT_FOUND);
    }

    @Test
    public void testSyncInvalidInput01() throws Exception {
        QueryRequest request = new QueryRequest("   ");
        request.setAsync(false);
        runTest(buildQueryURI(request, restIpAddress, restPort), null, INVALID_INPUT);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, INVALID_INPUT);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, INVALID_INPUT);
    }

    @Test
    public void testSyncInvalidInput02() throws Exception {
        QueryRequest request = new QueryRequest("");
        request.setAsync(false);
        runTest(buildQueryURI(request, restIpAddress, restPort), null, 405);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, 405);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, 405);
    }

    @Test
    public void testSyncInvalidQuery01() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1,2,3) return $y");
        request.setAsync(false);
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testSyncInvalidQuery02() throws Exception {
        QueryRequest request = new QueryRequest("for x in (1,2,3) return $x");
        request.setAsync(false);
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testSyncInvalidQuery03() throws Exception {
        QueryRequest request = new QueryRequest("insert nodes <book></book> into doc(\"abcd.xml\")/books");
        request.setAsync(false);
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    @Test
    public void testSyncInvalidQuery04() throws Exception {
        QueryRequest request = new QueryRequest("delete nodes /a/b//node()");
        request.setAsync(false);
        runTest(buildQueryURI(request, restIpAddress, restPort), null, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_JSON, PROBLEM_WITH_QUERY);
        runTest(buildQueryURI(request, restIpAddress, restPort), CONTENT_TYPE_XML, PROBLEM_WITH_QUERY);
    }

    private void runTest(URI uri, String accepts, int expectedStatusCode) throws Exception {
        runTest(uri, accepts, expectedStatusCode, HttpMethod.GET);
        runTest(uri, accepts, expectedStatusCode, HttpMethod.POST);
    }

    private void runTest(URI uri, String accepts, int expectedStatusCode, String httpMethod) throws Exception {
        CloseableHttpClient httpClient = HttpClients.custom().setConnectionTimeToLive(20, TimeUnit.SECONDS).build();

        ErrorResponse errorResponse;
        try {
            HttpUriRequest request = getRequest(uri, httpMethod);
            if (accepts != null) {
                request.setHeader(HttpHeaders.ACCEPT, accepts);
            }

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                Assert.assertEquals(expectedStatusCode, httpResponse.getStatusLine().getStatusCode());
                if (accepts != null) {
                    Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());
                }

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = RestUtils.readEntity(entity);
                errorResponse = RestUtils.mapEntity(response, ErrorResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }

        Assert.assertNotNull(errorResponse);
        Assert.assertNotNull(errorResponse.getError().getMessage());
        Assert.assertEquals(errorResponse.getError().getCode(), expectedStatusCode);
    }
}
