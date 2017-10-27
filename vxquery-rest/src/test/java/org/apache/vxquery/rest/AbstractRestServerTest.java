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

import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.HttpMethod;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.app.VXQueryApplication;
import org.apache.vxquery.app.util.LocalClusterUtil;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.AsyncQueryResponse;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.rest.response.SyncQueryResponse;
import org.apache.vxquery.rest.service.VXQueryConfig;
import org.apache.vxquery.rest.service.VXQueryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Abstract test class to be used for {@link VXQueryApplication} related tests.
 * These tests are expected to use the REST API for querying and fetching
 * results
 *
 * @author Erandi Ganepola
 */
public abstract class AbstractRestServerTest {

    protected static LocalClusterUtil vxqueryLocalCluster = new LocalClusterUtil();
    protected static String restIpAddress;
    protected static int restPort;
    protected static VXQueryService vxQueryService;

    @BeforeClass
    public static void setUp() throws Exception {
        vxqueryLocalCluster.init(new VXQueryConfig());
        vxQueryService = vxqueryLocalCluster.getVxQueryService();
        restIpAddress = vxqueryLocalCluster.getIpAddress();
        restPort = vxqueryLocalCluster.getRestPort();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        vxqueryLocalCluster.deinit();
    }

    protected static String normalize(String string) {
        if (string == null) {
            return null;
        }

        return string.replace("\r\n", "").replace("\n", "").replace("\r", "");
    }

    protected static void checkMetrics(QueryResponse response, boolean showMetrics) {
        if (showMetrics) {
            Assert.assertTrue(response.getMetrics().getCompileTime() > 0);
            Assert.assertTrue(response.getMetrics().getElapsedTime() > 0);
        } else {
            Assert.assertTrue(response.getMetrics().getCompileTime() == 0);
            Assert.assertTrue(response.getMetrics().getElapsedTime() == 0);
        }
    }

    protected static void checkResults(AsyncQueryResponse response, boolean compileOnly) {
        if (compileOnly) {
            Assert.assertNull(response.getResultUrl());
            Assert.assertEquals(0, response.getResultId());
        } else {
            Assert.assertTrue(response.getResultUrl().startsWith(Constants.RESULT_URL_PREFIX));
            Assert.assertNotEquals(0, response.getResultId());
        }
    }

    protected static void checkResults(SyncQueryResponse response, boolean compileOnly) {
        if (compileOnly) {
            Assert.assertNull(response.getResults());
        } else {
            Assert.assertNotNull(response.getResults());
            Assert.assertFalse(response.getResults().isEmpty());
        }
    }

    /**
     * Submit a {@link QueryRequest} and fetth the resulting
     * {@link AsyncQueryResponse}
     *
     * @param uri
     *            uri of the GET request
     * @param accepts
     *            application/json | application/xml
     * @param method
     *            Http Method to be used to send the request
     * @return Response received for the query request
     * @throws Exception
     */
    protected static <T> T getQuerySuccessResponse(URI uri, String accepts, Class<T> type, String method)
            throws Exception {
        CloseableHttpClient httpClient = HttpClients.custom().setConnectionTimeToLive(20, TimeUnit.SECONDS).build();

        try {
            HttpUriRequest request = getRequest(uri, method);

            if (accepts != null) {
                request.setHeader(HttpHeaders.ACCEPT, accepts);
            }

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                Assert.assertEquals(HttpResponseStatus.OK.code(), httpResponse.getStatusLine().getStatusCode());
                if (accepts != null) {
                    Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());
                }

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = RestUtils.readEntity(entity);
                return RestUtils.mapEntity(response, type, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    /**
     * Fetch the {@link QueryResultResponse} from query result endpoint once the
     * corresponding {@link QueryResultRequest} is given.
     *
     * @param resultRequest
     *            {@link QueryResultRequest}
     * @param accepts
     *            expected
     *
     *            <pre>
     *            Accepts
     *            </pre>
     *
     *            header in responses
     * @param method
     *            Http Method to be used to send the request
     * @return query result response received
     * @throws Exception
     */
    protected static QueryResultResponse getQueryResultResponse(QueryResultRequest resultRequest, String accepts,
            String method) throws Exception {
        URI uri = RestUtils.buildQueryResultURI(resultRequest, restIpAddress, restPort);
        CloseableHttpClient httpClient = HttpClients.custom().setConnectionTimeToLive(20, TimeUnit.SECONDS).build();
        try {
            HttpUriRequest request = getRequest(uri, method);

            if (accepts != null) {
                request.setHeader(HttpHeaders.ACCEPT, accepts);
            }

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                if (accepts != null) {
                    Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());
                }
                Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = RestUtils.readEntity(entity);
                return RestUtils.mapEntity(response, QueryResultResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    /**
     * Creates a POST or GET request accordingly from the given {@link URI}
     *
     * @param uri
     *            URI to which the request us to be sent
     * @param method
     *            Http method- GET or POST
     * @return request
     */
    protected static HttpUriRequest getRequest(URI uri, String method) {
        HttpUriRequest request;
        switch (method) {
            case HttpMethod.POST:
                request = new HttpPost(uri);
                break;
            case HttpMethod.GET:
            default:
                request = new HttpGet(uri);
        }

        return request;
    }
}
