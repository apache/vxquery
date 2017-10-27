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

package org.apache.vxquery.app.util;

import static org.apache.vxquery.rest.Constants.MODE_ASYNC;
import static org.apache.vxquery.rest.Constants.MODE_SYNC;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;
import static org.apache.vxquery.rest.Constants.Parameters.COMPILE_ONLY;
import static org.apache.vxquery.rest.Constants.Parameters.FRAME_SIZE;
import static org.apache.vxquery.rest.Constants.Parameters.METRICS;
import static org.apache.vxquery.rest.Constants.Parameters.MODE;
import static org.apache.vxquery.rest.Constants.Parameters.OPTIMIZATION;
import static org.apache.vxquery.rest.Constants.Parameters.REPEAT_EXECUTIONS;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_AST;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_OET;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_RP;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_TET;
import static org.apache.vxquery.rest.Constants.Parameters.STATEMENT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_RESULT_ENDPOINT;
import static org.apache.vxquery.rest.Constants.Parameters.USE_INDEX;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.utils.URIBuilder;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;

/**
 * A set of utility methods used by the REST related tasks
 *
 * @author Erandi Ganepola
 */
public class RestUtils {

    private RestUtils() {
    }

    /**
     * Builds the {@link URI} once the {@link QueryRequest} is given. Only the
     * parameters given (different from the default values) are put in the
     * {@link URI}
     * 
     * @param request
     *            {@link QueryRequest} to be converted to a {@link URI}
     * @param restIpAddress
     *            Ip address of the REST server
     * @param restPort
     *            port of the REST server
     * @return generated {@link URI}
     * @throws URISyntaxException
     */
    public static URI buildQueryURI(QueryRequest request, String restIpAddress, int restPort)
            throws URISyntaxException {
        URIBuilder builder =
                new URIBuilder().setScheme("http").setHost(restIpAddress).setPort(restPort).setPath(QUERY_ENDPOINT);

        if (request.getStatement() != null) {
            builder.addParameter(STATEMENT, request.getStatement());
        }
        if (request.isCompileOnly()) {
            builder.addParameter(COMPILE_ONLY, String.valueOf(request.isCompileOnly()));
        }
        if (request.getOptimization() != QueryRequest.DEFAULT_OPTIMIZATION) {
            builder.addParameter(OPTIMIZATION, String.valueOf(request.getOptimization()));
        }
        if (request.getFrameSize() != QueryRequest.DEFAULT_FRAMESIZE) {
            builder.addParameter(FRAME_SIZE, String.valueOf(request.getFrameSize()));
        }
        if (request.getRepeatExecutions() != 1) {
            builder.addParameter(REPEAT_EXECUTIONS, String.valueOf(request.getRepeatExecutions()));
        }
        if (request.isShowMetrics()) {
            builder.addParameter(METRICS, String.valueOf(request.isShowMetrics()));
        }
        if (request.isShowAbstractSyntaxTree()) {
            builder.addParameter(SHOW_AST, String.valueOf(request.isShowAbstractSyntaxTree()));
        }
        if (request.isShowTranslatedExpressionTree()) {
            builder.addParameter(SHOW_TET, String.valueOf(request.isShowTranslatedExpressionTree()));
        }
        if (request.isShowOptimizedExpressionTree()) {
            builder.addParameter(SHOW_OET, String.valueOf(request.isShowOptimizedExpressionTree()));
        }
        if (request.isShowRuntimePlan()) {
            builder.addParameter(SHOW_RP, String.valueOf(request.isShowRuntimePlan()));
        }
        if (!request.isAsync()) {
            builder.addParameter(MODE, request.isAsync() ? MODE_ASYNC : MODE_SYNC);
        }
        if (request.useIndexing()) {
            builder.addParameter(USE_INDEX, String.valueOf(request.useIndexing()));
        }

        return builder.build();
    }

    /**
     * Builds the query result {@link URI} given the {@link QueryResultRequest}
     * 
     * @param resultRequest
     *            result request
     * @param restIpAddress
     *            rest server's ip
     * @param restPort
     *            port of the rest server
     * @return generated {@link URI}
     * @throws URISyntaxException
     */
    public static URI buildQueryResultURI(QueryResultRequest resultRequest, String restIpAddress, int restPort)
            throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost(restIpAddress).setPort(restPort)
                .setPath(QUERY_RESULT_ENDPOINT.replace("*", String.valueOf(resultRequest.getResultId())));

        if (resultRequest.isShowMetrics()) {
            builder.setParameter(METRICS, String.valueOf(resultRequest.isShowMetrics()));
        }

        return builder.build();
    }

    /**
     * Reads the entity from an {@link HttpEntity}
     * 
     * @param entity
     *            entity instance to be read
     * @return entity read by this method as a string
     * @throws IOException
     */
    public static String readEntity(HttpEntity entity) throws IOException {
        StringBuilder responseBody = new StringBuilder();

        try (InputStream in = entity.getContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                responseBody.append(line);
            }
        }
        return responseBody.toString();
    }

    /**
     * Maps the object in the string representation to a java object. To map json
     * entities, this method use {@link ObjectMapper}. For XML this method use
     * {@link Unmarshaller}.
     * 
     * @param entity
     *            string representation of the object
     * @param type
     *            the class to which the string needs to be mapped to
     * @param contentType
     *            json or XML
     * @param <T>
     *            content's class type
     * @return mapped object
     * @throws IOException
     * @throws JAXBException
     */
    public static <T> T mapEntity(String entity, Class<T> type, String contentType) throws IOException, JAXBException {
        if (contentType == null) {
            contentType = CONTENT_TYPE_JSON;
        }

        switch (contentType) {
            case CONTENT_TYPE_XML:
                JAXBContext jaxbContext = JAXBContext.newInstance(type);
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                return type.cast(unmarshaller.unmarshal(new StringReader(entity)));
            case CONTENT_TYPE_JSON:
            default:
                ObjectMapper jsonMapper = new ObjectMapper();
                return jsonMapper.readValue(entity, type);
        }
    }
}
