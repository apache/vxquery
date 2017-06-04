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
package org.apache.vxquery.rest.servlet;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static org.apache.vxquery.rest.Constants.MODE_ASYNC;
import static org.apache.vxquery.rest.Constants.MODE_SYNC;
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.Constants;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.Error;
import org.apache.vxquery.rest.service.VXQueryService;

/**
 * Servlet to handle query requests.
 *
 * @author Erandi Ganepola
 */
public class QueryAPIServlet extends RestAPIServlet {

    private VXQueryService vxQueryService;

    public QueryAPIServlet(VXQueryService vxQueryService, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.vxQueryService = vxQueryService;
    }

    @Override
    protected APIResponse doHandle(IServletRequest request) {
        LOGGER.log(Level.INFO,
                String.format("Received a query request with query : %s", request.getParameter("statement")));

        QueryRequest queryRequest;
        try {
            queryRequest = getQueryRequest(request);
        } catch (Exception e) {
            return APIResponse.newErrorResponse(null,
                    Error.builder().withCode(Constants.ErrorCodes.INVALID_INPUT).withMessage("Invalid input").build());
        }

        try {
            return vxQueryService.execute(queryRequest);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when trying to execute query : " + queryRequest.getStatement(), e);
            return APIResponse.newErrorResponse(queryRequest.getRequestId(), Error.builder()
                    .withCode(Constants.ErrorCodes.UNFORSEEN_PROBLEM).withMessage(e.getMessage()).build());
        }
    }

    private QueryRequest getQueryRequest(IServletRequest request) throws IOException, JAXBException {
        if (request.getParameter(STATEMENT) == null || request.getParameter(STATEMENT).trim().isEmpty()) {
            throw new IllegalArgumentException("Parameter 'statement' is required to handle the request");
        }

        QueryRequest queryRequest = new QueryRequest(UUID.randomUUID().toString(), request.getParameter(STATEMENT));
        queryRequest.setCompileOnly(Boolean.parseBoolean(request.getParameter(COMPILE_ONLY)));
        queryRequest.setShowMetrics(Boolean.parseBoolean(request.getParameter(METRICS)));

        queryRequest.setShowAbstractSyntaxTree(Boolean.parseBoolean(request.getParameter(SHOW_AST)));
        queryRequest.setShowTranslatedExpressionTree(Boolean.parseBoolean(request.getParameter(SHOW_TET)));
        queryRequest.setShowOptimizedExpressionTree(Boolean.parseBoolean(request.getParameter(SHOW_OET)));
        queryRequest.setShowRuntimePlan(Boolean.parseBoolean(request.getParameter(SHOW_RP)));

        if (request.getParameter(OPTIMIZATION) != null) {
            queryRequest.setOptimization(Integer.parseInt(request.getParameter(OPTIMIZATION)));
        }
        if (request.getParameter(FRAME_SIZE) != null) {
            queryRequest.setFrameSize(Integer.parseInt(request.getParameter(FRAME_SIZE)));
        }
        if (request.getParameter(REPEAT_EXECUTIONS) != null) {
            queryRequest.setRepeatExecutions(Integer.parseInt(request.getParameter(REPEAT_EXECUTIONS)));
        }

        String sourceFileMap = request.getHttpRequest().content().toString(StandardCharsets.UTF_8);
        if (sourceFileMap != null && !sourceFileMap.isEmpty()) {
            Map<String, String> map = (Map<String, String>) RestUtils.mapEntity(sourceFileMap, Map.class,
                    request.getHeader(CONTENT_TYPE));
            LOGGER.log(Level.FINE, "Found source file map");
            Map<String, File> fileMap = map.entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), new File(entry.getValue())))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
            queryRequest.setSourceFileMap(fileMap);
        }

        if (request.getParameter(MODE) != null) {
            switch (request.getParameter(MODE)) {
                case MODE_SYNC:
                    queryRequest.setAsync(false);
                    break;
                case MODE_ASYNC:
                default:
                    queryRequest.setAsync(true);
                    break;
            }
        }

        return queryRequest;
    }
}
