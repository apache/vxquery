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

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.vxquery.rest.Constants;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.Error;
import org.apache.vxquery.rest.service.VXQueryService;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Servlet to handle query results requests.
 *
 * @author Erandi Ganepola
 */
public class QueryResultAPIServlet extends RestAPIServlet {

    private VXQueryService vxQueryService;

    public QueryResultAPIServlet(VXQueryService vxQueryService, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.vxQueryService = vxQueryService;
    }

    @Override
    protected APIResponse doHandle(IServletRequest request) {
        String uri = request.getHttpRequest().uri();
        System.out.println("uri request:"+uri);
        long resultId;
        try {
            String pathParam = uri.substring(uri.lastIndexOf("/") + 1);
            pathParam = pathParam.contains("?") ? pathParam.split("\\?")[0] : pathParam;
            resultId = Long.parseLong(pathParam);
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Result ID could not be retrieved from URL");
            return APIResponse.newErrorResponse(null, Error.builder().withCode(HttpResponseStatus.BAD_REQUEST.code())
                    .withMessage("Result ID couldn't be retrieved from URL").build());
        }

        QueryResultRequest resultRequest = new QueryResultRequest(resultId, UUID.randomUUID().toString());
        resultRequest.setShowMetrics(Boolean.parseBoolean(request.getParameter(Constants.Parameters.METRICS)));
        LOGGER.log(Level.INFO,
                String.format("Received a result request with resultId : %d", resultRequest.getResultId()));
        return vxQueryService.getResult(resultRequest);
    }
}
