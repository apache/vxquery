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

package org.apache.vxquery.rest.request;

import org.apache.vxquery.rest.RestServer;

/**
 * Request to represent a query request coming to the {@link RestServer}
 *
 * @author Erandi Ganepola
 */
public class QueryResultRequest {

    private long resultId;
    private boolean showMetrics = false;
    private String requestId;

    public QueryResultRequest(long resultId) {
        this(resultId, null);
    }

    public QueryResultRequest(long resultId, String requestId) {
        this.resultId = resultId;
        this.requestId = requestId;
    }

    public long getResultId() {
        return resultId;
    }

    public boolean isShowMetrics() {
        return showMetrics;
    }

    public void setShowMetrics(boolean showMetrics) {
        this.showMetrics = showMetrics;
    }

    public String getRequestId() {
        return requestId;
    }
}
