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

package org.apache.vxquery.rest.response;

import org.apache.vxquery.rest.service.Status;

/**
 * The base class of the query response (the response returned when a query is
 * sent for execution)
 * 
 * @author Erandi Ganepola
 */
public class QueryResponse extends APIResponse {

    private String statement;
    private String abstractSyntaxTree;
    private String translatedExpressionTree;
    private String optimizedExpressionTree;
    private String runtimePlan;
    private Metrics metrics = new Metrics();

    public QueryResponse() {
        super(Status.SUCCESS.toString());
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getAbstractSyntaxTree() {
        return abstractSyntaxTree;
    }

    public void setAbstractSyntaxTree(String abstractSyntaxTree) {
        this.abstractSyntaxTree = abstractSyntaxTree;
    }

    public String getTranslatedExpressionTree() {
        return translatedExpressionTree;
    }

    public void setTranslatedExpressionTree(String translatedExpressionTree) {
        this.translatedExpressionTree = translatedExpressionTree;
    }

    public String getOptimizedExpressionTree() {
        return optimizedExpressionTree;
    }

    public void setOptimizedExpressionTree(String optimizedExpressionTree) {
        this.optimizedExpressionTree = optimizedExpressionTree;
    }

    public String getRuntimePlan() {
        return runtimePlan;
    }

    public void setRuntimePlan(String runtimePlan) {
        this.runtimePlan = runtimePlan;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }
}
