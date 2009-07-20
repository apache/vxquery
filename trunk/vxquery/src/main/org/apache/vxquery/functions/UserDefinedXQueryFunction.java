/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.functions;

import javax.xml.namespace.QName;

import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ParameterVariable;
import org.apache.vxquery.runtime.RuntimePlan;
import org.apache.vxquery.runtime.base.FunctionIteratorFactory;

public class UserDefinedXQueryFunction extends Function {
    private ParameterVariable[] params;
    private Expression body;
    private RuntimePlan plan;

    public UserDefinedXQueryFunction(QName name, Signature signature, Expression body) {
        super(name, signature);
        this.body = body;
    }

    public Expression getBody() {
        return body;
    }

    public void setBody(Expression body) {
        this.body = body;
    }

    @Override
    public FunctionTag getTag() {
        return FunctionTag.UDXQUERY;
    }

    @Override
    public FunctionIteratorFactory getIteratorFactory() {
        return null;
    }

    public void setRuntimePlan(RuntimePlan plan) {
        this.plan = plan;
    }

    public ParameterVariable[] getParameters() {
        return params;
    }

    public void setParameters(ParameterVariable[] params) {
        this.params = params;
    }

    @Override
    public boolean useContextImplicitly() {
        return false;
    }
}