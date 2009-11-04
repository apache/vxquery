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
package org.apache.vxquery.compiler.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.functions.Function;

public class FunctionCallExpression extends Expression {
    private List<ExpressionHandle> args;

    private Function function;

    public FunctionCallExpression(StaticContext ctx, Function function, List<Expression> args) {
        super(ctx);
        this.function = function;
        this.args = new ArrayList<ExpressionHandle>();
        for (Expression e : args) {
            this.args.add(new ExpressionHandle(e));
        }
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.FUNCTION;
    }

    public List<ExpressionHandle> getArguments() {
        return args;
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitFunctionCallExpression(this);
    }
}