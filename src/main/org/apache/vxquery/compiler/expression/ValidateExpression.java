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

import org.apache.vxquery.context.StaticContext;

public class ValidateExpression extends Expression {
    private ExpressionHandle input;
    private Mode mode;

    public ValidateExpression(StaticContext ctx, Expression input, Mode mode) {
        super(ctx);
        this.input = new ExpressionHandle(input);
        this.mode = mode;
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.VALIDATE;
    }

    public enum Mode {
        LAX, STRICT, DEFAULT
    }

    public Expression getInput() {
        return input.get();
    }

    public void setInput(Expression input) {
        this.input.set(input);
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitValidateExpression(this);
    }
}