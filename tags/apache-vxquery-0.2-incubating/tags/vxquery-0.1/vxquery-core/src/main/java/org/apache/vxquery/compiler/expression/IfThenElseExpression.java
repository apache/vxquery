/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.expression;

import java.util.Map;

import org.apache.vxquery.context.StaticContext;

public class IfThenElseExpression extends Expression {
    private ExpressionHandle condition;
    private ExpressionHandle thenExpr;
    private ExpressionHandle elseExpr;

    public IfThenElseExpression(StaticContext ctx, Expression condition, Expression thenExpr, Expression elseExpr) {
        super(ctx);
        this.condition = new ExpressionHandle(condition);
        this.thenExpr = new ExpressionHandle(thenExpr);
        this.elseExpr = new ExpressionHandle(elseExpr);
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.IFTHENELSE;
    }

    public ExpressionHandle getCondition() {
        return condition;
    }

    public ExpressionHandle getThenExpression() {
        return thenExpr;
    }

    public ExpressionHandle getElseExpression() {
        return elseExpr;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitIfThenElseExpression(this);
    }

    @Override
    public <T> T accept(MutableExpressionVisitor<T> visitor, ExpressionHandle handle) {
        return visitor.visitIfThenElseExpression(handle);
    }

    @Override
    public Expression copy(Map<Variable, Expression> substitution) {
        return new IfThenElseExpression(ctx, condition.get().copy(substitution), thenExpr.get().copy(substitution),
                elseExpr.get().copy(substitution));
    }
}