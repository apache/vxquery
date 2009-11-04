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

import java.util.List;

import org.apache.vxquery.context.StaticContext;

public class QuantifiedExpression extends Expression {
    private Quantification quantification;
    private List<ForLetVariable> quantVars;
    private ExpressionHandle satisfiesExpr;

    public QuantifiedExpression(StaticContext ctx, Quantification quantification, List<ForLetVariable> quantVars,
            Expression satisfiesExpr) {
        super(ctx);
        this.quantification = quantification;
        this.quantVars = quantVars;
        this.satisfiesExpr = new ExpressionHandle(satisfiesExpr);
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.QUANTIFIED;
    }

    public enum Quantification {
        SOME, EVERY,
    }

    public Quantification getQuantification() {
        return quantification;
    }

    public void setQuantification(Quantification quantification) {
        this.quantification = quantification;
    }

    public List<ForLetVariable> getQuantifiedVariables() {
        return quantVars;
    }

    public void setQuantifiedVariables(List<ForLetVariable> quantVars) {
        this.quantVars = quantVars;
    }

    public Expression getSatisfiesExpression() {
        return satisfiesExpr.get();
    }

    public void setSatisfiesExpression(Expression satisfiesExpr) {
        this.satisfiesExpr.set(satisfiesExpr);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitQuantifiedExpression(this);
    }
}