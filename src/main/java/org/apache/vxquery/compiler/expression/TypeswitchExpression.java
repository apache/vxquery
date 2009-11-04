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
import org.apache.vxquery.types.SequenceType;

public class TypeswitchExpression extends Expression {
    private ForLetVariable input;

    private List<Case> cases;

    private Expression defaultExpr;

    public TypeswitchExpression(StaticContext ctx, ForLetVariable input, List<Case> cases, Expression defaultExpr) {
        super(ctx);
        this.input = input;
        this.cases = cases;
        this.defaultExpr = defaultExpr;
    }

    public ForLetVariable getInput() {
        return input;
    }

    public void setInput(ForLetVariable input) {
        this.input = input;
    }

    public List<Case> getCases() {
        return cases;
    }

    public Expression getDefaultExpression() {
        return defaultExpr;
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.TYPESWITCH;
    }

    public static final class Case {
        private ForLetVariable caseVar;
        private SequenceType type;
        private ExpressionHandle rExpr;

        public Case(ForLetVariable caseVar, SequenceType type, Expression rExpr) {
            this.caseVar = caseVar;
            this.type = type;
            this.rExpr = new ExpressionHandle(rExpr);
        }

        public ForLetVariable getCaseVariable() {
            return caseVar;
        }

        public void setCaseVariable(ForLetVariable caseVar) {
            this.caseVar = caseVar;
        }

        public SequenceType getCaseType() {
            return type;
        }

        public void setCaseType(SequenceType type) {
            this.type = type;
        }

        public Expression getReturnExpression() {
            return rExpr.get();
        }

        public void setReturnExpression(Expression rExpr) {
            this.rExpr.set(rExpr);
        }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitTypeswitchExpression(this);
    }
}