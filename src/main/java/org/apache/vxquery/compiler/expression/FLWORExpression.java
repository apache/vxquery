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

public class FLWORExpression extends Expression {
    private List<Clause> clauses;
    private ExpressionHandle rExpr;

    public FLWORExpression(StaticContext ctx, List<Clause> clauses, Expression rExpr) {
        super(ctx);
        this.clauses = clauses;
        this.rExpr = new ExpressionHandle(rExpr);
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.FLWOR;
    }

    public List<Clause> getClauses() {
        return clauses;
    }

    public Expression getReturnExpression() {
        return rExpr.get();
    }

    public void setReturnExpression(Expression rExpr) {
        this.rExpr.set(rExpr);
    }

    public enum ClauseTag {
        FOR, LET, WHERE, ORDERBY,
    }

    public static abstract class Clause {
        public abstract ClauseTag getTag();
    }

    public static final class ForClause extends Clause {
        private ForLetVariable forVar;
        private PositionVariable posVar;
        private ScoreVariable scoreVar;

        public ForClause(ForLetVariable forVar, PositionVariable posVar, ScoreVariable scoreVar) {
            this.forVar = forVar;
            this.posVar = posVar;
            this.scoreVar = scoreVar;
        }

        @Override
        public ClauseTag getTag() {
            return ClauseTag.FOR;
        }

        public ForLetVariable getForVariable() {
            return forVar;
        }

        public void setForVariable(ForLetVariable forVar) {
            this.forVar = forVar;
        }

        public PositionVariable getPosVariable() {
            return posVar;
        }

        public void setPosVariable(PositionVariable posVar) {
            this.posVar = posVar;
        }

        public ScoreVariable getScoreVariable() {
            return scoreVar;
        }

        public void setScoreVariable(ScoreVariable scoreVar) {
            this.scoreVar = scoreVar;
        }
    }

    public static final class LetClause extends Clause {
        private ForLetVariable letVar;

        public LetClause(ForLetVariable letVar) {
            this.letVar = letVar;
        }

        @Override
        public ClauseTag getTag() {
            return ClauseTag.LET;
        }

        public ForLetVariable getLetVariable() {
            return letVar;
        }

        public void setLetVariable(ForLetVariable letVar) {
            this.letVar = letVar;
        }
    }

    public static final class WhereClause extends Clause {
        private ExpressionHandle condition;

        public WhereClause(Expression condition) {
            this.condition = new ExpressionHandle(condition);
        }

        @Override
        public ClauseTag getTag() {
            return ClauseTag.WHERE;
        }

        public Expression getCondition() {
            return condition.get();
        }

        public void setCondition(Expression condition) {
            this.condition.set(condition);
        }
    }

    public enum OrderDirection {
        ASCENDING, DESCENDING,
    }

    public enum EmptyOrder {
        GREATEST, LEAST, DEFAULT,
    }

    public static final class OrderbyClause extends Clause {
        private List<ExpressionHandle> orderingExpressions;
        private List<OrderDirection> orderingDirections;
        private List<EmptyOrder> emptyOrders;
        private List<String> collations;
        private boolean stable;

        public OrderbyClause(List<Expression> orderingExpressions, List<OrderDirection> orderingDirections,
                List<EmptyOrder> emptyOrders, List<String> collations, boolean stable) {
            this.orderingExpressions = new ArrayList<ExpressionHandle>();
            for (Expression e : orderingExpressions) {
                this.orderingExpressions.add(new ExpressionHandle(e));
            }
            this.orderingDirections = orderingDirections;
            this.emptyOrders = emptyOrders;
            this.collations = collations;
            this.stable = stable;
        }

        @Override
        public ClauseTag getTag() {
            return ClauseTag.ORDERBY;
        }

        public List<ExpressionHandle> getOrderingExpressions() {
            return orderingExpressions;
        }

        public List<OrderDirection> getOrderingDirections() {
            return orderingDirections;
        }

        public void setOrderingDirections(List<OrderDirection> orderingDirections) {
            this.orderingDirections = orderingDirections;
        }

        public List<EmptyOrder> getEmptyOrders() {
            return emptyOrders;
        }

        public void setEmptyOrders(List<EmptyOrder> emptyOrders) {
            this.emptyOrders = emptyOrders;
        }

        public List<String> getCollations() {
            return collations;
        }

        public void setCollations(List<String> collations) {
            this.collations = collations;
        }

        public boolean isStable() {
            return stable;
        }

        public void setStability(boolean stable) {
            this.stable = stable;
        }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitFLWORExpression(this);
    }
}