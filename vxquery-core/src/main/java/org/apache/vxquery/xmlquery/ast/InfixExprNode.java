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
package org.apache.vxquery.xmlquery.ast;

import org.apache.vxquery.util.SourceLocation;

public class InfixExprNode extends ASTNode {
    private InfixOperator operator;
    private ASTNode leftExpr;
    private ASTNode rightExpr;

    public InfixExprNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.INFIX_EXPRESSION;
    }

    public enum InfixOperator {
        OR, AND, GENERAL_EQ, GENERAL_NE, GENERAL_LT, GENERAL_LE, GENERAL_GT, GENERAL_GE, VALUE_EQ, VALUE_NE, VALUE_LT, VALUE_LE, VALUE_GT, VALUE_GE, RANGE, PLUS, MINUS, MULTIPLY, DIV, IDIV, MOD, UNION, INTERSECT, EXCEPT, IS, PRECEDES, FOLLOWS
    }

    public InfixOperator getOperator() {
        return operator;
    }

    public void setOperator(InfixOperator operator) {
        this.operator = operator;
    }

    public ASTNode getLeftExpr() {
        return leftExpr;
    }

    public void setLeftExpr(ASTNode leftExpr) {
        this.leftExpr = leftExpr;
    }

    public ASTNode getRightExpr() {
        return rightExpr;
    }

    public void setRightExpr(ASTNode rightExpr) {
        this.rightExpr = rightExpr;
    }
}
