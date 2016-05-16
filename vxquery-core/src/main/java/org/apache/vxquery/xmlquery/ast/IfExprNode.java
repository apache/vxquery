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

public class IfExprNode extends ASTNode {
    private ASTNode ifExpr;
    private ASTNode thenExpr;
    private ASTNode elseExpr;

    public IfExprNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.IF_EXPRESSION;
    }

    public ASTNode getIfExpr() {
        return ifExpr;
    }

    public void setIfExpr(ASTNode ifExpr) {
        this.ifExpr = ifExpr;
    }

    public ASTNode getThenExpr() {
        return thenExpr;
    }

    public void setThenExpr(ASTNode thenExpr) {
        this.thenExpr = thenExpr;
    }

    public ASTNode getElseExpr() {
        return elseExpr;
    }

    public void setElseExpr(ASTNode elseExpr) {
        this.elseExpr = elseExpr;
    }
}
