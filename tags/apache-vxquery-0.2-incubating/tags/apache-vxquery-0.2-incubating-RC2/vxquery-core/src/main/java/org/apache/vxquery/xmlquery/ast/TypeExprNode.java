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

public class TypeExprNode extends ASTNode {
    private TypeOperator operator;
    private ASTNode expr;
    private ASTNode type;
    
    public TypeExprNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.TYPE_EXPRESSION;
    }
    
    public enum TypeOperator {
        TREAT, CASTABLE, CAST, INSTANCEOF
    }

    public TypeOperator getOperator() {
        return operator;
    }

    public void setOperator(TypeOperator operator) {
        this.operator = operator;
    }

    public ASTNode getExpr() {
        return expr;
    }

    public void setExpr(ASTNode expr) {
        this.expr = expr;
    }

    public ASTNode getType() {
        return type;
    }

    public void setType(ASTNode type) {
        this.type = type;
    }
}