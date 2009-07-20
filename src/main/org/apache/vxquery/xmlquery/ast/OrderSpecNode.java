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

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.util.SourceLocation;
import org.apache.vxquery.xmlquery.query.XQueryConstants;

public class OrderSpecNode extends ASTNode {
    private ASTNode expression;
    private XQueryConstants.OrderDirection direction;
    private StaticContext.EmptyOrderProperty emptyOrder;
    private String collation;
    
    public OrderSpecNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.ORDER_SPECIFICATION;
    }

    public ASTNode getExpression() {
        return expression;
    }

    public void setExpression(ASTNode expression) {
        this.expression = expression;
    }

    public XQueryConstants.OrderDirection getDirection() {
        return direction;
    }

    public void setDirection(XQueryConstants.OrderDirection direction) {
        this.direction = direction;
    }

    public StaticContext.EmptyOrderProperty getEmptyOrder() {
        return emptyOrder;
    }

    public void setEmptyOrder(StaticContext.EmptyOrderProperty emptyOrder) {
        this.emptyOrder = emptyOrder;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }
}