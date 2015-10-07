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

import java.util.List;

import org.apache.vxquery.util.SourceLocation;

public class OrderbyClauseNode extends FLWORClauseNode {
    private List<OrderSpecNode> orderSpec;
    private boolean stable;
    
    public OrderbyClauseNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.ORDERBY_CLAUSE;
    }

    public List<OrderSpecNode> getOrderSpec() {
        return orderSpec;
    }

    public void setOrderSpec(List<OrderSpecNode> orderSpec) {
        this.orderSpec = orderSpec;
    }

    public boolean isStable() {
        return stable;
    }

    public void setStable(boolean stable) {
        this.stable = stable;
    }
}
