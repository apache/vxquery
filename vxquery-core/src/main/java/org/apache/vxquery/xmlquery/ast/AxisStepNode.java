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

public class AxisStepNode extends ASTNode {
    private Axis axis;
    private ASTNode nodeTest;
    private List<ASTNode> predicates;
    
    public AxisStepNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.AXIS_STEP;
    }
    
    public enum Axis {
        CHILD, DESCENDANT, ATTRIBUTE, SELF, DESCENDANT_OR_SELF, FOLLOWING_SIBLING, FOLLOWING, ABBREV, ABBREV_ATTRIBUTE, PARENT, ANCESTOR, PRECEDING_SIBLING, PRECEDING, ANCESTOR_OR_SELF, DOT_DOT
    }

    public Axis getAxis() {
        return axis;
    }

    public void setAxis(Axis axis) {
        this.axis = axis;
    }

    public ASTNode getNodeTest() {
        return nodeTest;
    }

    public void setNodeTest(ASTNode nodeTest) {
        this.nodeTest = nodeTest;
    }

    public List<ASTNode> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<ASTNode> predicates) {
        this.predicates = predicates;
    }
}
