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
import org.apache.vxquery.datamodel.AxisKind;
import org.apache.vxquery.types.NodeType;

public class PathStepExpression extends Expression {
    private ExpressionHandle input;
    private AxisKind axis;
    private NodeType nodeType;

    public PathStepExpression(StaticContext ctx, Expression input, AxisKind axis, NodeType nodeType) {
        super(ctx);
        this.input = new ExpressionHandle(input);
        this.axis = axis;
        this.nodeType = nodeType;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitPathStepExpression(this);
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.PATH_STEP;
    }

    public ExpressionHandle getInput() {
        return input;
    }

    public AxisKind getAxis() {
        return axis;
    }

    public void setAxis(AxisKind axis) {
        this.axis = axis;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public <T> T accept(MutableExpressionVisitor<T> visitor, ExpressionHandle handle) {
        return visitor.visitPathStepExpression(handle);
    }

    @Override
    public Expression copy(Map<Variable, Expression> substitution) {
        return new PathStepExpression(ctx, input.get().copy(substitution), axis, nodeType);
    }
}