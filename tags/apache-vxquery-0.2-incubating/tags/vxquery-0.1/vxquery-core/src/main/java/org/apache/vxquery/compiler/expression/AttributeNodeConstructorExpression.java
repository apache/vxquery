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
import org.apache.vxquery.types.NodeKind;

public class AttributeNodeConstructorExpression extends ElementOrAttributeNodeConstructorExpression {
    public AttributeNodeConstructorExpression(StaticContext ctx, Expression name, Expression content) {
        super(ctx, name, content);
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.ATTRIBUTE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitAttributeNodeConstructorExpression(this);
    }

    @Override
    public <T> T accept(MutableExpressionVisitor<T> visitor, ExpressionHandle handle) {
        return visitor.visitAttributeNodeConstructorExpression(handle);
    }

    @Override
    public Expression copy(Map<Variable, Expression> substitution) {
        return new AttributeNodeConstructorExpression(ctx, name.get().copy(substitution), content.get().copy(
                substitution));
    }
}