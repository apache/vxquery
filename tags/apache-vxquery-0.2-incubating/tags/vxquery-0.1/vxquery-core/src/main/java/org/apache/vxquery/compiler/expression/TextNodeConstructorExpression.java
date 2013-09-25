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

public class TextNodeConstructorExpression extends NodeConstructorExpression {
    private ExpressionHandle content;

    public TextNodeConstructorExpression(StaticContext ctx, Expression content) {
        super(ctx);
        this.content = new ExpressionHandle(content);
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.TEXT;
    }

    public ExpressionHandle getContent() {
        return content;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitTextNodeConstructorExpression(this);
    }

    @Override
    public <T> T accept(MutableExpressionVisitor<T> visitor, ExpressionHandle handle) {
        return visitor.visitTextNodeConstructorExpression(handle);
    }

    @Override
    public Expression copy(Map<Variable, Expression> substitution) {
        return new TextNodeConstructorExpression(ctx, content.get().copy(substitution));
    }
}