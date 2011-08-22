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

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.types.SequenceType;

public abstract class TypeExpression extends Expression {
    protected ExpressionHandle input;
    protected SequenceType type;

    public TypeExpression(StaticContext ctx, Expression input, SequenceType type) {
        super(ctx);
        this.input = new ExpressionHandle(input);
        this.type = type;
    }

    public ExpressionHandle getInput() {
        return input;
    }

    public SequenceType getType() {
        return type;
    }

    public void setType(SequenceType type) {
        this.type = type;
    }
}