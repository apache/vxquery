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
package org.apache.vxquery.compiler.expression;

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.util.Pair;

public class ExtensionExpression extends Expression {
    private List<Pair<QName, String>> pragmas;
    private ExpressionHandle input;

    public ExtensionExpression(StaticContext ctx, Expression input, List<Pair<QName, String>> pragmas) {
        super(ctx);
        this.input = new ExpressionHandle(input);
        this.pragmas = pragmas;
    }

    @Override
    public ExprTag getTag() {
        return ExprTag.EXTENSION;
    }

    public List<Pair<QName, String>> getPragmas() {
        return pragmas;
    }

    public void setPragmas(List<Pair<QName, String>> pragmas) {
        this.pragmas = pragmas;
    }

    public Expression getInput() {
        return input.get();
    }

    public void setInput(Expression input) {
        this.input.set(input);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitExtensionExpression(this);
    }
}