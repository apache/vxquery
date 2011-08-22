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
package org.apache.vxquery.compiler.rewriter.rules;

import org.apache.vxquery.compiler.expression.ConstantExpression;
import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.InstanceofExpression;
import org.apache.vxquery.compiler.rewriter.framework.AbstractRewriteRule;
import org.apache.vxquery.compiler.tools.ExpressionUtils;
import org.apache.vxquery.datamodel.atomic.BooleanValue;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.types.TypeOperations;
import org.apache.vxquery.types.XQType;

public class EvaluateInvariantInstanceofExpressions extends AbstractRewriteRule {
    public EvaluateInvariantInstanceofExpressions(int minOptimizationLevel) {
        super(minOptimizationLevel);
    }

    public boolean rewritePost(ExpressionHandle exprHandle) {
        Expression expr = exprHandle.get();
        if (expr.getTag() == ExprTag.INSTANCEOF) {
            InstanceofExpression ie = (InstanceofExpression) expr;
            XQType eType = ie.getInput().accept(ExpressionUtils.createTypeInferringVisitor());
            XQType rType = ie.getType().toXQType();
            if (TypeOperations.isSubtypeOf(eType, rType)) {
                exprHandle.set(new ConstantExpression(expr.getStaticContext(), BooleanValue.TRUE, SequenceType.create(
                        BuiltinTypeRegistry.XS_BOOLEAN, Quantifier.QUANT_ONE)));
                return true;
            }
        }
        return false;
    }
}