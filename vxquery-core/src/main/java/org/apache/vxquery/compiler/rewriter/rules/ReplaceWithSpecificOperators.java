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

import java.util.logging.Logger;

import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.FunctionCallExpression;
import org.apache.vxquery.compiler.rewriter.framework.AbstractRewriteRule;
import org.apache.vxquery.compiler.tools.ExpressionUtils;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.TypeOperations;
import org.apache.vxquery.types.XQType;

public class ReplaceWithSpecificOperators extends AbstractRewriteRule {
    private static final Logger LOGGER = Logger.getLogger(ReplaceWithSpecificOperators.class.getName());

    public ReplaceWithSpecificOperators(int minOptimizationLevel) {
        super(minOptimizationLevel);
    }

    public boolean rewritePost(ExpressionHandle exprHandle) {
        Expression expr = exprHandle.get();
        if (expr.getTag() == ExprTag.FUNCTION) {
            FunctionCallExpression fce = (FunctionCallExpression) expr;
            if (BuiltinOperators.IDIV_QNAME.equals(fce.getFunction().getName())) {
                ExpressionHandle arg1 = fce.getArguments().get(0);
                ExpressionHandle arg2 = fce.getArguments().get(1);

                XQType inType1 = arg1.accept(ExpressionUtils.createTypeInferringVisitor());
                XQType inType2 = arg2.accept(ExpressionUtils.createTypeInferringVisitor());

                Quantifier inQ1 = TypeOperations.quantifier(inType1);
                Quantifier inQ2 = TypeOperations.quantifier(inType2);

                if (inQ1 == Quantifier.QUANT_ONE && inQ2 == Quantifier.QUANT_ONE) {
                    fce.setFunction(BuiltinOperators.NUMERIC_INTEGER_DIVIDE);
                    return true;
                }
            }
        }

        return false;
    }
}