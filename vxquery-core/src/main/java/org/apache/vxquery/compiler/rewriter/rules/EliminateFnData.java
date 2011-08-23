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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.ExpressionPrinter;
import org.apache.vxquery.compiler.expression.FunctionCallExpression;
import org.apache.vxquery.compiler.rewriter.framework.AbstractRewriteRule;
import org.apache.vxquery.compiler.tools.ExpressionUtils;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.TypeOperations;
import org.apache.vxquery.types.XQType;

public class EliminateFnData extends AbstractRewriteRule {
    private static final Logger LOGGER = Logger.getLogger(EliminateFnData.class.getName());

    public EliminateFnData(int minOptimizationLevel) {
        super(minOptimizationLevel);
    }

    public boolean rewritePost(ExpressionHandle exprHandle) {
        Expression expr = exprHandle.get();
        if (expr.getTag() == ExprTag.FUNCTION) {
            FunctionCallExpression fce = (FunctionCallExpression) expr;
            if (BuiltinFunctions.FN_DATA_QNAME.equals(fce.getFunction().getName())) {
                ExpressionHandle arg = fce.getArguments().get(0);
                XQType inType = arg.accept(ExpressionUtils.createTypeInferringVisitor());
                boolean dataRemovable = TypeOperations.isSubtypeOf(inType,
                        TypeOperations.quantified(BuiltinTypeRegistry.XS_ANY_ATOMIC, Quantifier.QUANT_STAR));
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Found fn:data()");
                    LOGGER.fine(ExpressionPrinter.prettyPrint(fce));
                    LOGGER.fine("Input type: " + inType);
                    LOGGER.fine("subtype(anyAtomicType*): " + dataRemovable);
                }
                if (dataRemovable) {
                    exprHandle.set(arg.get());
                    return true;
                }
            }
        }

        return false;
    }
}