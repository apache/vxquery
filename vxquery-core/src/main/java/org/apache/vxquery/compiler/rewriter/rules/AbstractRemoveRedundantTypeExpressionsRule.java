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
package org.apache.vxquery.compiler.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractRemoveRedundantTypeExpressionsRule implements IAlgebraicRewriteRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;
    final List<Mutable<ILogicalExpression>> functionList = new ArrayList<Mutable<ILogicalExpression>>();
    
    protected abstract FunctionIdentifier getSearchFunction();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = false;
        List<Mutable<ILogicalExpression>> expressions = OperatorToolbox.getExpressions(opRef);
        for (Mutable<ILogicalExpression> expression : expressions) {
            if (processTypeExpression(opRef, expression)) {
                modified = true;
            }
        }
        return modified;
    }

    private boolean processTypeExpression(Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> search) {
        boolean modified = false;
        SequenceType inputSequenceType;
        SequenceType sTypeArg;
        functionList.clear();
        ExpressionToolbox.findAllFunctionExpressions(search, getSearchFunction(), functionList);
        for (Mutable<ILogicalExpression> searchM : functionList) {
            // Get input function
            AbstractFunctionCallExpression searchFunction = (AbstractFunctionCallExpression) searchM.getValue();
            Mutable<ILogicalExpression> argFirstM = searchFunction.getArguments().get(ARG_DATA);

            // Find the input return type.
            inputSequenceType = ExpressionToolbox.getOutputSequenceType(opRef, argFirstM, dCtx);

            // Find the argument type.
            sTypeArg = null;
            if (hasTypeArgument()) {
                sTypeArg = ExpressionToolbox.getTypeExpressionTypeArgument(searchM, dCtx);
            }

            // remove
            if (matchesAllInstancesOf(sTypeArg, inputSequenceType)) {
                searchM.setValue(argFirstM.getValue());
                modified = true;
            }
        }
        return modified;
    }

    public abstract boolean matchesAllInstancesOf(SequenceType sTypeArg, SequenceType sTypeOutput);

    public boolean hasTypeArgument() {
        return true;
    }

}
