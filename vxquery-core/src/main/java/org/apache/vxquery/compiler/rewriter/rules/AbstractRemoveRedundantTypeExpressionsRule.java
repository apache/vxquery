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
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractRemoveRedundantTypeExpressionsRule implements IAlgebraicRewriteRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;
    final List<Mutable<ILogicalExpression>> functionList = new ArrayList<Mutable<ILogicalExpression>>();

    protected abstract FunctionIdentifier getSearchFunction();

    protected abstract boolean getTreatFunction();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = false;
        List<Mutable<ILogicalExpression>> expressions = OperatorToolbox.getExpressions(opRef);
        for (Mutable<ILogicalExpression> expression : expressions) {
            if (processTypeExpression(opRef, expression)) {
                context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
                modified = true;
            }
        }
        return modified;
    }

    private boolean processTypeExpression(Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> search) {
        boolean modified = false;
        boolean betweenAssigns = false;
        SequenceType inputSequenceType;
        SequenceType sTypeArg;
        functionList.clear();
        List<Mutable<ILogicalExpression>> finds = new ArrayList<Mutable<ILogicalExpression>>();
        ExpressionToolbox.findAllFunctionExpressions(search, getSearchFunction(), functionList);
        if (functionList.isEmpty()
                && opRef.getValue().getInputs().get(0).getValue().getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {

            ExpressionToolbox.findAllFunctionExpressions(search, BuiltinFunctions.FN_COUNT_1.getFunctionIdentifier(),
                    functionList);
            ExpressionToolbox.findAllFunctionExpressions(search, BuiltinFunctions.FN_SUM_1.getFunctionIdentifier(),
                    functionList);
            ExpressionToolbox.findAllFunctionExpressions(search, BuiltinFunctions.FN_AVG_1.getFunctionIdentifier(),
                    functionList);
            betweenAssigns = true;

        }
        for (Mutable<ILogicalExpression> searchM : functionList) {
            // Get input function
            AbstractFunctionCallExpression searchFunction = (AbstractFunctionCallExpression) searchM.getValue();
            Mutable<ILogicalExpression> argFirstM = searchFunction.getArguments().get(ARG_DATA);
            if (betweenAssigns && getTreatFunction()) {
                VariableReferenceExpression variableRefExp = (VariableReferenceExpression) argFirstM.getValue();
                LogicalVariable variableId = variableRefExp.getVariableReference();
                Mutable<ILogicalOperator> variableProducer = OperatorToolbox.findProducerOf(opRef, variableId);
                if (variableProducer != null) {
                    AbstractLogicalOperator variableOp = (AbstractLogicalOperator) variableProducer.getValue();
                    if (variableOp.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
                        AbstractAssignOperator assign = (AbstractAssignOperator) variableOp;

                        ExpressionToolbox.findAllFunctionExpressions(assign.getExpressions().get(0),
                                getSearchFunction(), finds);
                    }
                }
            }
            // Find the input return type.
            inputSequenceType = ExpressionToolbox.getOutputSequenceType(opRef, argFirstM, dCtx);
            if (!finds.isEmpty() && betweenAssigns && getTreatFunction()) {
                Function function = ExpressionToolbox.getBuiltIn(finds.get(0));
                if (function.getFunctionIdentifier().equals(getSearchFunction())) {
                    //betweenAssigns = true;
                    searchFunction = (AbstractFunctionCallExpression) finds.get(0).getValue();
                    argFirstM = searchFunction.getArguments().get(ARG_DATA);
                }
            }
            // Find the argument type.
            sTypeArg = null;
            if (hasTypeArgument()) {
                sTypeArg = ExpressionToolbox.getTypeExpressionTypeArgument(searchM, dCtx);
            }
            if (betweenAssigns && getTreatFunction()) {
                sTypeArg = inputSequenceType;
            }

            // remove
            if (matchesAllInstancesOf(sTypeArg, inputSequenceType)) {
                if (betweenAssigns) {
                    finds.get(0).setValue(argFirstM.getValue());
                } else {
                    searchM.setValue(argFirstM.getValue());
                }
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
