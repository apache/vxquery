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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.functions.BuiltinOperators;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for unnest followed by subplan with the root operator an
 * aggregate and removes the aggregate subplan.
 * 
 * <pre>
 * Before
 * 
 *   %PARENT_PLAN
 *   UNNEST( $v2 : iterate( $v1 ) )
 *   SUBPLAN{
 *     AGGREGATE( $v1 : sequence( %expression ) )
 *     %NESTED_PLAN
 *     NESTEDTUPLESOURCE
 *   }
 *   %CHILD_PLAN
 *   
 *   where %PARENT_PLAN does not use $v1.
 *    
 * After 
 * 
 *   %PARENT_PLAN
 *   UNNEST( $v2 : iterate( $v3 ) )
 *   ASSIGN( $v3 : %expression )
 *   %NESTED_PLAN
 *   %CHILD_PLAN
 * </pre>
 */
public class EliminateUnnestAggregateSubplanRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;

        // Check to see if the expression is the iterate operator.
        ILogicalExpression logicalExpression = (ILogicalExpression) unnest.getExpressionRef().getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        if (!functionCall.getFunctionIdentifier().equals(BuiltinOperators.ITERATE.getFunctionIdentifier())) {
            return false;
        }

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op2;

        AbstractLogicalOperator subplanOp = (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots().get(0)
                .getValue();
        if (subplanOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) subplanOp;

        // Check to see if the expression is a function and op:sequence.
        ILogicalExpression logicalExpression2 = (ILogicalExpression) aggregate.getExpressions().get(0).getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall2 = (AbstractFunctionCallExpression) logicalExpression2;
        if (!functionCall2.getFunctionIdentifier().equals(BuiltinOperators.SEQUENCE.getFunctionIdentifier())) {
            return false;
        }

        // Make inline the arguments for the subplan.
        AbstractLogicalOperator subplanEnd = OperatorToolbox.findLastSubplanOperator(subplanOp);
        int count = 0;
        for (Mutable<ILogicalOperator> input : subplan.getInputs()) {
            subplanEnd.getInputs().get(count++).setValue(input.getValue());
        }

        // Replace search string with assign.
        Mutable<ILogicalExpression> assignExpression = functionCall2.getArguments().get(0);
        LogicalVariable assignVariable = context.newVar();
        AssignOperator aOp = new AssignOperator(assignVariable, assignExpression);
        for (Mutable<ILogicalOperator> input : aggregate.getInputs()) {
            aOp.getInputs().add(input);
        }
        functionCall.getArguments().get(0).setValue(new VariableReferenceExpression(assignVariable));
        unnest.getInputs().get(0).setValue(aOp);
        context.computeAndSetTypeEnvironmentForOperator(aOp);
        context.computeAndSetTypeEnvironmentForOperator(unnest);
        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
