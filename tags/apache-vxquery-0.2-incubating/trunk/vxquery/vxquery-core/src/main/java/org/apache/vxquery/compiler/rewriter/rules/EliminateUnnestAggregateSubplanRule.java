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
import org.apache.vxquery.functions.BuiltinOperators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for unnest followed by subplan with the root operator an
 * aggregate and removes the aggregate subplan.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   UNNEST( $v2 : iterate( $v1 ) )
 *   SUBPLAN{
 *     AGGREGATE( $v1 : sequence( $v0 ) )
 *     plan__nested
 *     NESTEDTUPLESOURCE
 *   }
 *   plan__child
 *   
 *   where plan__parent does not use $v1 and $v0 is defined plan__child.
 *    
 * After 
 * 
 *   plan__parent
 *   UNNEST( $v2 : iterate( $v1 ) )
 *   ASSIGN( $v1 : $v0 )
 *   plan__nested
 *   plan__child
 * </pre>
 */
public class EliminateUnnestAggregateSubplanRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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
        AbstractLogicalOperator subplanEnd = findLastSubplanOperator(subplanOp);
        int count = 0;
        for (Mutable<ILogicalOperator> input : subplan.getInputs()) {
            subplanEnd.getInputs().get(count++).setValue(input.getValue());
        }

        // Replace search string with assign.
        Mutable<ILogicalExpression> assignExpression = functionCall2.getArguments().get(0);
        AssignOperator aOp = new AssignOperator(aggregate.getVariables().get(0), assignExpression);
        for (Mutable<ILogicalOperator> input : aggregate.getInputs()) {
            aOp.getInputs().add(input);
        }
        unnest.getInputs().get(0).setValue(aOp);

        return true;
    }

    private ILogicalExpression findLastFunctionExpression(ILogicalExpression expression) {
        if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expression;
            ILogicalExpression nextExpression = functionCall.getArguments().get(0).getValue();
            if (nextExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return expression;
            }
            return findLastFunctionExpression(nextExpression);
        }
        return null;
    }

    private AbstractLogicalOperator findLastSubplanOperator(AbstractLogicalOperator op) {
        AbstractLogicalOperator next;
        while (op.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            next = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            if (next.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                break;
            }
        }
        return op;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
