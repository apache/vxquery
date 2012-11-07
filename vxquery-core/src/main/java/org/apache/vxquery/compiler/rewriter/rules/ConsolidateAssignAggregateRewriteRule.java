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
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConsolidateAssignAggregateRewriteRule implements IAlgebraicRewriteRule {
    /**
     * Find where an assign for a aggregate function is used before aggregate operator for a sequence. .
     * Search pattern 1: assign [function-call: count(function-call: treat($$))]
     * Search pattern 2: $$ for aggregate [function-call: sequence()]
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // Check if assign is for sort-distinct-nodes-asc-or-atomics.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;

        // Check to see if the expression is a function and sort-distinct-nodes-asc-or-atomics.
        ILogicalExpression logicalExpression = (ILogicalExpression) assign.getExpressions().get(0).getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        if (!functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_COUNT_1.getFunctionIdentifier())) {
            return false;
        }
        
        // If the first argument is treat, allow it to pass.
        ILogicalExpression logicalExpression2 = (ILogicalExpression) functionCall.getArguments().get(0).getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall2 = (AbstractFunctionCallExpression) logicalExpression2;
        if (!functionCall2.getFunctionIdentifier().equals(BuiltinOperators.TREAT.getFunctionIdentifier())) {
            return false;
        }

        // find variable id for argument to count.
        ILogicalExpression logicalExpression3 = (ILogicalExpression) functionCall2.getArguments().get(0).getValue();
        if (logicalExpression3.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression variableExpression = (VariableReferenceExpression) logicalExpression3;
        int variableId = variableExpression.getVariableReference().getId();
        
        
        // Search for variable see if it is a aggregate sequence.
        AbstractLogicalOperator opSearch = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        opSearch = findSequenceAggregateOperator(opSearch, variableId);
        if (opSearch == null) {
            return false;
        }
        
        AggregateOperator aggregate = (AggregateOperator) opSearch;
        
        // Check to see if the expression is a function and sort-distinct-nodes-asc-or-atomics.
        ILogicalExpression logicalExpressionSearch = (ILogicalExpression) aggregate.getExpressions().get(0).getValue();
        if (logicalExpressionSearch.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCallSearch = (AbstractFunctionCallExpression) logicalExpressionSearch;
        if (!functionCallSearch.getFunctionIdentifier().equals(BuiltinOperators.SEQUENCE.getFunctionIdentifier())) {
            return false;
        }
        
        // Set the aggregate function to use count.
        functionCallSearch.setFunctionInfo(BuiltinFunctions.FN_COUNT_1);

        // Remove the aggregate assign.
        assign.getExpressions().set(0, functionCall2.getArguments().get(0));
        return true;
    }

    private AbstractLogicalOperator findSequenceAggregateOperator(AbstractLogicalOperator opSearch, int variableId) {
        while (opSearch.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (opSearch.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                // Check for variable assignment and sequence.
                AggregateOperator aggregate = (AggregateOperator) opSearch;

                // Check to see if the expression is a function and sort-distinct-nodes-asc-or-atomics.
                ILogicalExpression logicalExpressionSearch = (ILogicalExpression) aggregate.getExpressions().get(0).getValue();
                if (logicalExpressionSearch.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    opSearch = (AbstractLogicalOperator) opSearch.getInputs().get(0).getValue();
                    continue;
                }
                AbstractFunctionCallExpression functionCallSearch = (AbstractFunctionCallExpression) logicalExpressionSearch;
                if (!functionCallSearch.getFunctionIdentifier().equals(BuiltinOperators.SEQUENCE.getFunctionIdentifier())) {
                    opSearch = (AbstractLogicalOperator) opSearch.getInputs().get(0).getValue();
                    continue;
                }
                
                // TODO search for variable ID.
                
                // Found the aggregate operator!!!
                return opSearch;
            } else if (opSearch.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                // Run through subplan.
                SubplanOperator subplan = (SubplanOperator) opSearch;
                AbstractLogicalOperator opSubplan = (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots().get(0).getValue();
                AbstractLogicalOperator search = findSequenceAggregateOperator(opSubplan, variableId);
                if (search != null) {
                    return search;
                }
            }
            opSearch = (AbstractLogicalOperator) opSearch.getInputs().get(0).getValue();
        }
        if (opSearch.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return null;
        }
        return opSearch;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
