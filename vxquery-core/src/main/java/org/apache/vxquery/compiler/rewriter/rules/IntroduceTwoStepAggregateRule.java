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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceTwoStepAggregateRule implements IAlgebraicRewriteRule {
    /**
     * Find where an aggregate functions can be switched to two step.
     * Search pattern: aggregate [function-call: (count, min, max, sum)]
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // Check if aggregate function.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) op;

        Mutable<ILogicalExpression> mutableLogicalExpression = aggregate.getExpressions().get(0);
        ILogicalExpression logicalExpression = mutableLogicalExpression.getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;

        if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_COUNT_1.getFunctionIdentifier())) {
            AggregateFunctionCallExpression aggregateFunctionCall = (AggregateFunctionCallExpression) functionCall;
            if (aggregateFunctionCall.isTwoStep()) {
                return false;
            }
            aggregateFunctionCall.setTwoStep(true);
            aggregateFunctionCall.setStepOneAggregate(BuiltinFunctions.FN_COUNT_1);
            aggregateFunctionCall.setStepTwoAggregate(BuiltinFunctions.FN_SUM_1);
            return true;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_MAX_1.getFunctionIdentifier())) {
            AggregateFunctionCallExpression aggregateFunctionCall = (AggregateFunctionCallExpression) functionCall;
            if (aggregateFunctionCall.isTwoStep()) {
                return false;
            }
            aggregateFunctionCall.setTwoStep(true);
            aggregateFunctionCall.setStepOneAggregate(BuiltinFunctions.FN_MAX_1);
            aggregateFunctionCall.setStepTwoAggregate(BuiltinFunctions.FN_MAX_1);
            return true;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_MIN_1.getFunctionIdentifier())) {
            AggregateFunctionCallExpression aggregateFunctionCall = (AggregateFunctionCallExpression) functionCall;
            if (aggregateFunctionCall.isTwoStep()) {
                return false;
            }
            aggregateFunctionCall.setTwoStep(true);
            aggregateFunctionCall.setStepOneAggregate(BuiltinFunctions.FN_MIN_1);
            aggregateFunctionCall.setStepTwoAggregate(BuiltinFunctions.FN_MIN_1);
            return true;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_SUM_1.getFunctionIdentifier())) {
            AggregateFunctionCallExpression aggregateFunctionCall = (AggregateFunctionCallExpression) functionCall;
            if (aggregateFunctionCall.isTwoStep()) {
                return false;
            }
            aggregateFunctionCall.setTwoStep(true);
            aggregateFunctionCall.setStepOneAggregate(BuiltinFunctions.FN_SUM_1);
            aggregateFunctionCall.setStepTwoAggregate(BuiltinFunctions.FN_SUM_1);
            return true;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
