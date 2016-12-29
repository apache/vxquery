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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for aggregate operators with an aggregate function
 * expression that has not been initialized for two step aggregation.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   AGGREGATE( $v : af1( $v1 ) )
 *   plan__child
 *   
 *   Where af1 is a VXquery aggregate function expression configured for single 
 *   step processing and $v1 is defined in plan__child.
 *   
 * After
 * 
 *   if (af1 == count) aggregate operating settings:
 *     Step 1: count
 *     Step 2: sum
 *   if (af1 == avg) aggregate operating settings:
 *     Step 1: avg-local
 *     Step 2: avg-global
 *   if (af1 in (max, min, sum)) aggregate operating settings:
 *     Step 1: af1
 *     Step 2: af1
 * </pre>
 * 
 * @author prestonc
 */
public class IntroduceTwoStepAggregateRule implements IAlgebraicRewriteRule {
    final Map<FunctionIdentifier, Pair<IFunctionInfo, IFunctionInfo>> AGGREGATE_MAP = new HashMap<FunctionIdentifier, Pair<IFunctionInfo, IFunctionInfo>>();

    public IntroduceTwoStepAggregateRule() {
        AGGREGATE_MAP.put(BuiltinFunctions.FN_AVG_1.getFunctionIdentifier(),
                new Pair<IFunctionInfo, IFunctionInfo>(BuiltinOperators.AVG_LOCAL, BuiltinOperators.AVG_GLOBAL));
        AGGREGATE_MAP.put(BuiltinFunctions.FN_COUNT_1.getFunctionIdentifier(),
                new Pair<IFunctionInfo, IFunctionInfo>(BuiltinFunctions.FN_COUNT_1, BuiltinFunctions.FN_SUM_1));
        AGGREGATE_MAP.put(BuiltinFunctions.FN_MAX_1.getFunctionIdentifier(),
                new Pair<IFunctionInfo, IFunctionInfo>(BuiltinFunctions.FN_MAX_1, BuiltinFunctions.FN_MAX_1));
        AGGREGATE_MAP.put(BuiltinFunctions.FN_MIN_1.getFunctionIdentifier(),
                new Pair<IFunctionInfo, IFunctionInfo>(BuiltinFunctions.FN_MIN_1, BuiltinFunctions.FN_MIN_1));
        AGGREGATE_MAP.put(BuiltinFunctions.FN_SUM_1.getFunctionIdentifier(),
                new Pair<IFunctionInfo, IFunctionInfo>(BuiltinFunctions.FN_SUM_1, BuiltinFunctions.FN_SUM_1));
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // Check if aggregate function.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) op;
        if (aggregate.getExpressions().size() == 0) {
            return false;
        }
        Mutable<ILogicalExpression> mutableLogicalExpression = aggregate.getExpressions().get(0);
        ILogicalExpression logicalExpression = mutableLogicalExpression.getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;

        if (AGGREGATE_MAP.containsKey(functionCall.getFunctionIdentifier())) {
            AggregateFunctionCallExpression aggregateFunctionCall = (AggregateFunctionCallExpression) functionCall;
            if (aggregateFunctionCall.isTwoStep()) {
                return false;
            }
            aggregateFunctionCall.setTwoStep(true);
            aggregateFunctionCall.setStepOneAggregate(AGGREGATE_MAP.get(functionCall.getFunctionIdentifier()).first);
            aggregateFunctionCall.setStepTwoAggregate(AGGREGATE_MAP.get(functionCall.getFunctionIdentifier()).second);
            return true;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
