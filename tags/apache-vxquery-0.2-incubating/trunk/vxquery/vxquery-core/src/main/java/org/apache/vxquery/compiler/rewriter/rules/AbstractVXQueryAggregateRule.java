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

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractVXQueryAggregateRule implements IAlgebraicRewriteRule {

    protected AbstractFunctionCallExpression getAggregateLastFunctionCall(IFunctionInfo aggregateInfo,
            AbstractFunctionCallExpression functionCall) {
        if (aggregateInfo.equals(BuiltinFunctions.FN_COUNT_1)) {
            // Argument for count is "item()*"
            // If the first argument is treat, allow it to pass.
            ILogicalExpression logicalExpression2 = (ILogicalExpression) functionCall.getArguments().get(0).getValue();
            if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            AbstractFunctionCallExpression functionCall2 = (AbstractFunctionCallExpression) logicalExpression2;
            if (!functionCall2.getFunctionIdentifier().equals(BuiltinOperators.TREAT.getFunctionIdentifier())) {
                return null;
            }

            // find variable id for argument to count.
            Mutable<ILogicalExpression> mutableLogicalExpression3 = functionCall2.getArguments().get(0);
            ILogicalExpression logicalExpression3 = (ILogicalExpression) mutableLogicalExpression3.getValue();
            if (logicalExpression3.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return null;
            }
            return functionCall2;
        } else {
            // Argument for these aggregate is "xs:anyAtomicType*"
            // If the first argument is promote followed by data, allow it to pass.
            ILogicalExpression logicalExpression2 = (ILogicalExpression) functionCall.getArguments().get(0).getValue();
            if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            AbstractFunctionCallExpression functionCall2 = (AbstractFunctionCallExpression) logicalExpression2;
            if (!functionCall2.getFunctionIdentifier().equals(BuiltinOperators.PROMOTE.getFunctionIdentifier())) {
                return null;
            }

            // If the first argument is promote followed by data, allow it to pass.
            ILogicalExpression logicalExpression3 = (ILogicalExpression) functionCall2.getArguments().get(0).getValue();
            if (logicalExpression3.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            AbstractFunctionCallExpression functionCall3 = (AbstractFunctionCallExpression) logicalExpression3;
            if (!functionCall3.getFunctionIdentifier().equals(BuiltinFunctions.FN_DATA_1.getFunctionIdentifier())) {
                return null;
            }

            // find variable id for argument to the aggregate function.
            Mutable<ILogicalExpression> mutableLogicalExpression4 = functionCall3.getArguments().get(0);
            ILogicalExpression logicalExpression4 = (ILogicalExpression) mutableLogicalExpression4.getValue();
            if (logicalExpression4.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return null;
            }
            return functionCall3;
        }
    }

    protected IFunctionInfo getAggregateFunction(AbstractFunctionCallExpression functionCall) {
        if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_COUNT_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_COUNT_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_AVG_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_AVG_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_MIN_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_MIN_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_MAX_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_MAX_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_SUM_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_SUM_1;
        } else {
            return null;
        }
    }
}
