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
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

/**
 * The rule searches for an unnest operator (1) immediately following an unnest
 * operator (2). XQuery unnest operator (1) must have a scalar implementation
 * of the unnest function. If so the two unnest expressions can be merged
 * together.
 *
 * <pre>
 * Before
 * 
 *   plan__parent
 *   UNNEST( $v2 : uf2( $v1 ) )
 *   UNNEST( $v1 : uf1( $v0 ) )
 *   plan__child
 * 
 *   Where $v1 is not used in plan__parent and uf1 is not a descendant expression.
 * 
 * After
 * 
 *   plan__parent
 *   UNNEST( $v2 : uf2( sf1( $v0 ) ) )
 *   plan__child
 * 
 *   uf1 becomes sf1 since it changes from unnesting to scalar expression.
 * </pre>
 *
 * @author prestonc
 */
public class ConsolidateUnnestsRule extends AbstractUsedVariablesProcessingRule {

    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest1 = (UnnestOperator) op;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) unnest1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest2 = (UnnestOperator) op2;

        if (usedVariables.contains(unnest2.getVariable())) {
            return false;
        }

        // Check to see if the unnest2 expression has a scalar implementation.
        ILogicalExpression logicalExpression2 = (ILogicalExpression) unnest2.getExpressionRef().getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall2 = (AbstractFunctionCallExpression) logicalExpression2;
        Function functionInfo2 = (Function) functionCall2.getFunctionInfo();
        if (!functionInfo2.hasScalarEvaluatorFactory()) {
            return false;
        }
        // Exception for specific path expressions.
        if (functionCall2.getFunctionIdentifier().equals(BuiltinOperators.DESCENDANT.getFunctionIdentifier())
                || functionCall2.getFunctionIdentifier().equals(
                        BuiltinOperators.DESCENDANT_OR_SELF.getFunctionIdentifier())) {
            return false;
        }

        // Find unnest2 variable in unnest1
        Mutable<ILogicalExpression> unnest1Arg = ExpressionToolbox.findVariableExpression(unnest1.getExpressionRef(),
                unnest2.getVariable());
        if (unnest1Arg == null) {
            return false;
        }

        // Replace unnest2 expression in unnest1
        ScalarFunctionCallExpression child = new ScalarFunctionCallExpression(functionInfo2,
                functionCall2.getArguments());
        unnest1Arg.setValue(child);

        // Move input for unnest2 into unnest1
        unnest1.getInputs().clear();
        unnest1.getInputs().addAll(unnest2.getInputs());
        return true;
    }
}
