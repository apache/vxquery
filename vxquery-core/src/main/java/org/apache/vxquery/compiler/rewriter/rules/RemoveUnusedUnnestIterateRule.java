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
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodes;
import org.apache.vxquery.functions.BuiltinOperators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

/**
 * The rule searches for an unnest operator (1) immediately following an unnest
 * or data scan operator (2). If the variable is only used in unnest (1) then
 * unnest (1) can be removed.
 * 
 * <pre>
 * Before 
 * 
 *   plan__parent
 *   UNNEST( $v2 : iterate( $v1 ) )
 *   UNNEST( $v1 : $v )
 *   plan__child
 *   
 *   Where $v1 is not used in plan__parent.
 *   
 * After
 * 
 *   plan__parent
 *   ASSIGN( $v2 : $v1 )
 *   UNNEST( $v1 : $v )
 *   plan__child
 * </pre>
 * 
 * @author prestonc
 */
public class RemoveUnusedUnnestIterateRule extends AbstractUsedVariablesProcessingRule {

    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;

        // Check to see if the expression is a function and iterate.
        ILogicalExpression logicalExpressionUnnest1 = (ILogicalExpression) unnest.getExpressionRef().getValue();
        if (logicalExpressionUnnest1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCallUnnest1 = (AbstractFunctionCallExpression) logicalExpressionUnnest1;
        if (!functionCallUnnest1.getFunctionIdentifier().equals(BuiltinOperators.ITERATE.getFunctionIdentifier())) {
            return false;
        }

        // Check to see if the expression is a variable.
        Mutable<ILogicalExpression> logicalExpressionUnnestRef2 = functionCallUnnest1.getArguments().get(0);
        ILogicalExpression logicalExpressionUnnest2 = (ILogicalExpression) logicalExpressionUnnestRef2.getValue();
        if (logicalExpressionUnnest2.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression vre2 = (VariableReferenceExpression) logicalExpressionUnnest2;
        LogicalVariable unnestInput = vre2.getVariableReference();

        // Check if the input is an DATASCAN or UNNEST operator..
        Mutable<ILogicalOperator> opRef2 = unnest.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() == LogicalOperatorTag.UNNEST
                || op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            AbstractScanOperator aso = (AbstractScanOperator) op2;
            if (aso.getVariables().size() == 1 && aso.getVariables().contains(unnestInput)) {
                // Add in a noop.
                AssignOperator assign = new AssignOperator(unnest.getVariable(), logicalExpressionUnnestRef2);
                assign.getInputs().addAll(unnest.getInputs());
                opRef.setValue(assign);
                return true;
            }
        }
        return false;
    }
}
