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
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.functions.BuiltinOperators;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

/**
 * The rule searches for assign operators that have a xquery aggregate function.
 * The aggregate function will be more efficient when used by an aggregate
 * operator.
 * XQuery aggregate functions are implemented in both scalar (one XDM Instance
 * input as a sequence) and iterative (a stream of XDM Instances each one is
 * single object).
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   ASSIGN( $v2 : sf1( $v0 ) )
 *   plan__child
 *   
 *   Where sf1 is a XQuery aggregate function expression (count, max, min, 
 *   average, sum) with any supporting functions like treat, promote or data. 
 *   The variable $v0 is defined in plan__child.
 *   
 * After
 * 
 *   plan__parent
 *   SUBPLAN{
 *     AGGREGATE( $v2 : af1( $v1 ) )
 *     UNNEST( $v1 : iterate( $v0 ) )
 *     NESTEDTUPLESOURCE
 *   }
 *   plan__child
 * </pre>
 * 
 * @author prestonc
 */
public class ConvertAssignToAggregateRule extends AbstractVXQueryAggregateRule {
    /**
     * Find where an assign for a aggregate function is used before aggregate operator for a sequence.
     * Search pattern 1: assign [function-call: count(function-call: treat($$))]
     * Search pattern 2: $$ for aggregate [function-call: sequence()]
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        IFunctionInfo aggregateInfo;
        AbstractFunctionCallExpression finalFunctionCall;
        Mutable<ILogicalOperator> nextOperatorRef;

        // Check if assign is for aggregate function.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;

        Mutable<ILogicalExpression> mutableLogicalExpression = assign.getExpressions().get(0);
        ILogicalExpression logicalExpression = mutableLogicalExpression.getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        // TODO get the function through the function definition
        aggregateInfo = getAggregateFunction(functionCall);
        if (aggregateInfo == null) {
            return false;
        }
        Mutable<ILogicalExpression> mutableVariableExpresion = ExpressionToolbox.findVariableExpression(mutableLogicalExpression);
        if (mutableVariableExpresion == null) {
            return false;
        }
        Mutable<ILogicalExpression> finalFunctionCallM = ExpressionToolbox
                .findLastFunctionExpression(mutableLogicalExpression);
        finalFunctionCall = (AbstractFunctionCallExpression) finalFunctionCallM.getValue();

        
        // Build a subplan for replacing the sort distinct function with operators.
        // Nested tuple source.
        Mutable<ILogicalOperator> inputOperator = getInputOperator(assign.getInputs().get(0));
        NestedTupleSourceOperator ntsOperator = new NestedTupleSourceOperator(inputOperator);
        nextOperatorRef = new MutableObject<ILogicalOperator>(ntsOperator);

        // Get variable that is being used for sort and distinct operators.
        VariableReferenceExpression inputVariableRef = (VariableReferenceExpression) mutableVariableExpresion.getValue();
        LogicalVariable inputVariable = inputVariableRef.getVariableReference();

        // Unnest.
        LogicalVariable unnestVariable = context.newVar();
        UnnestOperator unnestOperator = getUnnestOperator(inputVariable, unnestVariable);
        unnestOperator.getInputs().add(nextOperatorRef);
        nextOperatorRef = new MutableObject<ILogicalOperator>(unnestOperator);

        // Aggregate.
        VariableReferenceExpression inputArg = new VariableReferenceExpression(unnestVariable);
        finalFunctionCall.getArguments().get(0).setValue(inputArg);
        Mutable<ILogicalExpression> aggregateArgs = functionCall.getArguments().get(0);

        LogicalVariable aggregateVariable = assign.getVariables().get(0);
        AggregateOperator aggregateOperator = getAggregateOperator(aggregateInfo, aggregateArgs, aggregateVariable);
        aggregateOperator.getInputs().add(nextOperatorRef);
        nextOperatorRef = new MutableObject<ILogicalOperator>(aggregateOperator);

        // Subplan.
        SubplanOperator subplanOperator = new SubplanOperator();
        subplanOperator.getInputs().add(assign.getInputs().get(0));
        subplanOperator.setRootOp(nextOperatorRef);

        assign.getInputs().clear();
        opRef.setValue(subplanOperator);

        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    private AggregateOperator getAggregateOperator(IFunctionInfo aggregateFunction,
            Mutable<ILogicalExpression> aggregateArgs, LogicalVariable aggregateVariable) {
        List<LogicalVariable> aggregateVariables = new ArrayList<LogicalVariable>();
        aggregateVariables.add(aggregateVariable);

        List<Mutable<ILogicalExpression>> aggregateSequenceArgs = new ArrayList<Mutable<ILogicalExpression>>();
        aggregateSequenceArgs.add(aggregateArgs);

        List<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
        ILogicalExpression aggregateExp = new AggregateFunctionCallExpression(aggregateFunction, false,
                aggregateSequenceArgs);
        Mutable<ILogicalExpression> aggregateExpRef = new MutableObject<ILogicalExpression>(aggregateExp);
        exprs.add(aggregateExpRef);

        return new AggregateOperator(aggregateVariables, exprs);
    }

    private Mutable<ILogicalOperator> getInputOperator(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        switch (op.getOperatorTag()) {
            case SUBPLAN:
                SubplanOperator subplan = (SubplanOperator) op;
                return getInputOperator(subplan.getNestedPlans().get(0).getRoots().get(0));
            case NESTEDTUPLESOURCE:
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
                return getInputOperator(nts.getDataSourceReference());
            default:
                return opRef;
        }
    }

    private UnnestOperator getUnnestOperator(LogicalVariable inputVariable, LogicalVariable unnestVariable) {
        VariableReferenceExpression inputVre = new VariableReferenceExpression(inputVariable);

        List<Mutable<ILogicalExpression>> iterateArgs = new ArrayList<Mutable<ILogicalExpression>>();
        iterateArgs.add(new MutableObject<ILogicalExpression>(inputVre));

        ILogicalExpression unnestExp = new UnnestingFunctionCallExpression(BuiltinOperators.ITERATE, iterateArgs);
        Mutable<ILogicalExpression> unnestExpRef = new MutableObject<ILogicalExpression>(unnestExp);

        return new UnnestOperator(unnestVariable, unnestExpRef);
    }

}
