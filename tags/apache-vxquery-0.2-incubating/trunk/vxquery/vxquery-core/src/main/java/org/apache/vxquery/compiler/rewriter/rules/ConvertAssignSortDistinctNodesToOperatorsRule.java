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
import org.apache.vxquery.functions.BuiltinOperators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConvertAssignSortDistinctNodesToOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    /**
     * Find where a sort distinct nodes is being used and not required based on input parameters.
     * Search pattern: assign [function-call: sort-distinct-nodes-asc-or-atomics]
     */
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        Mutable<ILogicalOperator> nextOperatorRef;

        // Check if assign is for sort-distinct-nodes-asc-or-atomics.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;

        // Check to see if the expression is a function and
        // sort-distinct-nodes-asc-or-atomics.
        ILogicalExpression logicalExpression = (ILogicalExpression) assign.getExpressions().get(0).getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        if (!functionCall.getFunctionIdentifier().equals(
                BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())
                && !functionCall.getFunctionIdentifier().equals(
                        BuiltinOperators.DISTINCT_NODES_OR_ATOMICS.getFunctionIdentifier())
                && !functionCall.getFunctionIdentifier().equals(
                        BuiltinOperators.SORT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())) {
            return false;
        }

        // Build a subplan for replacing the sort distinct function with operators.
        // Nested tuple source.
        Mutable<ILogicalOperator> inputOperator = getInputOperator(assign.getInputs().get(0));
        NestedTupleSourceOperator ntsOperator = new NestedTupleSourceOperator(inputOperator);
        nextOperatorRef = new MutableObject<ILogicalOperator>(ntsOperator);

        // Get variable that is being used for sort and distinct operators.
        VariableReferenceExpression inputVariableRef = (VariableReferenceExpression) functionCall.getArguments().get(0)
                .getValue();
        LogicalVariable inputVariable = inputVariableRef.getVariableReference();

        // Unnest.
        LogicalVariable unnestVariable = context.newVar();
        UnnestOperator unnestOperator = getUnnestOperator(inputVariable, unnestVariable);
        unnestOperator.getInputs().add(nextOperatorRef);
        nextOperatorRef = new MutableObject<ILogicalOperator>(unnestOperator);

        // Assign Node ID key.
        LogicalVariable nodeIdKeyVariable = context.newVar();
        AssignOperator nodeIdAssignOp = getAssignOperator(unnestVariable, nodeIdKeyVariable,
                BuiltinOperators.ID_FROM_NODE);
        nodeIdAssignOp.getInputs().add(nextOperatorRef);
        nextOperatorRef = new MutableObject<ILogicalOperator>(nodeIdAssignOp);

        // Prepare for Order and Distinct.
        Mutable<ILogicalExpression> nodeIdKeyVariableRef = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(nodeIdKeyVariable));

        // Distinct.
        if (functionCall.getFunctionIdentifier().equals(
                BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())
                || functionCall.getFunctionIdentifier().equals(
                        BuiltinOperators.DISTINCT_NODES_OR_ATOMICS.getFunctionIdentifier())) {
            LogicalVariable groupByVariable = context.newVar();
            ILogicalExpression nodeIdKeyVre = new VariableReferenceExpression(nodeIdKeyVariable);
            GroupByOperator groupByOperator = getGroupByOperator(groupByVariable, nodeIdKeyVre);
            groupByOperator.getInputs().add(nextOperatorRef);
            nextOperatorRef = new MutableObject<ILogicalOperator>(groupByOperator);
        }

//        // Order.
//        if (functionCall.getFunctionIdentifier().equals(
//                BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())
//                || functionCall.getFunctionIdentifier().equals(
//                        BuiltinOperators.SORT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())) {
//            OrderOperator orderOperator = getOrderOperator(nodeIdKeyVariableRef);
//            orderOperator.getInputs().add(nextOperatorRef);
//            nextOperatorRef = new MutableObject<ILogicalOperator>(orderOperator);
//        }
//
        // Aggregate.
        LogicalVariable aggregateVariable = assign.getVariables().get(0);
        AggregateOperator aggregateOperator = getAggregateOperator(unnestVariable, aggregateVariable);
        aggregateOperator.getInputs().add(nextOperatorRef);
        nextOperatorRef = new MutableObject<ILogicalOperator>(aggregateOperator);

        // Subplan.
        SubplanOperator subplanOperator = new SubplanOperator();
        subplanOperator.getInputs().add(assign.getInputs().get(0));
        subplanOperator.setRootOp(nextOperatorRef);

        opRef.setValue(subplanOperator);

        return true;
    }

    private AggregateOperator getAggregateOperator(LogicalVariable unnestVariable, LogicalVariable aggregateVariable) {
        List<LogicalVariable> aggregateVariables = new ArrayList<LogicalVariable>();
        aggregateVariables.add(aggregateVariable);

        List<Mutable<ILogicalExpression>> aggregateSequenceArgs = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalExpression> unnestVariableRef = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(unnestVariable));
        aggregateSequenceArgs.add(unnestVariableRef);
        
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
        ILogicalExpression aggregateExp = new AggregateFunctionCallExpression(BuiltinOperators.SEQUENCE, false,
                aggregateSequenceArgs);
        Mutable<ILogicalExpression> aggregateExpRef = new MutableObject<ILogicalExpression>(aggregateExp);
        exprs.add(aggregateExpRef);

        return new AggregateOperator(aggregateVariables, exprs);
    }

    private AssignOperator getAssignOperator(LogicalVariable unnestVariable, LogicalVariable outputVariable,
            IFunctionInfo inputFunction) {
        List<Mutable<ILogicalExpression>> nodeArgs = new ArrayList<Mutable<ILogicalExpression>>();
        nodeArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(unnestVariable)));
        ScalarFunctionCallExpression unctionExpression = new ScalarFunctionCallExpression(inputFunction, nodeArgs);
        Mutable<ILogicalExpression> nodeTreeIdExpression = new MutableObject<ILogicalExpression>(unctionExpression);
        return new AssignOperator(outputVariable, nodeTreeIdExpression);
    }

    private GroupByOperator getGroupByOperator(LogicalVariable outputVariable, ILogicalExpression variableRef) {
        GroupByOperator op = new GroupByOperator();
        op.addGbyExpression(outputVariable, variableRef);
        return op;
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

    private OrderOperator getOrderOperator(Mutable<ILogicalExpression> variableRef) {
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderArgs = new ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>>();
        orderArgs.add(new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, variableRef));
        OrderOperator oo = new OrderOperator(orderArgs);
//        oo.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        return oo;
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
