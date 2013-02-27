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
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;

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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveUnusedSortDistinctNodesRule implements IAlgebraicRewriteRule {

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
        // Do not process empty or nested tuple source.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        // Initialization.
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;

        // Find the available variables.
        HashMap<Integer, DocumentOrder> documentOrderVariables = getParentDocumentOrderVariableMap(opRef.getValue(),
                vxqueryContext);
        updateVariableMap(op, documentOrderVariables, vxqueryContext);

        // Save propagated value.
        vxqueryContext.putDocumentOrderOperatorVariableMap(opRef.getValue(), documentOrderVariables);
        return false;
    }

    private void updateVariableMap(AbstractLogicalOperator op, HashMap<Integer, DocumentOrder> documentOrderVariables,
            VXQueryOptimizationContext vxqueryContext) {
        int variableId;
        DocumentOrder documentOrder;
        HashMap<Integer, DocumentOrder> documentOrderVariablesForOperator = getParentDocumentOrderVariableMap(op,
                vxqueryContext);

        // Get the DocumentOrder from propagation.
        switch (op.getOperatorTag()) {
            case AGGREGATE:
                AggregateOperator aggregate = (AggregateOperator) op;
                ILogicalExpression aggregateLogicalExpression = (ILogicalExpression) aggregate.getExpressions().get(0)
                        .getValue();
                variableId = aggregate.getVariables().get(0).getId();
                documentOrder = propagateDocumentOrder(aggregateLogicalExpression, documentOrderVariablesForOperator);
                documentOrderVariables.put(variableId, documentOrder);
                break;
            case ASSIGN:
                AssignOperator assign = (AssignOperator) op;
                ILogicalExpression assignLogicalExpression = (ILogicalExpression) assign.getExpressions().get(0)
                        .getValue();
                variableId = assign.getVariables().get(0).getId();
                documentOrder = propagateDocumentOrder(assignLogicalExpression, documentOrderVariablesForOperator);
                documentOrderVariables.put(variableId, documentOrder);
                break;
            case CLUSTER:
                break;
            case DATASOURCESCAN:
                break;
            case DIE:
                break;
            case DISTINCT:
                break;
            case EMPTYTUPLESOURCE:
                break;
            case EXCHANGE:
                break;
            case EXTENSION_OPERATOR:
                break;
            case GROUP:
                break;
            case INDEX_INSERT_DELETE:
                break;
            case INNERJOIN:
                break;
            case INSERT_DELETE:
                break;
            case LEFTOUTERJOIN:
                break;
            case LIMIT:
                break;
            case NESTEDTUPLESOURCE:
                break;
            case ORDER:
                break;
            case PARTITIONINGSPLIT:
                break;
            case PROJECT:
                break;
            case REPLICATE:
                break;
            case RUNNINGAGGREGATE:
                break;
            case SCRIPT:
                break;
            case SELECT:
                break;
            case SINK:
                break;
            case SUBPLAN:
                // Find the last operator to set a variable and call this function again.
                SubplanOperator subplan = (SubplanOperator) op;
                AbstractLogicalOperator lastOperator = (AbstractLogicalOperator) subplan.getNestedPlans().get(0)
                        .getRoots().get(0).getValue();
                updateVariableMap(lastOperator, documentOrderVariables, vxqueryContext);
                break;
            case UNIONALL:
                break;
            case UNNEST:
                // Get unnest item property.
                UnnestOperator unnest = (UnnestOperator) op;
                ILogicalExpression logicalExpression = (ILogicalExpression) unnest.getExpressionRef().getValue();
                variableId = unnest.getVariables().get(0).getId();
                documentOrder = propagateDocumentOrder(logicalExpression, documentOrderVariablesForOperator);

                // Reset properties based on unnest duplication.
                getDocumentOrderNOVariables(documentOrderVariables);
                documentOrderVariables.put(variableId, documentOrder);

                // Add position variable property.
                if (unnest.getPositionalVariable() != null) {
                    variableId = unnest.getPositionalVariable().getId();
                    documentOrderVariables.put(variableId, DocumentOrder.YES);
                }
                break;
            case UNNEST_MAP:
                break;
            case UPDATE:
                break;
            case WRITE:
                break;
            case WRITE_RESULT:
                break;
            default:
                break;
        }

    }

    /**
     * Sets all the variables to DocumentOrder.NO.
     * @param documentOrderVariables
     */
    private void getDocumentOrderNOVariables(
            HashMap<Integer, DocumentOrder> documentOrderVariables) {
        for (Entry<Integer, DocumentOrder> entry : documentOrderVariables.entrySet()) {
            documentOrderVariables.put(entry.getKey(), DocumentOrder.NO);
        }
    }

    /**
     * Get the DocumentOrder variable map of the parent operator.
     * 
     * @param op
     * @param vxqueryContext
     * @return
     */
    private HashMap<Integer, DocumentOrder> getParentDocumentOrderVariableMap(ILogicalOperator op,
            VXQueryOptimizationContext vxqueryContext) {
        AbstractLogicalOperator parentOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        if (parentOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return new HashMap<Integer, DocumentOrder>();
        }
        if (parentOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            NestedTupleSourceOperator nestedTuplesource = (NestedTupleSourceOperator) parentOp;
            return getParentDocumentOrderVariableMap(nestedTuplesource.getDataSourceReference().getValue(),
                    vxqueryContext);
        }
        return new HashMap<Integer, DocumentOrder>(vxqueryContext.getDocumentOrderOperatorVariableMap(parentOp));
    }

    private DocumentOrder propagateDocumentOrder(ILogicalExpression expr, HashMap<Integer, DocumentOrder> variableMap) {
        DocumentOrder documentOrder = null;
        switch (expr.getExpressionTag()) {
            case FUNCTION_CALL:
                AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expr;

                // Look up all arguments.
                List<DocumentOrder> argProperties = new ArrayList<DocumentOrder>();
                for (Mutable<ILogicalExpression> argExpr : functionCall.getArguments()) {
                    argProperties.add(propagateDocumentOrder(argExpr.getValue(), variableMap));
                }

                // Propagate the property.
                Function func = (Function) functionCall.getFunctionInfo();
                documentOrder = func.getDocumentOrderPropagationPolicy().propagate(argProperties);
                break;
            case VARIABLE:
                VariableReferenceExpression variableReference = (VariableReferenceExpression) expr;
                int argVariableId = variableReference.getVariableReference().getId();
                documentOrder = variableMap.get(argVariableId);
                break;
            case CONSTANT:
            default:
                documentOrder = DocumentOrder.YES;
                break;
        }
        return documentOrder;
    }

    private boolean isOperatorSortDistinctNodesAscOrAtomics(Mutable<ILogicalOperator> opRef) {
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
                BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())) {
            return false;
        }
        return true;
    }
}
