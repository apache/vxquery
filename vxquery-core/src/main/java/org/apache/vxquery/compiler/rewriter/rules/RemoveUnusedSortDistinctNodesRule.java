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
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.cardinality.Cardinality;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodes;
import org.apache.vxquery.compiler.rewriter.rules.util.CardinalityRuleToolbox;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for where the xquery sort distinct function is used and
 * determines if the sort and distinct is necessary. The plan is modified if
 * any of these items is not required.
 *
 * <pre>
 * Before
 *
 *   plan__parent
 *   ASSIGN( $v1 : sort_distinct_nodes_asc_or_atomics( $v0 ) )
 *   plan__child
 *
 *   Where $v0 is a variable defined in plan__child.
 *
 * After
 *
 *   if ( $v0 is unique nodes &amp;&amp; $v0 is in document order )
 *
 *     plan__parent
 *     ASSIGN( $v1 : $v0 )
 *     plan__child
 *
 *   if ( $v0 is NOT unique nodes &amp;&amp; $v0 is in document order )
 *
 *     plan__parent
 *     ASSIGN( $v1 : distinct_nodes_or_atomics( $v0 ) )
 *     plan__child
 *
 *   if ( $v0 is unique nodes &amp;&amp; $v0 is NOT in document order )
 *
 *     plan__parent
 *     ASSIGN( $v1 : sort_nodes_asc( $v0 ) )
 *     plan__child
 *
 *   if ( $v0 is NOT unique nodes &amp;&amp; $v0 is NOT in document order )
 *
 *     plan__parent
 *     ASSIGN( $v1 : sort_distinct_nodes_asc_or_atomics( $v0 ) )
 *     plan__child
 * </pre>
 *
 * @author prestonc
 */

public class RemoveUnusedSortDistinctNodesRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean operatorChanged = false;
        // Do not process empty or nested tuple source.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        // Initialization.
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;

        // Find the available variables.
        Cardinality cardinalityVariable = CardinalityRuleToolbox.getProducerCardinality(opRef.getValue(),
                vxqueryContext);
        HashMap<Integer, DocumentOrder> documentOrderVariables = getProducerDocumentOrderVariableMap(opRef.getValue(),
                vxqueryContext);
        HashMap<Integer, UniqueNodes> uniqueNodesVariables = getProducerUniqueNodesVariableMap(opRef.getValue(),
                vxqueryContext);

        // Update sort operator if found.
        int variableId = getOperatorSortDistinctNodesAscOrAtomicsArgumentVariableId(opRef);
        if (variableId > 0) {
            // Find the function expression.
            // All the checks for these variable assigns and casting were done in the
            // getOperatorSortDistinctNodesAscOrAtomicsArgumentVariableId function.
            AssignOperator assign = (AssignOperator) op;
            ILogicalExpression logicalExpression = (ILogicalExpression) assign.getExpressions().get(0).getValue();
            AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;

            if (uniqueNodesVariables.get(variableId) == UniqueNodes.YES) {
                // Only unique nodes.
                if (documentOrderVariables.get(variableId) == DocumentOrder.YES) {
                    // Do not need to sort or remove duplicates from the result.
                    assign.getExpressions().get(0).setValue(functionCall.getArguments().get(0).getValue());
                    operatorChanged = true;
                } else {
                    // Unique nodes but needs to be sorted.
                    functionCall.setFunctionInfo(BuiltinOperators.SORT_NODES_ASC);
                    operatorChanged = true;
                }
            } else {
                // Duplicates possible in the result.
                if (documentOrderVariables.get(variableId) == DocumentOrder.YES) {
                    // Do not need to sort the result.
                    functionCall.setFunctionInfo(BuiltinOperators.DISTINCT_NODES_OR_ATOMICS);
                    operatorChanged = true;
                } else {
                    // No change.
                }
            }
        }
        if (operatorChanged) {
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        // Now with the new operator, update the variable mappings.
        cardinalityVariable = CardinalityRuleToolbox.updateCardinalityVariable(op, cardinalityVariable, vxqueryContext);
        updateVariableMap(op, cardinalityVariable, documentOrderVariables, uniqueNodesVariables, vxqueryContext);

        // Save propagated value.
        vxqueryContext.putCardinalityOperatorMap(opRef.getValue(), cardinalityVariable);
        vxqueryContext.putDocumentOrderOperatorVariableMap(opRef.getValue(), documentOrderVariables);
        vxqueryContext.putUniqueNodesOperatorVariableMap(opRef.getValue(), uniqueNodesVariables);
        return operatorChanged;
    }

    private int getOperatorSortDistinctNodesAscOrAtomicsArgumentVariableId(Mutable<ILogicalOperator> opRef) {
        // Check if assign is for sort-distinct-nodes-asc-or-atomics.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return 0;
        }
        AssignOperator assign = (AssignOperator) op;

        // Check to see if the expression is a function and
        // sort-distinct-nodes-asc-or-atomics.
        ILogicalExpression logicalExpression = (ILogicalExpression) assign.getExpressions().get(0).getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return 0;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        if (!functionCall.getFunctionIdentifier()
                .equals(BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS.getFunctionIdentifier())) {
            return 0;
        }

        // Find the variable id used as the parameter.
        ILogicalExpression logicalExpression2 = (ILogicalExpression) functionCall.getArguments().get(0).getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return 0;
        }
        VariableReferenceExpression variableExpression = (VariableReferenceExpression) logicalExpression2;
        return variableExpression.getVariableReference().getId();
    }

    /**
     * Get the DocumentOrder variable map of the parent operator.
     *
     * @param op
     * @param vxqueryContext
     * @return
     */
    private HashMap<Integer, DocumentOrder> getProducerDocumentOrderVariableMap(ILogicalOperator op,
            VXQueryOptimizationContext vxqueryContext) {
        AbstractLogicalOperator producerOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        switch (producerOp.getOperatorTag()) {
            case EMPTYTUPLESOURCE:
                return new HashMap<Integer, DocumentOrder>();
            case NESTEDTUPLESOURCE:
                NestedTupleSourceOperator nestedTuplesource = (NestedTupleSourceOperator) producerOp;
                return getProducerDocumentOrderVariableMap(nestedTuplesource.getDataSourceReference().getValue(),
                        vxqueryContext);
            default:
                return new HashMap<Integer, DocumentOrder>(
                        vxqueryContext.getDocumentOrderOperatorVariableMap(producerOp));
        }
    }

    /**
     * Get the UniqueNodes variable map of the parent operator.
     *
     * @param op
     * @param vxqueryContext
     * @return Hash map of variables to unique nodes.
     */
    private HashMap<Integer, UniqueNodes> getProducerUniqueNodesVariableMap(ILogicalOperator op,
            VXQueryOptimizationContext vxqueryContext) {
        AbstractLogicalOperator producerOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        switch (producerOp.getOperatorTag()) {
            case EMPTYTUPLESOURCE:
                return new HashMap<Integer, UniqueNodes>();
            case NESTEDTUPLESOURCE:
                NestedTupleSourceOperator nestedTuplesource = (NestedTupleSourceOperator) producerOp;
                return getProducerUniqueNodesVariableMap(nestedTuplesource.getDataSourceReference().getValue(),
                        vxqueryContext);
            default:
                return new HashMap<Integer, UniqueNodes>(vxqueryContext.getUniqueNodesOperatorVariableMap(producerOp));
        }
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

    private UniqueNodes propagateUniqueNodes(ILogicalExpression expr, HashMap<Integer, UniqueNodes> variableMap) {
        UniqueNodes uniqueNodes = null;
        switch (expr.getExpressionTag()) {
            case FUNCTION_CALL:
                AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expr;

                // Look up all arguments.
                List<UniqueNodes> argProperties = new ArrayList<UniqueNodes>();
                for (Mutable<ILogicalExpression> argExpr : functionCall.getArguments()) {
                    argProperties.add(propagateUniqueNodes(argExpr.getValue(), variableMap));
                }

                // Propagate the property.
                Function func = (Function) functionCall.getFunctionInfo();
                uniqueNodes = func.getUniqueNodesPropagationPolicy().propagate(argProperties);
                break;
            case VARIABLE:
                VariableReferenceExpression variableReference = (VariableReferenceExpression) expr;
                int argVariableId = variableReference.getVariableReference().getId();
                uniqueNodes = variableMap.get(argVariableId);
                break;
            case CONSTANT:
            default:
                uniqueNodes = UniqueNodes.YES;
                break;
        }
        return uniqueNodes;
    }

    /**
     * Sets all the variables to DocumentOrder.
     *
     * @param documentOrderVariables
     * @param documentOrder
     */
    private void resetDocumentOrderVariables(HashMap<Integer, DocumentOrder> documentOrderVariables,
            DocumentOrder documentOrder) {
        for (Entry<Integer, DocumentOrder> entry : documentOrderVariables.entrySet()) {
            documentOrderVariables.put(entry.getKey(), documentOrder);
        }
    }

    /**
     * Sets all the variables to UniqueNodes.
     *
     * @param uniqueNodesVariables
     * @param uniqueNodes
     */
    private void resetUniqueNodesVariables(HashMap<Integer, UniqueNodes> uniqueNodesVariables,
            UniqueNodes uniqueNodes) {
        for (Entry<Integer, UniqueNodes> entry : uniqueNodesVariables.entrySet()) {
            uniqueNodesVariables.put(entry.getKey(), uniqueNodes);
        }
    }

    private void updateVariableMap(AbstractLogicalOperator op, Cardinality cardinalityVariable,
            HashMap<Integer, DocumentOrder> documentOrderVariables, HashMap<Integer, UniqueNodes> uniqueNodesVariables,
            VXQueryOptimizationContext vxqueryContext) {
        int variableId;
        DocumentOrder documentOrder;
        HashMap<Integer, DocumentOrder> documentOrderVariablesForOperator = getProducerDocumentOrderVariableMap(op,
                vxqueryContext);
        UniqueNodes uniqueNodes;
        HashMap<Integer, UniqueNodes> uniqueNodesVariablesForOperator = getProducerUniqueNodesVariableMap(op,
                vxqueryContext);

        // Get the DocumentOrder from propagation.
        switch (op.getOperatorTag()) {
            case AGGREGATE:
                AggregateOperator aggregate = (AggregateOperator) op;
                for (int index = 0; index < aggregate.getExpressions().size(); index++) {
                    ILogicalExpression aggregateLogicalExpression = (ILogicalExpression) aggregate.getExpressions()
                            .get(index).getValue();
                    variableId = aggregate.getVariables().get(index).getId();
                    documentOrder = propagateDocumentOrder(aggregateLogicalExpression,
                            documentOrderVariablesForOperator);
                    uniqueNodes = propagateUniqueNodes(aggregateLogicalExpression, uniqueNodesVariablesForOperator);
                    documentOrderVariables.put(variableId, documentOrder);
                    uniqueNodesVariables.put(variableId, uniqueNodes);
                }
                break;
            case ASSIGN:
                AssignOperator assign = (AssignOperator) op;
                for (int index = 0; index < assign.getExpressions().size(); index++) {
                    ILogicalExpression assignLogicalExpression = (ILogicalExpression) assign.getExpressions().get(index)
                            .getValue();
                    variableId = assign.getVariables().get(index).getId();
                    documentOrder = propagateDocumentOrder(assignLogicalExpression, documentOrderVariablesForOperator);
                    uniqueNodes = propagateUniqueNodes(assignLogicalExpression, uniqueNodesVariablesForOperator);
                    documentOrderVariables.put(variableId, documentOrder);
                    uniqueNodesVariables.put(variableId, uniqueNodes);
                }
                break;
            case INNERJOIN:
            case LEFTOUTERJOIN:
                resetDocumentOrderVariables(documentOrderVariables, DocumentOrder.NO);
                resetUniqueNodesVariables(uniqueNodesVariables, UniqueNodes.NO);
                break;
            case ORDER:
                // Get order variable id that is altered.
                OrderOperator order = (OrderOperator) op;
                for (int index = 0; index < order.getOrderExpressions().size(); index++) {
                    ILogicalExpression orderLogicalExpression = order.getOrderExpressions().get(index).second
                            .getValue();
                    if (orderLogicalExpression.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        throw new RuntimeException("Operator (" + op.getOperatorTag()
                                + ") has received unexpected input in rewrite rule.");
                    }
                    VariableReferenceExpression variableExpression = (VariableReferenceExpression) orderLogicalExpression;
                    variableId = variableExpression.getVariableReference().getId();

                    // Remove document order from variable used in order operator.
                    documentOrderVariables.put(variableId, DocumentOrder.NO);
                }
                break;
            case SUBPLAN:
                // Find the last operator to set a variable and call this function again.
                SubplanOperator subplan = (SubplanOperator) op;
                for (int index = 0; index < subplan.getNestedPlans().size(); index++) {
                    AbstractLogicalOperator lastOperator = (AbstractLogicalOperator) subplan.getNestedPlans().get(index)
                            .getRoots().get(0).getValue();
                    updateVariableMap(lastOperator, cardinalityVariable, documentOrderVariables, uniqueNodesVariables,
                            vxqueryContext);
                }
                break;
            case UNNEST:
                // Get unnest item property.
                UnnestOperator unnest = (UnnestOperator) op;
                ILogicalExpression unnestLogicalExpression = (ILogicalExpression) unnest.getExpressionRef().getValue();
                variableId = unnest.getVariables().get(0).getId();
                Cardinality inputCardinality = vxqueryContext
                        .getCardinalityOperatorMap(op.getInputs().get(0).getValue());
                documentOrder = propagateDocumentOrder(unnestLogicalExpression, documentOrderVariablesForOperator);
                uniqueNodes = propagateUniqueNodes(unnestLogicalExpression, uniqueNodesVariablesForOperator);

                // Reset properties based on unnest duplication.
                resetDocumentOrderVariables(documentOrderVariables, DocumentOrder.NO);
                resetUniqueNodesVariables(uniqueNodesVariables, UniqueNodes.NO);
                if (inputCardinality == Cardinality.ONE) {
                    documentOrderVariables.put(variableId, documentOrder);
                    uniqueNodesVariables.put(variableId, uniqueNodes);
                } else {
                    documentOrderVariables.put(variableId, DocumentOrder.NO);
                    uniqueNodesVariables.put(variableId, UniqueNodes.NO);
                }

                // Add position variable property.
                if (unnest.getPositionalVariable() != null) {
                    variableId = unnest.getPositionalVariable().getId();
                    if (inputCardinality == Cardinality.ONE) {
                        documentOrderVariables.put(variableId, DocumentOrder.YES);
                        uniqueNodesVariables.put(variableId, UniqueNodes.YES);
                    } else {
                        documentOrderVariables.put(variableId, DocumentOrder.NO);
                        uniqueNodesVariables.put(variableId, UniqueNodes.NO);
                    }
                }
                break;

            // The following operators do not change or add to the variable map.

            case DATASOURCESCAN:
            case DISTRIBUTE_RESULT:
            case EMPTYTUPLESOURCE:
            case EXCHANGE:
            case GROUP:
            case NESTEDTUPLESOURCE:
            case PROJECT:
            case SELECT:
            case WRITE:
            case WRITE_RESULT:
                break;

            // The following operators' analysis has not yet been implemented.
            default:
                throw new RuntimeException(
                        "Operator (" + op.getOperatorTag() + ") has not been implemented in rewrite rule.");
        }
    }

}
