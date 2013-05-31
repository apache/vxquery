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
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.cardinality.Cardinality;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class EliminateSubplanForSinglePathsRule implements IAlgebraicRewriteRule {
    /**
     * Find where an unnest is followed by a subplan with the root operator of aggregate.
     * Search pattern: unnest -> subplan -> (aggregate ... )
     * Replacement pattern: assign -> ...
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // Do not process empty or nested tuple source.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        // Set cardinality in the context. Must update each time the rule is run.
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        Cardinality cardinalityVariable = getProducerCardinality(opRef.getValue(), vxqueryContext);

        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN && cardinalityVariable == Cardinality.ONE) {
            SubplanOperator subplan = (SubplanOperator) op;

            AbstractLogicalOperator subplanOp = (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots()
                    .get(0).getValue();
            if (subplanOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                return false;
            }

            // Change plan to remove the subplan.
            opRef.setValue(subplanOp);

            // Make inline the arguments for the subplan.
            AbstractLogicalOperator subplanEnd = findLastSubplanOperator(subplanOp);
            subplanEnd.getInputs().get(0).setValue(subplan.getInputs().get(0).getValue());

        }

        // Now with the new operator, update the variable mappings.
        cardinalityVariable = updateCardinalityVariable(op, cardinalityVariable, vxqueryContext);
        // Save propagated value.
        vxqueryContext.putCardinalityOperatorMap(opRef.getValue(), cardinalityVariable);

        return false;
    }

    private AbstractLogicalOperator findLastSubplanOperator(AbstractLogicalOperator op) {
        AbstractLogicalOperator next;
        while (op.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            next = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            if (next.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                break;
            }
        }
        return op;
    }

    /**
     * Get the Cardinality variable of the parent operator.
     * 
     * @param op
     * @param vxqueryContext
     * @return
     */
    private Cardinality getProducerCardinality(ILogicalOperator op, VXQueryOptimizationContext vxqueryContext) {
        AbstractLogicalOperator producerOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        switch (producerOp.getOperatorTag()) {
            case EMPTYTUPLESOURCE:
                return Cardinality.ONE;
            case NESTEDTUPLESOURCE:
                NestedTupleSourceOperator nestedTuplesource = (NestedTupleSourceOperator) producerOp;
                return getProducerCardinality(nestedTuplesource.getDataSourceReference().getValue(), vxqueryContext);
            default:
                return vxqueryContext.getCardinalityOperatorMap(producerOp);
        }
    }

    private Cardinality updateCardinalityVariable(AbstractLogicalOperator op, Cardinality cardinalityVariable,
            VXQueryOptimizationContext vxqueryContext) {
        switch (op.getOperatorTag()) {
            case AGGREGATE:
                cardinalityVariable = Cardinality.ONE;
                break;
            case GROUP:
            case SUBPLAN:
                // Find the last operator to set a variable and call this function again.
                AbstractOperatorWithNestedPlans operatorWithNestedPlan = (AbstractOperatorWithNestedPlans) op;
                AbstractLogicalOperator lastOperator = (AbstractLogicalOperator) operatorWithNestedPlan
                        .getNestedPlans().get(0).getRoots().get(0).getValue();
                cardinalityVariable = vxqueryContext.getCardinalityOperatorMap(lastOperator);
                break;
            case UNNEST:
                cardinalityVariable = Cardinality.MANY;
                break;

            // The following operators do not change the variable.
            case ASSIGN:
            case DATASOURCESCAN:
            case EMPTYTUPLESOURCE:
            case EXCHANGE:
            case LIMIT:
            case NESTEDTUPLESOURCE:
            case ORDER:
            case PROJECT:
            case SELECT:
            case WRITE:
            case WRITE_RESULT:
                break;

            // The following operators' analysis has not yet been implemented.
            case CLUSTER:
            case DIE:
            case DISTINCT:
            case EXTENSION_OPERATOR:
            case INDEX_INSERT_DELETE:
            case INNERJOIN:
            case INSERT_DELETE:
            case LEFTOUTERJOIN:
            case PARTITIONINGSPLIT:
            case REPLICATE:
            case RUNNINGAGGREGATE:
            case SCRIPT:
            case SINK:
            case UNIONALL:
            case UNNEST_MAP:
            case UPDATE:
            default:
                throw new RuntimeException("Operator (" + op.getOperatorTag()
                        + ") has not been implemented in rewrite rule.");
        }
        return cardinalityVariable;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
