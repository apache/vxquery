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
package org.apache.vxquery.compiler.rewriter.rules.util;

import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.cardinality.Cardinality;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;

public class CardinalityRuleToolbox {
    /**
     * Get the Cardinality variable of the parent operator.
     * 
     * @param op
     * @param vxqueryContext
     * @return
     */
    public static Cardinality getProducerCardinality(ILogicalOperator op, VXQueryOptimizationContext vxqueryContext) {
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

    public static Cardinality updateCardinalityVariable(AbstractLogicalOperator op, Cardinality cardinalityVariable,
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
            case DATASOURCESCAN:
            case INNERJOIN:
            case LEFTOUTERJOIN:
            case UNNEST:
                cardinalityVariable = Cardinality.MANY;
                break;

            // The following operators do not change the variable.
            case ASSIGN:
            case DISTRIBUTE_RESULT:
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
            default:
                throw new RuntimeException("Operator (" + op.getOperatorTag()
                        + ") has not been implemented in rewrite rule.");
        }
        return cardinalityVariable;
    }
}
