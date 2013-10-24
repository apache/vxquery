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
import org.apache.vxquery.compiler.rewriter.rules.util.CardinalityRuleToolbox;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for subplans that only have one path of execution.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   SUBPLAN{
 *     plan__nested
 *     NESTEDTUPLESOURCE
 *   }
 *   plan__child
 * 
 *   Where |plan__child| == 1
 * 
 * After 
 * 
 *   plan__parent
 *   plan__nested
 *   plan__child
 * </pre>
 * 
 * @author prestonc
 */
public class EliminateSubplanForSinglePathsRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Fix EliminateSubplanForSinglePathsRule to check for variables used after the subplan.
        // TODO Add back to the rewrite rule list once fixed.
        
        // Do not process empty or nested tuple source.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        // Set cardinality in the context. Must update each time the rule is run.
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        Cardinality cardinalityVariable = CardinalityRuleToolbox.getProducerCardinality(opRef.getValue(), vxqueryContext);
        
        // Track variables created
        
        // Track variables used

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
        cardinalityVariable = CardinalityRuleToolbox.updateCardinalityVariable(op, cardinalityVariable, vxqueryContext);
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

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
