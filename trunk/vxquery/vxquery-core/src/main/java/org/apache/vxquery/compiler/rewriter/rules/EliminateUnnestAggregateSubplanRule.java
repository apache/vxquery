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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class EliminateUnnestAggregateSubplanRule implements IAlgebraicRewriteRule {
    /**
     * Find where an unnest is followed by a subplan with the root operator of aggregate.
     * Search pattern: unnest -> subplan -> (aggregate ... )
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;

        // Check if assign is for fn:Collection.
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op2;

        AbstractLogicalOperator subplanOp = (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots().get(0)
                .getValue();
        if (subplanOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        
        AbstractLogicalOperator subplanEnd = findLastSubplanOperator(subplanOp);
        
        // Remove the subplan.
        unnest.getInputs().get(0).setValue(subplanOp);
        
        // Make inline the arguments for the subplan.
        subplanEnd.getInputs().get(0).setValue(subplan.getInputs().get(0).getValue());

        return true;
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
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
