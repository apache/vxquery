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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The AbstractUsedVariablesProcessingRule provides a frame work to track used
 * variables to assist in some rewrite rules that must track used variables in
 * the above plan.
 * 
 * @author prestonc
 */
public abstract class AbstractUsedVariablesProcessingRule implements IAlgebraicRewriteRule {

    protected List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
    protected boolean hasRun = false;
    Mutable<ILogicalOperator> firstOpRef;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (hasRun && !firstOpRef.equals(opRef)) {
            return false;
        } else {
            firstOpRef = opRef;
        }
        boolean modified = false;
        boolean modified_last_pass;
        do {
            usedVariables.clear();
            modified_last_pass = rewritePreTrackingUsedVariables(opRef, context);
            if (modified_last_pass) {
                modified = modified_last_pass;
            }
        } while (modified_last_pass);
        hasRun = true;
        return modified;
    }

    protected boolean rewritePreTrackingUsedVariables(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = processOperator(opRef, context);
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Descend into nested plans merging unnest along the way.
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opwnp = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan rootPlans : opwnp.getNestedPlans()) {
                for (Mutable<ILogicalOperator> inputOpRef : rootPlans.getRoots()) {
                    if (rewritePreTrackingUsedVariables(inputOpRef, context)) {
                        modified = true;
                    }
                }
            }
        }

        // Only add variables after operator is used.
        VariableUtilities.getUsedVariables(op, usedVariables);

        // Descend into children merging unnest along the way.
        if (op.hasInputs()) {
            for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
                if (rewritePreTrackingUsedVariables(inputOpRef, context)) {
                    modified = true;
                }
            }
        }

        return modified;
    }

    protected abstract boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException;
}
