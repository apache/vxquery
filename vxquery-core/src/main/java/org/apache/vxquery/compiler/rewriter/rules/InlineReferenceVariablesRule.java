/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InlineVariablesRule;

/**
 * Where assign operators only assign a new variable ID for a reference expression,
 * all references are updated to the first variable ID.
 */
public class InlineReferenceVariablesRule extends InlineVariablesRule {

    @Override
    protected boolean performBottomUpAction(AbstractLogicalOperator op) throws AlgebricksException {
        inlineVisitor.setOperator(op);
        return op.acceptExpressionTransform(inlineVisitor);
    }

    @Override
    protected boolean inlineVariables(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Update mapping from variables to expressions during top-down traversal.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            List<LogicalVariable> vars = assignOp.getVariables();
            List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();
            for (int i = 0; i < vars.size(); i++) {
                ILogicalExpression expr = exprs.get(i).getValue();
                // Ignore functions.                
                if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                varAssignRhs.put(vars.get(i), exprs.get(i).getValue());
            }
        }

        boolean modified = false;
        // Descend into nested plans removing projects on the way.
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            SubplanOperator subplan = (SubplanOperator) op;
            for (ILogicalPlan nestedOpRef : subplan.getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootOpRef : nestedOpRef.getRoots()) {
                    if (inlineVariables(rootOpRef, context)) {
                        modified = true;
                    }
                }
            }
        }
        // Descend into children removing projects on the way.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (inlineVariables(inputOpRef, context)) {
                modified = true;
            }
        }

        if (performBottomUpAction(op)) {
            modified = true;
        }

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            context.addToDontApplySet(this, op);
            // Re-enable rules that we may have already tried. They could be applicable now after inlining.
            context.removeFromAlreadyCompared(opRef.getValue());
        }

        return modified;
    }

}
