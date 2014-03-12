/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushFunctionsOntoEqJoinBranches implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op;

        ILogicalExpression expr = join.getCondition().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (!(fi.equals(AlgebricksBuiltinFunctions.AND) || fi.equals(AlgebricksBuiltinFunctions.EQ))) {
            return false;
        }
        boolean modified = false;
        List<Mutable<ILogicalExpression>> functionList = new ArrayList<Mutable<ILogicalExpression>>();
        List<Mutable<ILogicalExpression>> variableList = new ArrayList<Mutable<ILogicalExpression>>();
        functionList.clear();
        ExpressionToolbox.findAllFunctionExpressions(join.getCondition(), AlgebricksBuiltinFunctions.EQ, functionList);
        Collection<LogicalVariable> producedVariables = new ArrayList<LogicalVariable>();
        for (Mutable<ILogicalExpression> searchM : functionList) {
            ILogicalExpression search = searchM.getValue();
            if (search.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression searchExp = (AbstractFunctionCallExpression) search;
            // Go through all argument for EQ.
            for (Mutable<ILogicalExpression> expressionM : searchExp.getArguments()) {
                // Push on to branch when possible.
                for (Mutable<ILogicalOperator> branch : join.getInputs()) {
                    producedVariables.clear();
                    getProducedVariablesInDescendantsAndSelf(branch.getValue(), producedVariables);
                    variableList.clear();
                    ExpressionToolbox.findVariableExpressions(expressionM, variableList);
                    boolean found = true;
                    for (Mutable<ILogicalExpression> searchVariableM : variableList) {
                        VariableReferenceExpression vre = (VariableReferenceExpression) searchVariableM.getValue();
                        if (!producedVariables.contains(vre.getVariableReference())) {
                            found = false;
                        }
                    }
                    if (found) {
                        // push down
                        LogicalVariable assignVariable = context.newVar();
                        AssignOperator aOp = new AssignOperator(assignVariable, new MutableObject<ILogicalExpression>(expressionM.getValue()));
                        aOp.getInputs().add(new MutableObject<ILogicalOperator>(branch.getValue()));
                        branch.setValue(aOp);
                        aOp.recomputeSchema();
                        
                        expressionM.setValue(new VariableReferenceExpression(assignVariable));
                        modified = true;
                    }
                }
            }
        }
        return modified;
    }

    public static void getProducedVariablesInDescendantsAndSelf(ILogicalOperator op, Collection<LogicalVariable> vars)
            throws AlgebricksException {
        // DFS traversal
        VariableUtilities.getProducedVariables(op, vars);
        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            getProducedVariablesInDescendantsAndSelf(c.getValue(), vars);
        }
    }


}
