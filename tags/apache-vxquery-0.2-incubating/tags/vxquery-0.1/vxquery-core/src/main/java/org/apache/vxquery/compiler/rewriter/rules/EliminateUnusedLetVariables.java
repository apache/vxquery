/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.rewriter.rules;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.FLWORExpression;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.compiler.rewriter.framework.AbstractRewriteRule;
import org.apache.vxquery.compiler.tools.ExpressionUtils;
import org.apache.vxquery.compiler.tools.FreeVariableMaintainer;

public class EliminateUnusedLetVariables extends AbstractRewriteRule {
    public EliminateUnusedLetVariables(int minOptimizationLevel) {
        super(minOptimizationLevel);
    }

    public boolean rewritePost(ExpressionHandle exprHandle) {
        boolean changed = false;
        Expression expr = exprHandle.get();
        if (expr.getTag() == ExprTag.FLWOR) {
            FLWORExpression flwor = (FLWORExpression) expr;
            Set<Variable> freeVars = new HashSet<Variable>();
            FreeVariableMaintainer fvm = ExpressionUtils.createFreeVariableMaintainer(freeVars);
            fvm.updateFreeVariables(flwor.getReturnExpression().get());

            List<FLWORExpression.Clause> clauses = flwor.getClauses();
            for (int i = clauses.size() - 1; i >= 0; --i) {
                FLWORExpression.Clause clause = clauses.get(i);
                if (clause.getTag() == FLWORExpression.ClauseTag.LET) {
                    FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                    if (!freeVars.contains(lc.getLetVariable())) {
                        clauses.remove(i);
                        changed = true;
                        continue;
                    }
                }
                fvm.updateFreeVariables(clause);
            }
        }
        return changed;
    }
}