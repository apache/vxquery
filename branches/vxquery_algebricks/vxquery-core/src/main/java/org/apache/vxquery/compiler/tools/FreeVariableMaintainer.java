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
package org.apache.vxquery.compiler.tools;

import java.util.Set;

import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.FLWORExpression;
import org.apache.vxquery.compiler.expression.ForLetVariable;
import org.apache.vxquery.compiler.expression.PositionVariable;
import org.apache.vxquery.compiler.expression.ScoreVariable;
import org.apache.vxquery.compiler.expression.Variable;

public class FreeVariableMaintainer {
    private Set<Variable> freeVars;
    private FreeVariableFinder finder;

    FreeVariableMaintainer(Set<Variable> freeVars) {
        this.freeVars = freeVars;
        this.finder = new FreeVariableFinder(freeVars);
    }

    public void updateFreeVariables(Expression expr) {
        expr.accept(finder);
    }

    public void updateFreeVariables(FLWORExpression.Clause clause) {
        switch (clause.getTag()) {
            case FOR:
                FLWORExpression.ForClause fc = (FLWORExpression.ForClause) clause;
                ForLetVariable forVar = fc.getForVariable();
                forVar.getSequence().accept(finder);
                freeVars.remove(forVar);
                PositionVariable posVar = fc.getPosVariable();
                if (posVar != null) {
                    freeVars.remove(posVar);
                }
                ScoreVariable scoreVar = fc.getScoreVariable();
                if (scoreVar != null) {
                    freeVars.remove(scoreVar);
                }
                break;

            case LET:
                FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                ForLetVariable letVar = lc.getLetVariable();
                letVar.getSequence().accept(finder);
                freeVars.remove(letVar);
                break;

            case ORDERBY:
                FLWORExpression.OrderbyClause oc = (FLWORExpression.OrderbyClause) clause;
                for (ExpressionHandle eh : oc.getOrderingExpressions()) {
                    eh.get().accept(finder);
                }
                break;

            case WHERE:
                FLWORExpression.WhereClause wc = (FLWORExpression.WhereClause) clause;
                wc.getCondition().accept(finder);
                break;
        }
    }
}
