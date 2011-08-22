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
package org.apache.vxquery.compiler.tools;

import java.util.List;
import java.util.Set;

import org.apache.vxquery.compiler.expression.AttributeNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.CastExpression;
import org.apache.vxquery.compiler.expression.CastableExpression;
import org.apache.vxquery.compiler.expression.CommentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ConstantExpression;
import org.apache.vxquery.compiler.expression.DocumentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ElementNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.ExpressionVisitor;
import org.apache.vxquery.compiler.expression.ExtensionExpression;
import org.apache.vxquery.compiler.expression.FLWORExpression;
import org.apache.vxquery.compiler.expression.ForLetVariable;
import org.apache.vxquery.compiler.expression.FunctionCallExpression;
import org.apache.vxquery.compiler.expression.IfThenElseExpression;
import org.apache.vxquery.compiler.expression.InstanceofExpression;
import org.apache.vxquery.compiler.expression.PINodeConstructorExpression;
import org.apache.vxquery.compiler.expression.PathStepExpression;
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression;
import org.apache.vxquery.compiler.expression.TextNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.expression.TypeswitchExpression;
import org.apache.vxquery.compiler.expression.ValidateExpression;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.compiler.expression.VariableReferenceExpression;

class FreeVariableFinder implements ExpressionVisitor<Void> {
    private Set<Variable> vars;

    FreeVariableFinder(Set<Variable> vars) {
        this.vars = vars;
    }

    @Override
    public Void visitAttributeNodeConstructorExpression(AttributeNodeConstructorExpression expr) {
        expr.getName().accept(this);
        expr.getContent().accept(this);
        return null;
    }

    @Override
    public Void visitCastExpression(CastExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitCastableExpression(CastableExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitCommentNodeConstructorExpression(CommentNodeConstructorExpression expr) {
        expr.getContent().accept(this);
        return null;
    }

    @Override
    public Void visitConstantExpression(ConstantExpression expr) {
        return null;
    }

    @Override
    public Void visitDocumentNodeConstructorExpression(DocumentNodeConstructorExpression expr) {
        expr.getContent().accept(this);
        return null;
    }

    @Override
    public Void visitElementNodeConstructorExpression(ElementNodeConstructorExpression expr) {
        expr.getName().accept(this);
        expr.getContent().accept(this);
        return null;
    }

    @Override
    public Void visitExtensionExpression(ExtensionExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitFLWORExpression(FLWORExpression expr) {
        expr.getReturnExpression().accept(this);
        List<FLWORExpression.Clause> clauses = expr.getClauses();
        int len = clauses.size();
        for (int i = len - 1; i >= 0; --i) {
            FLWORExpression.Clause clause = clauses.get(i);
            switch (clause.getTag()) {
                case FOR: {
                    FLWORExpression.ForClause fc = (FLWORExpression.ForClause) clause;
                    vars.remove(fc.getForVariable());
                    vars.remove(fc.getPosVariable());
                    vars.remove(fc.getScoreVariable());
                    fc.getForVariable().getSequence().accept(this);
                    break;
                }

                case LET: {
                    FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                    lc.getLetVariable().getSequence().accept(this);
                    vars.remove(lc.getLetVariable());
                    break;
                }

                case WHERE: {
                    FLWORExpression.WhereClause wc = (FLWORExpression.WhereClause) clause;
                    wc.getCondition().accept(this);
                    break;
                }

                case ORDERBY: {
                    FLWORExpression.OrderbyClause oc = (FLWORExpression.OrderbyClause) clause;
                    for (ExpressionHandle h : oc.getOrderingExpressions()) {
                        h.accept(this);
                    }
                    break;
                }
            }
        }
        return null;
    }

    @Override
    public Void visitFunctionCallExpression(FunctionCallExpression expr) {
        for (ExpressionHandle arg : expr.getArguments()) {
            arg.get().accept(this);
        }
        return null;
    }

    @Override
    public Void visitIfThenElseExpression(IfThenElseExpression expr) {
        expr.getCondition().accept(this);
        expr.getThenExpression().accept(this);
        expr.getElseExpression().accept(this);
        return null;
    }

    @Override
    public Void visitInstanceofExpression(InstanceofExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitPINodeConstructorExpression(PINodeConstructorExpression expr) {
        expr.getTarget().accept(this);
        expr.getContent().accept(this);
        return null;
    }

    @Override
    public Void visitPathStepExpression(PathStepExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitPromoteExpression(PromoteExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitQuantifiedExpression(QuantifiedExpression expr) {
        expr.getSatisfiesExpression().accept(this);
        List<ForLetVariable> fVars = expr.getQuantifiedVariables();
        int len = fVars.size();
        for (int i = len - 1; i >= 0; --i) {
            ForLetVariable fv = fVars.get(i);
            vars.remove(fv);
            fv.getSequence().accept(this);
        }
        return null;
    }

    @Override
    public Void visitTextNodeConstructorExpression(TextNodeConstructorExpression expr) {
        expr.getContent().accept(this);
        return null;
    }

    @Override
    public Void visitTreatExpression(TreatExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitTypeswitchExpression(TypeswitchExpression expr) {
        expr.getDefaultExpression().accept(this);
        List<TypeswitchExpression.Case> cases = expr.getCases();
        int len = cases.size();
        for (int i = len - 1; i >= 0; --i) {
            TypeswitchExpression.Case c = cases.get(i);
            c.getReturnExpression().accept(this);
            vars.remove(c.getCaseVariable());
        }
        vars.remove(expr.getInput());
        expr.getInput().getSequence().accept(this);
        return null;
    }

    @Override
    public Void visitValidateExpression(ValidateExpression expr) {
        expr.getInput().accept(this);
        return null;
    }

    @Override
    public Void visitVariableReferenceExpression(VariableReferenceExpression expr) {
        vars.add(expr.getVariable());
        return null;
    }
}