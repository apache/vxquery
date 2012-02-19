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
package org.apache.vxquery.compiler.rewriter.framework;

import java.util.List;

import org.apache.vxquery.compiler.expression.AttributeNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.CastExpression;
import org.apache.vxquery.compiler.expression.CastableExpression;
import org.apache.vxquery.compiler.expression.CommentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.DocumentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ElementNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.ExtensionExpression;
import org.apache.vxquery.compiler.expression.FLWORExpression;
import org.apache.vxquery.compiler.expression.ForLetVariable;
import org.apache.vxquery.compiler.expression.FunctionCallExpression;
import org.apache.vxquery.compiler.expression.IfThenElseExpression;
import org.apache.vxquery.compiler.expression.InstanceofExpression;
import org.apache.vxquery.compiler.expression.MutableExpressionVisitor;
import org.apache.vxquery.compiler.expression.PINodeConstructorExpression;
import org.apache.vxquery.compiler.expression.PathStepExpression;
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression;
import org.apache.vxquery.compiler.expression.TextNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.expression.TypeswitchExpression;
import org.apache.vxquery.compiler.expression.ValidateExpression;

public class RulesetDriver implements MutableExpressionVisitor<Boolean> {
    private RewriteRule rule;

    public void rewrite(ExpressionHandle handle, List<RewriteRule> ruleset, int optimizationLevel) {
        boolean done = false;
        while (!done) {
            done = true;
            for (RewriteRule rule : ruleset) {
                if (optimizationLevel < rule.getMinOptimizationLevel()) {
                    continue;
                }
                this.rule = rule;
                done = !handle.accept(this) && done;
            }
        }
    }

    @Override
    public Boolean visitAttributeNodeConstructorExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        AttributeNodeConstructorExpression e = (AttributeNodeConstructorExpression) handle.get();
        boolean res = e.getName().accept(this);
        res = e.getContent().accept(this) || res;

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitCastExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        CastExpression e = (CastExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitCastableExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        CastableExpression e = (CastableExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitCommentNodeConstructorExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        CommentNodeConstructorExpression e = (CommentNodeConstructorExpression) handle.get();
        boolean res = e.getContent().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitConstantExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        if (rule.rewritePost(handle)) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitDocumentNodeConstructorExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        DocumentNodeConstructorExpression e = (DocumentNodeConstructorExpression) handle.get();
        boolean res = e.getContent().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitElementNodeConstructorExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        ElementNodeConstructorExpression e = (ElementNodeConstructorExpression) handle.get();
        boolean res = e.getName().accept(this);
        res = e.getContent().accept(this) || res;

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitExtensionExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        ExtensionExpression e = (ExtensionExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitFLWORExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        boolean res = false;
        FLWORExpression e = (FLWORExpression) handle.get();
        for (FLWORExpression.Clause clause : e.getClauses()) {
            switch (clause.getTag()) {
                case FOR: {
                    FLWORExpression.ForClause fc = (FLWORExpression.ForClause) clause;
                    res = fc.getForVariable().getSequence().accept(this) || res;
                    break;
                }

                case LET: {
                    FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                    res = lc.getLetVariable().getSequence().accept(this) || res;
                    break;
                }

                case WHERE: {
                    FLWORExpression.WhereClause wc = (FLWORExpression.WhereClause) clause;
                    res = wc.getCondition().accept(this) || res;
                    break;
                }

                case ORDERBY: {
                    FLWORExpression.OrderbyClause oc = (FLWORExpression.OrderbyClause) clause;
                    for (ExpressionHandle h : oc.getOrderingExpressions()) {
                        res = h.accept(this) || res;
                    }
                    break;
                }
            }
        }
        res = e.getReturnExpression().accept(this) || res;

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitFunctionCallExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        boolean res = false;
        FunctionCallExpression e = (FunctionCallExpression) handle.get();
        for (ExpressionHandle h : e.getArguments()) {
            res = h.accept(this) || res;
        }

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitIfThenElseExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        boolean res = false;
        IfThenElseExpression e = (IfThenElseExpression) handle.get();
        res = e.getCondition().accept(this) || res;
        res = e.getThenExpression().accept(this) || res;
        res = e.getElseExpression().accept(this) || res;

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitInstanceofExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        InstanceofExpression e = (InstanceofExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitPINodeConstructorExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        PINodeConstructorExpression e = (PINodeConstructorExpression) handle.get();
        boolean res = e.getContent().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitPathStepExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        PathStepExpression e = (PathStepExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitPromoteExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        PromoteExpression e = (PromoteExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitQuantifiedExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        QuantifiedExpression e = (QuantifiedExpression) handle.get();
        boolean res = false;
        for (ForLetVariable v : e.getQuantifiedVariables()) {
            res = v.getSequence().accept(this) || res;
        }
        res = e.getSatisfiesExpression().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitTextNodeConstructorExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        TextNodeConstructorExpression e = (TextNodeConstructorExpression) handle.get();
        boolean res = e.getContent().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitTreatExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        TreatExpression e = (TreatExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitTypeswitchExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        TypeswitchExpression e = (TypeswitchExpression) handle.get();
        boolean res = e.getInput().getSequence().accept(this);
        for (TypeswitchExpression.Case c : e.getCases()) {
            res = c.getCaseVariable().getSequence().accept(this) || res;
            res = c.getReturnExpression().accept(this) || res;
        }
        res = e.getDefaultExpression().accept(this) || res;

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitValidateExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        ValidateExpression e = (ValidateExpression) handle.get();
        boolean res = e.getInput().accept(this);

        if (rule.rewritePost(handle)) {
            return true;
        }
        return res;
    }

    @Override
    public Boolean visitVariableReferenceExpression(ExpressionHandle handle) {
        if (rule.rewritePre(handle)) {
            return true;
        }

        if (rule.rewritePost(handle)) {
            return true;
        }
        return false;
    }
}