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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.ExpressionPrinter;
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.rewriter.framework.AbstractRewriteRule;
import org.apache.vxquery.compiler.tools.ExpressionUtils;
import org.apache.vxquery.types.TypeOperations;
import org.apache.vxquery.types.XQType;

public class EliminateUselessTypeCoersionExpressions extends AbstractRewriteRule {
    private static final Logger LOGGER = Logger.getLogger(EliminateUselessTypeCoersionExpressions.class.getName());

    public EliminateUselessTypeCoersionExpressions(int minOptimizationLevel) {
        super(minOptimizationLevel);
    }

    public boolean rewritePost(ExpressionHandle exprHandle) {
        Expression expr = exprHandle.get();
        if (expr.getTag() == ExprTag.TREAT) {
            TreatExpression te = (TreatExpression) expr;
            XQType eType = te.getInput().accept(ExpressionUtils.createTypeInferringVisitor());
            XQType rType = te.getType().toXQType();
            boolean subtype = TypeOperations.isSubtypeOf(eType, rType);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Found TREAT expression");
                LOGGER.fine(ExpressionPrinter.prettyPrint(te));
                LOGGER.fine("Input type: " + eType);
                LOGGER.fine("Required type: " + rType);
                LOGGER.fine("isSubtypeOf(eType, rType): " + subtype);
            }
            if (subtype) {
                exprHandle.set(te.getInput().get());
                return true;
            }
        } else if (expr.getTag() == ExprTag.PROMOTE) {
            PromoteExpression pe = (PromoteExpression) expr;
            XQType eType = pe.getInput().accept(ExpressionUtils.createTypeInferringVisitor());
            XQType rType = pe.getType().toXQType();
            boolean subtype = TypeOperations.isSubtypeOf(eType, rType);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Found PROMOTE expression");
                LOGGER.fine(ExpressionPrinter.prettyPrint(pe));
                LOGGER.fine("Input type: " + eType);
                LOGGER.fine("Required type: " + rType);
                LOGGER.fine("isSubtypeOf(eType, rType): " + subtype);
            }
            if (subtype) {
                exprHandle.set(pe.getInput().get());
                return true;
            }
        }

        return false;
    }
}