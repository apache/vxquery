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

import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.IfThenElseExpression;
import org.apache.vxquery.compiler.rewriter.framework.AbstractRewriteRule;
import org.apache.vxquery.compiler.tools.ExpressionUtils;

public class EliminateIfThenElseDeadCode extends AbstractRewriteRule {
    public EliminateIfThenElseDeadCode(int minOptimizationLevel) {
        super(minOptimizationLevel);
    }

    public boolean rewritePost(ExpressionHandle exprHandle) {
        Expression expr = exprHandle.get();
        if (expr.getTag() == ExprTag.IFTHENELSE) {
            IfThenElseExpression ie = (IfThenElseExpression) expr;
            Expression condition = ie.getCondition().get();
            Boolean bv = ExpressionUtils.getBooleanValue(condition);
            if (bv != null) {
                exprHandle.set(bv ? ie.getThenExpression().get() : ie.getElseExpression().get());
                return true;
            }
        }
        return false;
    }
}