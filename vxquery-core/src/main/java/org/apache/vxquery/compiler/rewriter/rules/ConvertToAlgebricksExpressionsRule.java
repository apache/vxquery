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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConvertToAlgebricksExpressionsRule implements IAlgebraicRewriteRule {
    final List<Mutable<ILogicalExpression>> functionList = new ArrayList<Mutable<ILogicalExpression>>();

    final Map<FunctionIdentifier, FunctionIdentifier> ALGEBRICKS_MAP = new HashMap<FunctionIdentifier, FunctionIdentifier>();

    public ConvertToAlgebricksExpressionsRule() {
        ALGEBRICKS_MAP.put(BuiltinOperators.AND.getFunctionIdentifier(), AlgebricksBuiltinFunctions.AND);
        ALGEBRICKS_MAP.put(BuiltinOperators.OR.getFunctionIdentifier(), AlgebricksBuiltinFunctions.OR);
        ALGEBRICKS_MAP.put(BuiltinFunctions.FN_NOT_1.getFunctionIdentifier(), AlgebricksBuiltinFunctions.NOT);
        ALGEBRICKS_MAP.put(BuiltinOperators.VALUE_EQ.getFunctionIdentifier(), AlgebricksBuiltinFunctions.EQ);
        ALGEBRICKS_MAP.put(BuiltinOperators.VALUE_NE.getFunctionIdentifier(), AlgebricksBuiltinFunctions.NEQ);
        ALGEBRICKS_MAP.put(BuiltinOperators.VALUE_LT.getFunctionIdentifier(), AlgebricksBuiltinFunctions.LT);
        ALGEBRICKS_MAP.put(BuiltinOperators.VALUE_LE.getFunctionIdentifier(), AlgebricksBuiltinFunctions.LE);
        ALGEBRICKS_MAP.put(BuiltinOperators.VALUE_GT.getFunctionIdentifier(), AlgebricksBuiltinFunctions.GT);
        ALGEBRICKS_MAP.put(BuiltinOperators.VALUE_GE.getFunctionIdentifier(), AlgebricksBuiltinFunctions.GE);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = false;
        List<Mutable<ILogicalExpression>> expressions = OperatorToolbox.getExpressions(opRef);
        for (Mutable<ILogicalExpression> expression : expressions) {
            if (processExpression(opRef, expression, context)) {
                modified = true;
            }
        }
        return modified;
    }

    private boolean processExpression(Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> search,
            IOptimizationContext context) {
        boolean modified = false;
        functionList.clear();
        ExpressionToolbox.findAllFunctionExpressions(search, BuiltinFunctions.FN_BOOLEAN_1.getFunctionIdentifier(),
                functionList);
        for (Mutable<ILogicalExpression> searchM : functionList) {
            // Get input function
            AbstractFunctionCallExpression searchFunction = (AbstractFunctionCallExpression) searchM.getValue();
            ILogicalExpression argFirst = searchFunction.getArguments().get(0).getValue();
            if (argFirst.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) argFirst;
            if (ALGEBRICKS_MAP.containsKey(functionCall.getFunctionIdentifier())) {
                FunctionIdentifier algebricksFid = ALGEBRICKS_MAP.get(functionCall.getFunctionIdentifier());
                IFunctionInfo algebricksFunction = context.getMetadataProvider().lookupFunction(algebricksFid);
                functionCall.setFunctionInfo(algebricksFunction);
                searchM.setValue(argFirst);
                modified = true;
            }
        }
        return modified;
    }

}
