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
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for where the Algebricks builtin function are temporarly in the plan in place of XQuery function.
 * The combination the Algebricks builtin function are replace with boolean XQuery function and the XQuery equivalent
 * function.
 *
 * <pre>
 * Before
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : algebricks_function( \@input_expression ) )
 *   plan__child
 * 
 *   where the function annotation contains a hint on which xquery expression is represented by the algebricks function.
 * 
 * After
 * 
 *   plan__parent
 *   %OPERATOR( $v1 :xquery_expression( \@input_expression ) ) )
 *   plan__child
 * 
 *   note the xquery_expression may include the boolean function to ensure only a true or false result.
 *
 * </pre>
 *
 * @author prestonc, shivanim
 */
public class ConvertFromAlgebricksExpressionsRule implements IAlgebraicRewriteRule {
    final List<Mutable<ILogicalExpression>> functionList = new ArrayList<Mutable<ILogicalExpression>>();
    final static Map<FunctionIdentifier, Pair<IFunctionInfo, IFunctionInfo>> ALGEBRICKS_MAP = new HashMap<FunctionIdentifier, Pair<IFunctionInfo, IFunctionInfo>>();
    final static String ALGEBRICKS_CONVERSION_ANNOTATION = "ConversionToAndFromAlgebricks";
    AbstractFunctionCallExpression searchFunction;
    boolean isBoolean = false;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ConvertFromAlgebricksExpressionsRule() {
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.AND, new Pair(BuiltinOperators.AND, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.EQ, new Pair(BuiltinOperators.VALUE_EQ, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.GE, new Pair(BuiltinOperators.VALUE_GE, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.GT, new Pair(BuiltinOperators.VALUE_GT, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.IS_NULL, new Pair(null, BuiltinFunctions.FN_EMPTY_1));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.LE, new Pair(BuiltinOperators.VALUE_LE, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.LT, new Pair(BuiltinOperators.VALUE_LT, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.NOT, new Pair(null, BuiltinFunctions.FN_NOT_1));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.NEQ, new Pair(BuiltinOperators.VALUE_NE, null));
        ALGEBRICKS_MAP.put(AlgebricksBuiltinFunctions.OR, new Pair(BuiltinOperators.OR, null));
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
            if (processExpression(opRef, expression)) {
                modified = true;
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        return modified;
    }

    private boolean processExpression(Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> search) {
        boolean modified = false;
        functionList.clear();
        ExpressionToolbox.findAllFunctionExpressions(search, functionList);
        for (Mutable<ILogicalExpression> searchM : functionList) {
            searchFunction = (AbstractFunctionCallExpression) searchM.getValue();

            if (ALGEBRICKS_MAP.containsKey(searchFunction.getFunctionIdentifier())) {
                FunctionIdentifier fid = searchFunction.getFunctionIdentifier();

                if (ALGEBRICKS_MAP.get(fid).first == null) {
                    modified = convertAlgebricksExpression(searchM, ALGEBRICKS_MAP.get(fid).second, false);
                } else if (ALGEBRICKS_MAP.get(fid).second == null) {
                    modified = convertAlgebricksExpression(searchM, ALGEBRICKS_MAP.get(fid).first, true);
                } else {
                    IExpressionAnnotation annotate = searchFunction.getAnnotations().get(
                            ALGEBRICKS_CONVERSION_ANNOTATION);

                    if (((FunctionIdentifier) annotate.getObject()).equals(ALGEBRICKS_MAP.get(fid).first
                            .getFunctionIdentifier())) {

                        modified = convertAlgebricksExpression(searchM, ALGEBRICKS_MAP.get(fid).first, true);
                    } else if (((FunctionIdentifier) annotate.getObject()).equals(ALGEBRICKS_MAP.get(fid).second
                            .getFunctionIdentifier())) {

                        modified = convertAlgebricksExpression(searchM, ALGEBRICKS_MAP.get(fid).second, false);
                    }
                }
            }
        }
        return modified;
    }

    @SuppressWarnings("unchecked")
    private boolean convertAlgebricksExpression(Mutable<ILogicalExpression> searchM, IFunctionInfo funcInfo,
            boolean isBoolean) {
        AbstractFunctionCallExpression searchFunction = (AbstractFunctionCallExpression) searchM.getValue();
        searchFunction.setFunctionInfo(funcInfo);

        if (isBoolean) {
            ScalarFunctionCallExpression functionCallExp = new ScalarFunctionCallExpression(
                    BuiltinFunctions.FN_BOOLEAN_1, new MutableObject<ILogicalExpression>(searchM.getValue()));
            searchM.setValue(functionCallExp);
        }
        return true;
    }
}
