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
package org.apache.vxquery.compiler.rewriter.rules.util;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class ExpressionToolbox {
    
    public static Mutable<ILogicalExpression> findVariableExpression(Mutable<ILogicalExpression> mutableLe, LogicalVariable lv) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression vre = (VariableReferenceExpression) le;
            if (vre.getVariableReference() == lv) {
                return mutableLe;
            }
        } else if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                Mutable<ILogicalExpression> resultLe = findVariableExpression(argExp, lv);
                if (resultLe != null) {
                    return resultLe;
                }
            }
        }
        return null;
    }

    public static Mutable<ILogicalExpression> findFunctionExpression(Mutable<ILogicalExpression> mutableLe, FunctionIdentifier fi) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            if (afce.getFunctionIdentifier().equals(fi)) {
                return mutableLe;
            }
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                Mutable<ILogicalExpression> resultLe = findFunctionExpression(argExp, fi);
                if (resultLe != null) {
                    return resultLe;
                }
            }
        }
        return null;
    }

    public static Function getBuiltIn(Mutable<ILogicalExpression> mutableLe) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Function function : BuiltinFunctions.FUNCTION_COLLECTION) {
                if (function.getFunctionIdentifier().equals(afce.getFunctionIdentifier())) {
                    return function;
                }
            }
            for (Function function : BuiltinOperators.OPERATOR_COLLECTION) {
                if (function.getFunctionIdentifier().equals(afce.getFunctionIdentifier())) {
                    return function;
                }
            }
        }
        return null;
    }
    
    public static void getConstantAsPointable(ConstantExpression typeExpression, TaggedValuePointable tvp) {
        VXQueryConstantValue treatTypeConstant = (VXQueryConstantValue) typeExpression.getValue();
        tvp.set(treatTypeConstant.getValue(), 0, treatTypeConstant.getValue().length);
    }


}
